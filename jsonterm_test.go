package bufarrowlib

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// container builds a Container{name, children...} dynamic message.
func container(md protoreflect.MessageDescriptor, name string, children ...*dynamicpb.Message) *dynamicpb.Message {
	m := dynamicpb.NewMessage(md)
	m.Set(md.Fields().ByName("name"), protoreflect.ValueOfString(name))
	if len(children) > 0 {
		l := m.Mutable(md.Fields().ByName("children")).List()
		for _, c := range children {
			l.Append(protoreflect.ValueOfMessage(c))
		}
	}
	return m
}

// structField returns the named child field of a struct type.
func structField(t *testing.T, st *arrow.StructType, name string) arrow.Field {
	t.Helper()
	f, ok := st.FieldByName(name)
	if !ok {
		t.Fatalf("field %q not in %s", name, st)
	}
	return f
}

// TestJSONTerminationRequiresOptIn guards that cyclic types still fail
// without the option.
func TestJSONTerminationRequiresOptIn(t *testing.T) {
	for _, name := range []string{"Container", "SelfRef", "MutualA"} {
		_, err := New(wktDesc(t, name), memory.DefaultAllocator)
		if !errors.Is(err, ErrCyclicType) {
			t.Errorf("New(%s) error = %v, want ErrCyclicType", name, err)
		}
	}
	// Naming an unrelated type does not suppress the error.
	_, err := New(wktDesc(t, "Container"), memory.DefaultAllocator, WithJSONTermination("samples.SelfRef"))
	if !errors.Is(err, ErrCyclicType) {
		t.Errorf("New(Container, other type) error = %v, want ErrCyclicType", err)
	}
}

// TestJSONTerminationSchema verifies termination happens only at the cycle
// point: the root's scalar fields stay structural.
func TestJSONTerminationSchema(t *testing.T) {
	t.Run("repeated self reference", func(t *testing.T) {
		md := wktDesc(t, "Container")
		tc, err := New(md, memory.DefaultAllocator, WithJSONTermination(md.FullName()))
		if err != nil {
			t.Fatalf("New error = %v", err)
		}
		defer tc.Release()
		if got := fieldType(t, tc.Schema(), "name"); got.ID() != arrow.STRING {
			t.Errorf("name = %v, want string", got)
		}
		lt, ok := fieldType(t, tc.Schema(), "children").(*arrow.ListType)
		if !ok || lt.Elem().ID() != arrow.STRING {
			t.Errorf("children = %v, want list<string>", fieldType(t, tc.Schema(), "children"))
		}
	})
	t.Run("singular self reference", func(t *testing.T) {
		md := wktDesc(t, "SelfRef")
		tc, err := New(md, memory.DefaultAllocator, WithJSONTermination(md.FullName()))
		if err != nil {
			t.Fatalf("New error = %v", err)
		}
		defer tc.Release()
		if got := fieldType(t, tc.Schema(), "child"); got.ID() != arrow.STRING {
			t.Errorf("child = %v, want string", got)
		}
	})
	t.Run("mutual cycle expands one round", func(t *testing.T) {
		md := wktDesc(t, "MutualA")
		tc, err := New(md, memory.DefaultAllocator, WithJSONTermination("samples.MutualA", "samples.MutualB"))
		if err != nil {
			t.Fatalf("New error = %v", err)
		}
		defer tc.Release()
		// MutualA{a, b: MutualB{b, a: <json>}}
		bt, ok := fieldType(t, tc.Schema(), "b").(*arrow.StructType)
		if !ok {
			t.Fatalf("b = %v, want struct", fieldType(t, tc.Schema(), "b"))
		}
		if got := structField(t, bt, "b").Type; got.ID() != arrow.STRING {
			t.Errorf("b.b = %v, want string", got)
		}
		if got := structField(t, bt, "a").Type; got.ID() != arrow.STRING {
			t.Errorf("b.a = %v, want protojson string", got)
		}
	})
	t.Run("naming only the re-entered type suffices", func(t *testing.T) {
		// The cycle is detected when MutualA is re-entered, so naming it
		// alone is enough. MutualB stays structural.
		md := wktDesc(t, "MutualA")
		tc, err := New(md, memory.DefaultAllocator, WithJSONTermination("samples.MutualA"))
		if err != nil {
			t.Fatalf("New error = %v", err)
		}
		tc.Release()
	})
}

// TestJSONTerminationRoundTrip appends a three-level tree and checks the
// terminated cells semantically, then through Transcoder.Proto.
func TestJSONTerminationRoundTrip(t *testing.T) {
	md := wktDesc(t, "Container")
	tc, err := New(md, memory.DefaultAllocator, WithJSONTermination(md.FullName()))
	if err != nil {
		t.Fatalf("New error = %v", err)
	}
	defer tc.Release()

	grandchild := container(md, "gc")
	child := container(md, "c1", grandchild)
	root := container(md, "root", child, container(md, "c2"))
	tc.Append(root.Interface())
	tc.Append(container(md, "leaf").Interface())

	rec := tc.NewRecordBatch()
	defer rec.Release()
	if rec.NumRows() != 2 {
		t.Fatalf("NumRows = %d, want 2", rec.NumRows())
	}

	names := rec.Column(rec.Schema().FieldIndices("name")[0]).(*array.String)
	if names.Value(0) != "root" {
		t.Errorf("name[0] = %q, want root", names.Value(0))
	}
	ls := rec.Column(rec.Schema().FieldIndices("children")[0]).(*array.List)
	vals := ls.ListValues().(*array.String)
	start, end := ls.ValueOffsets(0)
	if end-start != 2 {
		t.Fatalf("children[0] len = %d, want 2", end-start)
	}
	got := dynamicpb.NewMessage(md)
	if err := protojson.Unmarshal([]byte(vals.Value(int(start))), got.Interface()); err != nil {
		t.Fatalf("protojson.Unmarshal(%q) error = %v", vals.Value(int(start)), err)
	}
	if !proto.Equal(got.Interface(), child.Interface()) {
		t.Errorf("child mismatch: got %v want %v", got, child)
	}
	if s, e := ls.ValueOffsets(1); s != e {
		t.Errorf("children[1] len = %d, want 0", e-s)
	}

	msgs := tc.Proto(rec, nil)
	if len(msgs) != 2 {
		t.Fatalf("Proto returned %d messages, want 2", len(msgs))
	}
	if !proto.Equal(msgs[0], root.Interface()) {
		t.Errorf("Proto round-trip mismatch:\n got %v\nwant %v", msgs[0], root)
	}
}

// TestJSONTerminationQuotedScalars locks in that scalar strings inside a
// terminated subtree are JSON text, so they are double-quoted in the raw cell.
// Covers both the user-cycle path and the recursive-WKT path.
func TestJSONTerminationQuotedScalars(t *testing.T) {
	md := wktDesc(t, "SettingsTree")
	tc, err := New(md, memory.DefaultAllocator, WithJSONTermination(md.FullName()))
	if err != nil {
		t.Fatalf("New error = %v", err)
	}
	defer tc.Release()

	settingsFD := md.Fields().ByName("settings")
	valueMD := settingsFD.MapValue().Message()
	withSetting := func(m *dynamicpb.Message, k, v string) {
		val := dynamicpb.NewMessage(valueMD)
		val.Set(valueMD.Fields().ByName("string_value"), protoreflect.ValueOfString(v))
		m.Mutable(settingsFD).Map().Set(protoreflect.ValueOfString(k).MapKey(), protoreflect.ValueOfMessage(val))
	}

	root := dynamicpb.NewMessage(md)
	root.Set(md.Fields().ByName("key"), protoreflect.ValueOfString("ns"))
	withSetting(root, "mode", "somevalue")
	sub := dynamicpb.NewMessage(md)
	sub.Set(md.Fields().ByName("key"), protoreflect.ValueOfString("inner"))
	root.Mutable(md.Fields().ByName("subtrees")).List().Append(protoreflect.ValueOfMessage(sub))
	tc.Append(root.Interface())

	rec := tc.NewRecordBatch()
	defer rec.Release()

	// key is not inside a terminated subtree, so it stays plain.
	if got := rec.Column(rec.Schema().FieldIndices("key")[0]).(*array.String).Value(0); got != "ns" {
		t.Errorf("key = %q, want unquoted ns", got)
	}

	// The Value WKT cell is the JSON text with quotes.
	items := rec.Column(rec.Schema().FieldIndices("settings")[0]).(*array.Map).Items().(*array.String)
	raw := items.Value(0)
	if raw != `"somevalue"` {
		t.Errorf("settings[mode] raw = %q, want %q", raw, `"somevalue"`)
	}
	var s string
	if err := json.Unmarshal([]byte(raw), &s); err != nil || s != "somevalue" {
		t.Errorf("json.Unmarshal(%q) = %q, %v; want somevalue", raw, s, err)
	}

	// The user-cycle cell embeds key as a quoted JSON string.
	subs := rec.Column(rec.Schema().FieldIndices("subtrees")[0]).(*array.List).ListValues().(*array.String)
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(subs.Value(0)), &obj); err != nil {
		t.Fatalf("json.Unmarshal(%q) error = %v", subs.Value(0), err)
	}
	if !bytes.Equal(bytes.TrimSpace(obj["key"]), []byte(`"inner"`)) {
		t.Errorf("subtree key = %s, want %q", obj["key"], `"inner"`)
	}
}

// TestJSONTerminationLeavesRecursiveWKTs guards that the option does not change
// unconditional termination of Struct/Value/ListValue.
func TestJSONTerminationLeavesRecursiveWKTs(t *testing.T) {
	md := wktDesc(t, "WithStruct")
	plain, err := New(md, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("New error = %v", err)
	}
	defer plain.Release()
	opted, err := New(md, memory.DefaultAllocator, WithJSONTermination("google.protobuf.Struct", "google.protobuf.Value"))
	if err != nil {
		t.Fatalf("New(WithJSONTermination) error = %v", err)
	}
	defer opted.Release()
	if !plain.Schema().Equal(opted.Schema()) {
		t.Errorf("schema changed:\n plain %s\nopted %s", plain.Schema(), opted.Schema())
	}
	if got := fieldType(t, opted.Schema(), "settings"); got.ID() != arrow.STRING {
		t.Errorf("settings = %v, want string", got)
	}
}

// TestJSONTerminationClone verifies Clone inherits the option.
func TestJSONTerminationClone(t *testing.T) {
	md := wktDesc(t, "Container")
	tc, err := New(md, memory.DefaultAllocator, WithJSONTermination(md.FullName()))
	if err != nil {
		t.Fatalf("New error = %v", err)
	}
	defer tc.Release()
	cl, err := tc.Clone(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("Clone error = %v", err)
	}
	defer cl.Release()
	if !tc.Schema().Equal(cl.Schema()) {
		t.Errorf("clone schema differs:\n got %s\nwant %s", cl.Schema(), tc.Schema())
	}
	cl.Append(container(md, "r", container(md, "c")).Interface())
	rec := cl.NewRecordBatch()
	defer rec.Release()
	if rec.NumRows() != 1 {
		t.Errorf("NumRows = %d, want 1", rec.NumRows())
	}
}

// TestCyclicTypeErrorAs verifies callers can read the cyclic type with
// errors.As, and that errors.Is still matches ErrCyclicType.
func TestCyclicTypeErrorAs(t *testing.T) {
	_, err := New(wktDesc(t, "SelfRef"), memory.DefaultAllocator)
	if !errors.Is(err, ErrCyclicType) {
		t.Fatalf("error = %v, want ErrCyclicType", err)
	}
	var ce *CyclicTypeError
	if !errors.As(err, &ce) {
		t.Fatalf("errors.As(%v, *CyclicTypeError) = false", err)
	}
	if ce.Type != "samples.SelfRef" || ce.Path != "child" {
		t.Errorf("CyclicTypeError = {%s %s}, want {samples.SelfRef child}", ce.Type, ce.Path)
	}
}

// TestCyclicTypeErrorRetryLoop shows the manual retry pattern converges.
func TestCyclicTypeErrorRetryLoop(t *testing.T) {
	md := wktDesc(t, "MultiCycle")
	var names []protoreflect.FullName
	for attempt := 0; ; attempt++ {
		if attempt > 10 {
			t.Fatalf("retry loop did not converge, names = %v", names)
		}
		tc, err := New(md, memory.DefaultAllocator, WithJSONTermination(names...))
		var ce *CyclicTypeError
		if errors.As(err, &ce) {
			names = append(names, ce.Type)
			continue
		}
		if err != nil {
			t.Fatalf("New error = %v", err)
		}
		tc.Release()
		break
	}
	if len(names) != 3 {
		t.Errorf("retry loop found %v, want 3 types", names)
	}
}

// TestCyclicTypes verifies every independent cycle is reported in one call,
// and that the result makes New succeed.
func TestCyclicTypes(t *testing.T) {
	md := wktDesc(t, "MultiCycle")
	names, err := CyclicTypes(md)
	if err != nil {
		t.Fatalf("CyclicTypes error = %v", err)
	}
	want := []protoreflect.FullName{"samples.Container", "samples.MutualA", "samples.SelfRef"}
	if !slices.Equal(names, want) {
		t.Fatalf("CyclicTypes = %v, want %v", names, want)
	}
	tc, err := New(md, memory.DefaultAllocator, WithJSONTermination(names...))
	if err != nil {
		t.Fatalf("New(WithJSONTermination(CyclicTypes)) error = %v", err)
	}
	defer tc.Release()

	// Matches the auto option exactly.
	auto, err := New(md, memory.DefaultAllocator, WithJSONTerminationAuto())
	if err != nil {
		t.Fatalf("New(WithJSONTerminationAuto) error = %v", err)
	}
	defer auto.Release()
	if !tc.Schema().Equal(auto.Schema()) {
		t.Errorf("schemas differ:\nnamed %s\n auto %s", tc.Schema(), auto.Schema())
	}
}

// TestCyclicTypesNone verifies acyclic schemas, including diamonds and the
// recursive WKTs, report no cycles.
func TestCyclicTypesNone(t *testing.T) {
	for _, name := range []string{"Acyclic", "DiamondOuter", "WithNestedNs"} {
		names, err := CyclicTypes(wktDesc(t, name))
		if err != nil {
			t.Errorf("CyclicTypes(%s) error = %v", name, err)
		}
		if names != nil {
			t.Errorf("CyclicTypes(%s) = %v, want nil", name, names)
		}
	}
}

// TestCyclicTypesDepthError verifies non-cycle errors still surface.
func TestCyclicTypesDepthError(t *testing.T) {
	_, err := CyclicTypes(wktDesc(t, "DeepAcyclic12"))
	if !errors.Is(err, ErrMxDepth) {
		t.Errorf("CyclicTypes(DeepAcyclic12) error = %v, want ErrMxDepth", err)
	}
}

// TestJSONTerminationAuto verifies the auto option handles every cycle,
// round-trips data, leaves acyclic schemas unchanged, and survives Clone.
func TestJSONTerminationAuto(t *testing.T) {
	t.Run("acyclic schema unchanged", func(t *testing.T) {
		md := wktDesc(t, "Acyclic")
		plain, err := New(md, memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("New error = %v", err)
		}
		defer plain.Release()
		auto, err := New(md, memory.DefaultAllocator, WithJSONTerminationAuto())
		if err != nil {
			t.Fatalf("New(auto) error = %v", err)
		}
		defer auto.Release()
		if !plain.Schema().Equal(auto.Schema()) {
			t.Errorf("schema changed:\nplain %s\n auto %s", plain.Schema(), auto.Schema())
		}
	})
	t.Run("round trip and clone", func(t *testing.T) {
		md := wktDesc(t, "Container")
		tc, err := New(md, memory.DefaultAllocator, WithJSONTerminationAuto())
		if err != nil {
			t.Fatalf("New error = %v", err)
		}
		defer tc.Release()
		cl, err := tc.Clone(memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("Clone error = %v", err)
		}
		defer cl.Release()
		root := container(md, "root", container(md, "c", container(md, "gc")))
		cl.Append(root.Interface())
		rec := cl.NewRecordBatch()
		defer rec.Release()
		msgs := cl.Proto(rec, nil)
		if len(msgs) != 1 || !proto.Equal(msgs[0], root.Interface()) {
			t.Errorf("round-trip mismatch: got %v want %v", msgs, root)
		}
	})
}
