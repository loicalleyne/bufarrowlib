package pbpath

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ── Parse + Exec integration tests ──────────────────────────────────────

func TestPipelineBasic(t *testing.T) {
	containerMD, _ := buildFilterTestDescriptor(t)
	msg := buildFilterTestMsg(t, containerMD, containerMD.Fields().ByName("items").Message())

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int      // expected number of output Values
		wantStrs []string // if set, check String() of each output
	}{
		{
			name:     "identity",
			pipeline: ".",
			wantLen:  1,
		},
		{
			name:     "field access",
			pipeline: ".name",
			wantLen:  1,
			wantStrs: []string{"test-container"},
		},
		{
			name:     "nested field access",
			pipeline: ".single.name",
			wantLen:  1,
			wantStrs: []string{"solo"},
		},
		{
			name:     "deep nested field",
			pipeline: ".single.inner.label",
			wantLen:  1,
			wantStrs: []string{"lbl-s"},
		},
		{
			name:     "iterate list",
			pipeline: ".items | .[]",
			wantLen:  4, // 4 items
		},
		{
			name:     "iterate then field",
			pipeline: ".items | .[] | .name",
			wantLen:  4,
			wantStrs: []string{"alpha", "beta", "gamma", "delta"},
		},
		{
			name:     "field then iterate shorthand",
			pipeline: ".items | .[] | .value",
			wantLen:  4,
			wantStrs: []string{"10", "20", "30", "40"},
		},
		{
			name:     "index element",
			pipeline: ".items | .[0] | .name",
			wantLen:  1,
			wantStrs: []string{"alpha"},
		},
		{
			name:     "negative index",
			pipeline: ".items | .[-1] | .name",
			wantLen:  1,
			wantStrs: []string{"delta"},
		},
		{
			name:     "select equality",
			pipeline: `.items | .[] | select(.name == "gamma") | .value`,
			wantLen:  1,
			wantStrs: []string{"30"},
		},
		{
			name:     "select comparison",
			pipeline: ".items | .[] | select(.value > 20) | .name",
			wantLen:  2,
			wantStrs: []string{"gamma", "delta"},
		},
		{
			name:     "select boolean truthy",
			pipeline: ".items | .[] | select(.active) | .name",
			wantLen:  3,
			wantStrs: []string{"alpha", "gamma", "delta"},
		},
		{
			name:     "select negation",
			pipeline: ".items | .[] | select(.active | not) | .name",
			wantLen:  1,
			wantStrs: []string{"beta"},
		},
		{
			name:     "select and",
			pipeline: `.items | .[] | select(.active and .value > 10) | .name`,
			wantLen:  2,
			wantStrs: []string{"gamma", "delta"},
		},
		{
			name:     "select or",
			pipeline: `.items | .[] | select(.value == 10 or .value == 40) | .name`,
			wantLen:  2,
			wantStrs: []string{"alpha", "delta"},
		},
		{
			name:     "collect",
			pipeline: "[.items | .[] | .name]",
			wantLen:  1, // one list value containing 4 names
		},
		{
			name:     "collect then length",
			pipeline: "[.items | .[] | .name] | length",
			wantLen:  1,
			wantStrs: []string{"4"},
		},
		{
			name:     "literal string",
			pipeline: `"hello"`,
			wantLen:  1,
			wantStrs: []string{"hello"},
		},
		{
			name:     "literal int",
			pipeline: "42",
			wantLen:  1,
			wantStrs: []string{"42"},
		},
		{
			name:     "literal float",
			pipeline: "3.14",
			wantLen:  1,
			wantStrs: []string{"3.14"},
		},
		{
			name:     "literal bool true",
			pipeline: "true",
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		{
			name:     "literal null",
			pipeline: "null",
			wantLen:  1,
			wantStrs: []string{"null"},
		},
		{
			name:     "empty",
			pipeline: "empty",
			wantLen:  0,
		},
		{
			name:     "comparison eq true",
			pipeline: ".name == .name",
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		{
			name:     "comparison ne",
			pipeline: `.name != "other"`,
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		{
			name:     "grouping",
			pipeline: "(.name)",
			wantLen:  1,
			wantStrs: []string{"test-container"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParsePipeline(containerMD, tc.pipeline)
			if err != nil {
				t.Fatalf("ParsePipeline(%q): %v", tc.pipeline, err)
			}
			results, err := p.ExecMessage(msg.ProtoReflect())
			if err != nil {
				t.Fatalf("Exec(%q): %v", tc.pipeline, err)
			}
			if len(results) != tc.wantLen {
				t.Errorf("Exec(%q): got %d results, want %d", tc.pipeline, len(results), tc.wantLen)
				for i, v := range results {
					t.Logf("  [%d] = %s", i, v.String())
				}
				return
			}
			if tc.wantStrs != nil {
				for i, want := range tc.wantStrs {
					if i >= len(results) {
						break
					}
					got := results[i].String()
					if got != want {
						t.Errorf("  result[%d] = %q, want %q", i, got, want)
					}
				}
			}
		})
	}
}

// ── Built-in function tests ─────────────────────────────────────────────

func TestPipelineBuiltins(t *testing.T) {
	containerMD, _ := buildFilterTestDescriptor(t)
	msg := buildFilterTestMsg(t, containerMD, containerMD.Fields().ByName("items").Message())

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		{
			name:     "length of list",
			pipeline: ".items | length",
			wantLen:  1,
			wantStrs: []string{"4"},
		},
		{
			name:     "length of string",
			pipeline: ".name | length",
			wantLen:  1,
			wantStrs: []string{"14"}, // "test-container" = 14 chars
		},
		{
			name:     "length of null",
			pipeline: "null | length",
			wantLen:  1,
			wantStrs: []string{"0"},
		},
		{
			name:     "type of string",
			pipeline: ".name | type",
			wantLen:  1,
			wantStrs: []string{"string"},
		},
		{
			name:     "type of number",
			pipeline: ".single.value | type",
			wantLen:  1,
			wantStrs: []string{"number"},
		},
		{
			name:     "type of bool",
			pipeline: ".single.active | type",
			wantLen:  1,
			wantStrs: []string{"boolean"},
		},
		{
			name:     "type of null",
			pipeline: "null | type",
			wantLen:  1,
			wantStrs: []string{"null"},
		},
		{
			name:     "type of list",
			pipeline: ".items | type",
			wantLen:  1,
			wantStrs: []string{"array"},
		},
		{
			name:     "type of object",
			pipeline: ".single | type",
			wantLen:  1,
			wantStrs: []string{"object"},
		},
		{
			name:     "keys of object",
			pipeline: ".single | keys | length",
			wantLen:  1,
			// keys returns populated fields; all 6 fields are set.
			wantStrs: []string{"6"},
		},
		{
			name:     "not on true",
			pipeline: "true | not",
			wantLen:  1,
			wantStrs: []string{"false"},
		},
		{
			name:     "not on false",
			pipeline: "false | not",
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		{
			name:     "not on null",
			pipeline: "null | not",
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		{
			name:     "empty",
			pipeline: "empty",
			wantLen:  0,
		},
		{
			name:     "add numbers",
			pipeline: "[.items | .[] | .value] | add",
			wantLen:  1,
			wantStrs: []string{"100"}, // 10+20+30+40
		},
		{
			name:     "add strings",
			pipeline: `[.items | .[] | .kind] | add`,
			wantLen:  1,
			wantStrs: []string{"ABAB"},
		},
		{
			name:     "tostring",
			pipeline: ".single.value | tostring",
			wantLen:  1,
			wantStrs: []string{"99"},
		},
		{
			name:     "tonumber from string",
			pipeline: `"42" | tonumber`,
			wantLen:  1,
			wantStrs: []string{"42"},
		},
		{
			name:     "tonumber from float string",
			pipeline: `"3.14" | tonumber`,
			wantLen:  1,
			wantStrs: []string{"3.14"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParsePipeline(containerMD, tc.pipeline)
			if err != nil {
				t.Fatalf("ParsePipeline(%q): %v", tc.pipeline, err)
			}
			results, err := p.ExecMessage(msg.ProtoReflect())
			if err != nil {
				t.Fatalf("Exec(%q): %v", tc.pipeline, err)
			}
			if len(results) != tc.wantLen {
				t.Errorf("Exec(%q): got %d results, want %d", tc.pipeline, len(results), tc.wantLen)
				for i, v := range results {
					t.Logf("  [%d] = %s", i, v.String())
				}
				return
			}
			if tc.wantStrs != nil {
				for i, want := range tc.wantStrs {
					if i >= len(results) {
						break
					}
					got := results[i].String()
					if got != want {
						t.Errorf("  result[%d] = %q, want %q", i, got, want)
					}
				}
			}
		})
	}
}

// ── Parse error tests ───────────────────────────────────────────────────

func TestPipelineParseErrors(t *testing.T) {
	containerMD, _ := buildFilterTestDescriptor(t)

	tcs := []struct {
		name     string
		pipeline string
		wantErr  string
	}{
		{
			name:     "unknown field",
			pipeline: ".nonexistent",
			wantErr:  "not found",
		},
		{
			name:     "field on scalar",
			pipeline: ".name.sub",
			wantErr:  "cannot access field",
		},
		{
			name:     "unknown function",
			pipeline: "foobar",
			wantErr:  "unknown",
		},
		{
			name:     "unclosed bracket",
			pipeline: ".[",
			wantErr:  "expected",
		},
		{
			name:     "unclosed paren",
			pipeline: "(.name",
			wantErr:  "expected ')'",
		},
		{
			name:     "select missing paren",
			pipeline: "select .name",
			wantErr:  "expected '('",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParsePipeline(containerMD, tc.pipeline)
			if err == nil {
				t.Fatalf("ParsePipeline(%q): expected error containing %q, got nil", tc.pipeline, tc.wantErr)
			}
			if !containsStr(err.Error(), tc.wantErr) {
				t.Errorf("ParsePipeline(%q): error %q does not contain %q", tc.pipeline, err.Error(), tc.wantErr)
			}
		})
	}
}

// containsStr is defined in filter_test.go

// ── Value.String() tests ────────────────────────────────────────────────

func TestValueString(t *testing.T) {
	tcs := []struct {
		name string
		val  Value
		want string
	}{
		{name: "null", val: Null(), want: "null"},
		{name: "bool true", val: ScalarBool(true), want: "true"},
		{name: "bool false", val: ScalarBool(false), want: "false"},
		{name: "int", val: ScalarInt64(42), want: "42"},
		{name: "float", val: ScalarFloat64(3.14), want: "3.14"},
		{name: "string", val: ScalarString("hello"), want: "hello"},
		{name: "empty list", val: ListVal(nil), want: "[]"},
		{name: "list of ints", val: ListVal([]Value{ScalarInt64(1), ScalarInt64(2)}), want: "[1,2]"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.val.String()
			if got != tc.want {
				t.Errorf("Value.String() = %q, want %q", got, tc.want)
			}
		})
	}
}

// ── Value.Get() test ────────────────────────────────────────────────────

func TestValueGet(t *testing.T) {
	containerMD, _ := buildFilterTestDescriptor(t)
	msg := buildFilterTestMsg(t, containerMD, containerMD.Fields().ByName("items").Message())

	v := MessageVal(msg.ProtoReflect())
	nameVal := v.Get(containerMD.Fields().ByName("name"))
	if nameVal.Kind() != ScalarKind {
		t.Fatalf("Get(name): expected ScalarKind, got %v", nameVal.Kind())
	}
	got := nameVal.String()
	if got != "test-container" {
		t.Errorf("Get(name) = %q, want %q", got, "test-container")
	}

	// Get on null
	null := Null()
	nullField := null.Get(containerMD.Fields().ByName("name"))
	if !nullField.IsNull() {
		t.Errorf("Null.Get() should return null, got %v", nullField.Kind())
	}
}

// ── Pipeline.ExecMessage convenience ────────────────────────────────────

func TestPipelineExecMessage(t *testing.T) {
	containerMD, _ := buildFilterTestDescriptor(t)
	msg := buildFilterTestMsg(t, containerMD, containerMD.Fields().ByName("items").Message())

	p, err := ParsePipeline(containerMD, ".name")
	if err != nil {
		t.Fatalf("ParsePipeline: %v", err)
	}
	results, err := p.ExecMessage(msg.ProtoReflect())
	if err != nil {
		t.Fatalf("ExecMessage: %v", err)
	}
	if len(results) != 1 || results[0].String() != "test-container" {
		t.Errorf("ExecMessage(.name) = %v, want [test-container]", results)
	}
}

// ── Benchmark ───────────────────────────────────────────────────────────

func BenchmarkPipelineSelectField(b *testing.B) {
	containerMD, _ := buildPipeTestDescriptor(b)
	msg := buildPipeTestMsg(b, containerMD)

	p, err := ParsePipeline(containerMD, `.items | .[] | select(.active and .value > 10) | .name`)
	if err != nil {
		b.Fatalf("ParsePipeline: %v", err)
	}
	input := []Value{MessageVal(msg.ProtoReflect())}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := p.Exec(input)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPipelineCollectAdd(b *testing.B) {
	containerMD, _ := buildPipeTestDescriptor(b)
	msg := buildPipeTestMsg(b, containerMD)

	p, err := ParsePipeline(containerMD, `[.items | .[] | .value] | add`)
	if err != nil {
		b.Fatalf("ParsePipeline: %v", err)
	}
	input := []Value{MessageVal(msg.ProtoReflect())}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := p.Exec(input)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// buildPipeTestDescriptor creates the same test schema as buildFilterTestDescriptor
// but accepts testing.TB for benchmark compatibility.
func buildPipeTestDescriptor(tb testing.TB) (containerMD, itemMD protoreflect.MessageDescriptor) {
	tb.Helper()

	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	int64Type := descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	boolType := descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum()
	doubleType := descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum()
	messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	labelOptional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	labelRepeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("pipe_test.proto"),
		Package: proto.String("pipetest"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Inner"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("label"), Number: proto.Int32(1), Type: stringType, Label: labelOptional},
					{Name: proto.String("count"), Number: proto.Int32(2), Type: int64Type, Label: labelOptional},
				},
			},
			{
				Name: proto.String("Item"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("name"), Number: proto.Int32(1), Type: stringType, Label: labelOptional},
					{Name: proto.String("value"), Number: proto.Int32(2), Type: int64Type, Label: labelOptional},
					{Name: proto.String("active"), Number: proto.Int32(3), Type: boolType, Label: labelOptional},
					{Name: proto.String("kind"), Number: proto.Int32(4), Type: stringType, Label: labelOptional},
					{Name: proto.String("score"), Number: proto.Int32(5), Type: doubleType, Label: labelOptional},
					{Name: proto.String("inner"), Number: proto.Int32(6), Type: messageType, TypeName: proto.String(".pipetest.Inner"), Label: labelOptional},
				},
			},
			{
				Name: proto.String("Container"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("items"), Number: proto.Int32(1), Type: messageType, TypeName: proto.String(".pipetest.Item"), Label: labelRepeated},
					{Name: proto.String("lookup"), Number: proto.Int32(2), Type: messageType, TypeName: proto.String(".pipetest.Container.LookupEntry"), Label: labelRepeated},
					{Name: proto.String("name"), Number: proto.Int32(3), Type: stringType, Label: labelOptional},
					{Name: proto.String("single"), Number: proto.Int32(4), Type: messageType, TypeName: proto.String(".pipetest.Item"), Label: labelOptional},
				},
				NestedType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("LookupEntry"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{Name: proto.String("key"), Number: proto.Int32(1), Type: stringType, Label: labelOptional},
							{Name: proto.String("value"), Number: proto.Int32(2), Type: messageType, TypeName: proto.String(".pipetest.Item"), Label: labelOptional},
						},
						Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
					},
				},
			},
		},
	}

	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		tb.Fatalf("protodesc.NewFile: %v", err)
	}
	return fd.Messages().ByName("Container"), fd.Messages().ByName("Item")
}

func buildPipeTestMsg(tb testing.TB, containerMD protoreflect.MessageDescriptor) proto.Message {
	tb.Helper()

	itemMD := containerMD.Fields().ByName("items").Message()
	innerMD := itemMD.Fields().ByName("inner").Message()

	mkItem := func(name string, value int64, active bool, kind string, score float64, innerLabel string, innerCount int64) *dynamicpb.Message {
		item := dynamicpb.NewMessage(itemMD)
		item.Set(itemMD.Fields().ByName("name"), protoreflect.ValueOfString(name))
		item.Set(itemMD.Fields().ByName("value"), protoreflect.ValueOfInt64(value))
		item.Set(itemMD.Fields().ByName("active"), protoreflect.ValueOfBool(active))
		item.Set(itemMD.Fields().ByName("kind"), protoreflect.ValueOfString(kind))
		item.Set(itemMD.Fields().ByName("score"), protoreflect.ValueOfFloat64(score))
		inner := dynamicpb.NewMessage(innerMD)
		inner.Set(innerMD.Fields().ByName("label"), protoreflect.ValueOfString(innerLabel))
		inner.Set(innerMD.Fields().ByName("count"), protoreflect.ValueOfInt64(innerCount))
		item.Set(itemMD.Fields().ByName("inner"), protoreflect.ValueOfMessage(inner))
		return item
	}

	container := dynamicpb.NewMessage(containerMD)
	container.Set(containerMD.Fields().ByName("name"), protoreflect.ValueOfString("test-container"))

	items := container.Mutable(containerMD.Fields().ByName("items")).List()
	items.Append(protoreflect.ValueOfMessage(mkItem("alpha", 10, true, "A", 1.5, "lbl-a", 100)))
	items.Append(protoreflect.ValueOfMessage(mkItem("beta", 20, false, "B", 2.5, "lbl-b", 200)))
	items.Append(protoreflect.ValueOfMessage(mkItem("gamma", 30, true, "A", 3.5, "lbl-c", 300)))
	items.Append(protoreflect.ValueOfMessage(mkItem("delta", 40, true, "B", 0.5, "lbl-d", 400)))

	lookup := container.Mutable(containerMD.Fields().ByName("lookup")).Map()
	lookup.Set(protoreflect.ValueOfString("x").MapKey(), protoreflect.ValueOfMessage(mkItem("x-item", 100, true, "X", 9.0, "lbl-x", 1000)))
	lookup.Set(protoreflect.ValueOfString("y").MapKey(), protoreflect.ValueOfMessage(mkItem("y-item", 200, false, "Y", 8.0, "lbl-y", 2000)))
	lookup.Set(protoreflect.ValueOfString("z").MapKey(), protoreflect.ValueOfMessage(mkItem("z-item", 300, true, "Z", 7.0, "lbl-z", 3000)))

	container.Set(containerMD.Fields().ByName("single"),
		protoreflect.ValueOfMessage(mkItem("solo", 99, true, "S", 5.0, "lbl-s", 999)))

	return container
}
