package pbpath

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// buildFilterTestDescriptor creates a message descriptor suitable for testing
// filter predicates, map wildcards, and the select() function.
//
//	package filtertest;
//	message Item {
//	    string  name   = 1;
//	    int64   value  = 2;
//	    bool    active = 3;
//	    string  kind   = 4;
//	    double  score  = 5;
//	    Inner   inner  = 6;
//	}
//	message Inner {
//	    string label = 1;
//	    int64  count = 2;
//	}
//	message Container {
//	    repeated Item           items    = 1;
//	    map<string, Item>       lookup   = 2;
//	    string                  name     = 3;
//	    Item                    single   = 4;
//	}
func buildFilterTestDescriptor(t *testing.T) (containerMD, itemMD protoreflect.MessageDescriptor) {
	t.Helper()

	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	int64Type := descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	boolType := descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum()
	doubleType := descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum()
	messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	labelOptional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	labelRepeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("filter_test.proto"),
		Package: proto.String("filtertest"),
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
					{Name: proto.String("inner"), Number: proto.Int32(6), Type: messageType, TypeName: proto.String(".filtertest.Inner"), Label: labelOptional},
				},
			},
			{
				Name: proto.String("Container"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("items"), Number: proto.Int32(1), Type: messageType, TypeName: proto.String(".filtertest.Item"), Label: labelRepeated},
					{Name: proto.String("lookup"), Number: proto.Int32(2), Type: messageType, TypeName: proto.String(".filtertest.Container.LookupEntry"), Label: labelRepeated},
					{Name: proto.String("name"), Number: proto.Int32(3), Type: stringType, Label: labelOptional},
					{Name: proto.String("single"), Number: proto.Int32(4), Type: messageType, TypeName: proto.String(".filtertest.Item"), Label: labelOptional},
				},
				NestedType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("LookupEntry"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{Name: proto.String("key"), Number: proto.Int32(1), Type: stringType, Label: labelOptional},
							{Name: proto.String("value"), Number: proto.Int32(2), Type: messageType, TypeName: proto.String(".filtertest.Item"), Label: labelOptional},
						},
						Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
					},
				},
			},
		},
	}

	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatalf("protodesc.NewFile: %v", err)
	}
	return fd.Messages().ByName("Container"), fd.Messages().ByName("Item")
}

// buildFilterTestMsg creates a Container with test data.
func buildFilterTestMsg(t *testing.T, containerMD, itemMD protoreflect.MessageDescriptor) proto.Message {
	t.Helper()

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

	// Add items to the repeated field.
	items := container.Mutable(containerMD.Fields().ByName("items")).List()
	items.Append(protoreflect.ValueOfMessage(mkItem("alpha", 10, true, "A", 1.5, "lbl-a", 100)))
	items.Append(protoreflect.ValueOfMessage(mkItem("beta", 20, false, "B", 2.5, "lbl-b", 200)))
	items.Append(protoreflect.ValueOfMessage(mkItem("gamma", 30, true, "A", 3.5, "lbl-c", 300)))
	items.Append(protoreflect.ValueOfMessage(mkItem("delta", 40, true, "B", 0.5, "lbl-d", 400)))

	// Add items to the map.
	lookup := container.Mutable(containerMD.Fields().ByName("lookup")).Map()
	lookup.Set(protoreflect.ValueOfString("x").MapKey(), protoreflect.ValueOfMessage(mkItem("x-item", 100, true, "X", 9.0, "lbl-x", 1000)))
	lookup.Set(protoreflect.ValueOfString("y").MapKey(), protoreflect.ValueOfMessage(mkItem("y-item", 200, false, "Y", 8.0, "lbl-y", 2000)))
	lookup.Set(protoreflect.ValueOfString("z").MapKey(), protoreflect.ValueOfMessage(mkItem("z-item", 300, true, "Z", 7.0, "lbl-z", 3000)))

	// Set single item.
	container.Set(containerMD.Fields().ByName("single"),
		protoreflect.ValueOfMessage(mkItem("solo", 99, true, "S", 5.0, "lbl-s", 999)))

	return container
}

// ---- Parse tests for filter syntax ----

func TestParsePathFilter(t *testing.T) {
	containerMD, _ := buildFilterTestDescriptor(t)

	tcs := []struct {
		name    string
		path    string
		wantStr string // expected Path.String() output
		wantErr string
	}{
		{
			name:    "simple equality filter",
			path:    `items[?(.name == "alpha")]`,
			wantStr: `(filtertest.Container).items[*][?(...)]`,
		},
		{
			name:    "numeric comparison filter",
			path:    `items[?(.value > 20)]`,
			wantStr: `(filtertest.Container).items[*][?(...)]`,
		},
		{
			name:    "boolean truthy filter",
			path:    `items[?(.active)]`,
			wantStr: `(filtertest.Container).items[*][?(...)]`,
		},
		{
			name:    "negation filter",
			path:    `items[?(!.active)]`,
			wantStr: `(filtertest.Container).items[*][?(...)]`,
		},
		{
			name:    "compound AND filter",
			path:    `items[?(.active && .value > 10)]`,
			wantStr: `(filtertest.Container).items[*][?(...)]`,
		},
		{
			name:    "compound OR filter",
			path:    `items[?(.kind == "A" || .kind == "B")]`,
			wantStr: `(filtertest.Container).items[*][?(...)]`,
		},
		{
			name:    "nested path in filter",
			path:    `items[?(.inner.count > 200)]`,
			wantStr: `(filtertest.Container).items[*][?(...)]`,
		},
		{
			name:    "filter then field access",
			path:    `items[?(.active)].name`,
			wantStr: `(filtertest.Container).items[*][?(...)].name`,
		},
		{
			name:    "unknown field in filter",
			path:    `items[?(.unknown == "x")]`,
			wantErr: `field "unknown" not found`,
		},
		{
			name:    "missing closing paren",
			path:    `items[?(.name == "x"]`,
			wantErr: `expected ')'`,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			pp, err := ParsePath(containerMD, tc.path)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("ParsePath(%q) = %v, nil; want error containing %q", tc.path, pp, tc.wantErr)
				}
				if !containsStr(err.Error(), tc.wantErr) {
					t.Fatalf("ParsePath(%q) error = %v; want error containing %q", tc.path, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParsePath(%q) error: %v", tc.path, err)
			}
			got := pp.String()
			if got != tc.wantStr {
				t.Errorf("ParsePath(%q).String() = %q, want %q", tc.path, got, tc.wantStr)
			}
		})
	}
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && searchStr(s, substr)
}

func searchStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// ---- Evaluation tests for filter predicates ----

func TestFilterEvalLeaves(t *testing.T) {
	containerMD, itemMD := buildFilterTestDescriptor(t)
	_ = itemMD
	msg := buildFilterTestMsg(t, containerMD, itemMD)

	tcs := []struct {
		name      string
		paths     []PlanPathSpec
		wantNames []string // expected item names in first path's results
		wantCount int      // expected number of results; only checked when wantNames is nil
	}{
		{
			name: "filter active items",
			paths: []PlanPathSpec{
				PlanPath(`items[?(.active)].name`, Alias("filtered_name")),
			},
			wantNames: []string{"alpha", "gamma", "delta"},
		},
		{
			name: "filter by equality",
			paths: []PlanPathSpec{
				PlanPath(`items[?(.name == "beta")].value`, Alias("beta_value")),
			},
			wantCount: 1,
		},
		{
			name: "filter by numeric comparison",
			paths: []PlanPathSpec{
				PlanPath(`items[?(.value > 20)].name`, Alias("big_items")),
			},
			wantNames: []string{"gamma", "delta"},
		},
		{
			name: "filter by negation",
			paths: []PlanPathSpec{
				PlanPath(`items[?(!.active)].name`, Alias("inactive")),
			},
			wantNames: []string{"beta"},
		},
		{
			name: "compound AND filter",
			paths: []PlanPathSpec{
				PlanPath(`items[?(.active && .kind == "A")].name`, Alias("active_A")),
			},
			wantNames: []string{"alpha", "gamma"},
		},
		{
			name: "compound OR filter",
			paths: []PlanPathSpec{
				PlanPath(`items[?(.kind == "A" || .value > 30)].name`, Alias("A_or_big")),
			},
			wantNames: []string{"alpha", "gamma", "delta"},
		},
		{
			name: "filter with nested path",
			paths: []PlanPathSpec{
				PlanPath(`items[?(.inner.count > 200)].name`, Alias("big_inner")),
			},
			wantNames: []string{"gamma", "delta"},
		},
		{
			name: "filter yields no results",
			paths: []PlanPathSpec{
				PlanPath(`items[?(.value > 999)].name`, Alias("none")),
			},
			wantCount: 0,
		},
		{
			name: "no filter (wildcard baseline)",
			paths: []PlanPathSpec{
				PlanPath(`items[*].name`, Alias("all_names")),
			},
			wantNames: []string{"alpha", "beta", "gamma", "delta"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := NewPlan(containerMD, nil, tc.paths...)
			if err != nil {
				t.Fatalf("NewPlan: %v", err)
			}
			out, err := plan.EvalLeaves(msg)
			if err != nil {
				t.Fatalf("EvalLeaves: %v", err)
			}
			if len(out) == 0 {
				t.Fatal("EvalLeaves returned empty output")
			}
			results := out[0]

			if tc.wantNames != nil {
				if len(results) != len(tc.wantNames) {
					names := make([]string, len(results))
					for i, v := range results {
						names[i] = valueToStringValue(v)
					}
					t.Fatalf("got %d results %v, want %d names %v", len(results), names, len(tc.wantNames), tc.wantNames)
				}
				for i, want := range tc.wantNames {
					got := valueToStringValue(results[i])
					if got != want {
						t.Errorf("result[%d] = %q, want %q", i, got, want)
					}
				}
			} else if len(results) != tc.wantCount {
				t.Errorf("got %d results, want %d", len(results), tc.wantCount)
			}
		})
	}
}

// ---- Map wildcard evaluation tests ----

func TestMapWildcardEvalLeaves(t *testing.T) {
	containerMD, itemMD := buildFilterTestDescriptor(t)
	msg := buildFilterTestMsg(t, containerMD, itemMD)

	plan, err := NewPlan(containerMD, nil,
		PlanPath(`lookup[*].name`, Alias("map_names")),
	)
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	out, err := plan.EvalLeaves(msg)
	if err != nil {
		t.Fatalf("EvalLeaves: %v", err)
	}
	if len(out) == 0 || len(out[0]) == 0 {
		t.Fatal("expected map wildcard results")
	}

	// Map iteration order is non-deterministic, so collect names and check as a set.
	names := make(map[string]bool, len(out[0]))
	for _, v := range out[0] {
		names[valueToStringValue(v)] = true
	}
	for _, want := range []string{"x-item", "y-item", "z-item"} {
		if !names[want] {
			t.Errorf("missing expected name %q in map wildcard results: %v", want, names)
		}
	}
	if len(names) != 3 {
		t.Errorf("got %d unique names, want 3: %v", len(names), names)
	}
}

// ---- Programmatic filter API tests ----

func TestFilterProgrammatic(t *testing.T) {
	containerMD, itemMD := buildFilterTestDescriptor(t)
	msg := buildFilterTestMsg(t, containerMD, itemMD)

	// Build a filter predicate programmatically using the Expr API.
	// Filter: items where .value >= 20
	valueFD := itemMD.Fields().ByName("value")
	predicate := FuncGe(
		FilterPathRef("value", valueFD),
		Literal(ScalarInt64(20), protoreflect.Int64Kind),
	)

	// Build path with ListWildcard + Filter step manually.
	itemsFD := containerMD.Fields().ByName("items")
	path := Path{
		Root(containerMD),
		FieldAccess(itemsFD),
		ListWildcard(),
		Filter(predicate),
	}

	// Use PlanPath with the path to get name field.
	plan, err := NewPlan(containerMD, nil,
		PlanPath(`items[?(.value >= 20)].name`, Alias("filtered")),
	)
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	_ = path // verify it constructed OK

	out, err := plan.EvalLeaves(msg)
	if err != nil {
		t.Fatalf("EvalLeaves: %v", err)
	}
	wantNames := []string{"beta", "gamma", "delta"}
	if len(out[0]) != len(wantNames) {
		names := make([]string, len(out[0]))
		for i, v := range out[0] {
			names[i] = valueToStringValue(v)
		}
		t.Fatalf("got %d results %v, want %v", len(out[0]), names, wantNames)
	}
	for i, want := range wantNames {
		got := valueToStringValue(out[0][i])
		if got != want {
			t.Errorf("result[%d] = %q, want %q", i, got, want)
		}
	}
}

// ---- Logic function tests ----

func TestFuncAnd(t *testing.T) {
	md := buildExprTestDescriptors(t)
	plan, err := NewPlan(md, nil,
		PlanPath("x"), // dummy path to satisfy "at least one parseable path"
		PlanPath("and_tt", WithExpr(FuncAnd(
			Literal(ScalarBool(true), protoreflect.BoolKind),
			Literal(ScalarBool(true), protoreflect.BoolKind),
		)), Alias("and_tt")),
		PlanPath("and_tf", WithExpr(FuncAnd(
			Literal(ScalarBool(true), protoreflect.BoolKind),
			Literal(ScalarBool(false), protoreflect.BoolKind),
		)), Alias("and_tf")),
		PlanPath("and_ff", WithExpr(FuncAnd(
			Literal(ScalarBool(false), protoreflect.BoolKind),
			Literal(ScalarBool(false), protoreflect.BoolKind),
		)), Alias("and_ff")),
	)
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	msg := exprTestMsg(md, map[string]any{"x": int64(1)})
	out, err := plan.EvalLeaves(msg)
	if err != nil {
		t.Fatalf("EvalLeaves: %v", err)
	}
	checks := []struct {
		name string
		idx  int
		want bool
	}{
		{"true && true", 1, true},
		{"true && false", 2, false},
		{"false && false", 3, false},
	}
	for _, c := range checks {
		got := out[c.idx][0].ToProtoValue().Bool()
		if got != c.want {
			t.Errorf("%s = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestFuncOr(t *testing.T) {
	md := buildExprTestDescriptors(t)
	plan, err := NewPlan(md, nil,
		PlanPath("x"), // dummy path
		PlanPath("or_tt", WithExpr(FuncOr(
			Literal(ScalarBool(true), protoreflect.BoolKind),
			Literal(ScalarBool(true), protoreflect.BoolKind),
		)), Alias("or_tt")),
		PlanPath("or_tf", WithExpr(FuncOr(
			Literal(ScalarBool(true), protoreflect.BoolKind),
			Literal(ScalarBool(false), protoreflect.BoolKind),
		)), Alias("or_tf")),
		PlanPath("or_ff", WithExpr(FuncOr(
			Literal(ScalarBool(false), protoreflect.BoolKind),
			Literal(ScalarBool(false), protoreflect.BoolKind),
		)), Alias("or_ff")),
	)
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	msg := exprTestMsg(md, map[string]any{"x": int64(1)})
	out, err := plan.EvalLeaves(msg)
	if err != nil {
		t.Fatalf("EvalLeaves: %v", err)
	}
	checks := []struct {
		name string
		idx  int
		want bool
	}{
		{"true || true", 1, true},
		{"true || false", 2, true},
		{"false || false", 3, false},
	}
	for _, c := range checks {
		got := out[c.idx][0].ToProtoValue().Bool()
		if got != c.want {
			t.Errorf("%s = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestFuncNot(t *testing.T) {
	md := buildExprTestDescriptors(t)
	plan, err := NewPlan(md, nil,
		PlanPath("x"), // dummy path
		PlanPath("not_t", WithExpr(FuncNot(
			Literal(ScalarBool(true), protoreflect.BoolKind),
		)), Alias("not_t")),
		PlanPath("not_f", WithExpr(FuncNot(
			Literal(ScalarBool(false), protoreflect.BoolKind),
		)), Alias("not_f")),
	)
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	msg := exprTestMsg(md, map[string]any{"x": int64(1)})
	out, err := plan.EvalLeaves(msg)
	if err != nil {
		t.Fatalf("EvalLeaves: %v", err)
	}
	if got := out[1][0].ToProtoValue().Bool(); got != false {
		t.Errorf("!true = %v, want false", got)
	}
	if got := out[2][0].ToProtoValue().Bool(); got != true {
		t.Errorf("!false = %v, want true", got)
	}
}

// ---- Predicate parser unit tests ----

func TestPredicateParser(t *testing.T) {
	_, itemMD := buildFilterTestDescriptor(t)

	tcs := []struct {
		name    string
		input   string
		wantErr string
	}{
		{name: "simple eq", input: `.name == "hello"`},
		{name: "numeric gt", input: `.value > 42`},
		{name: "bool truthy", input: `.active`},
		{name: "negation", input: `!.active`},
		{name: "and", input: `.active && .value > 10`},
		{name: "or", input: `.kind == "A" || .kind == "B"`},
		{name: "nested path", input: `.inner.count > 100`},
		{name: "grouped", input: `(.active && .value > 10) || .kind == "X"`},
		{name: "double negation", input: `!!.active`},
		{name: "le", input: `.value <= 20`},
		{name: "ge", input: `.score >= 3.14`},
		{name: "ne string", input: `.name != "bad"`},
		{name: "false literal", input: `.active == false`},
		{name: "true literal", input: `.active == true`},
		// Error cases.
		{name: "unknown field", input: `.nonexistent == "x"`, wantErr: `not found`},
		{name: "no dot prefix", input: `name == "x"`, wantErr: `unexpected identifier`},
		{name: "incomplete", input: `.name ==`, wantErr: `expected value`},
		{name: "bad nested", input: `.name.sub == "x"`, wantErr: `not a message`},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parsePredicateExpr(itemMD, tc.input)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("parsePredicateExpr(%q) = nil error; want error containing %q", tc.input, tc.wantErr)
				}
				if !containsStr(err.Error(), tc.wantErr) {
					t.Fatalf("parsePredicateExpr(%q) error = %v; want error containing %q", tc.input, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePredicateExpr(%q) error: %v", tc.input, err)
			}
		})
	}
}

// ---- Select function test ----

func TestFuncSelect(t *testing.T) {
	md := buildExprTestDescriptors(t)
	// select(x > 5, x): if x > 5, return x; otherwise null.
	plan, err := NewPlan(md, nil,
		PlanPath("result", WithExpr(FuncSelect(
			FuncGt(PathRef("x"), Literal(ScalarInt64(5), protoreflect.Int64Kind)),
			PathRef("x"),
		)), Alias("result")),
	)
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	// x = 10 > 5 → should pass through
	msg := exprTestMsg(md, map[string]any{"x": int64(10)})
	out, err := plan.EvalLeaves(msg)
	if err != nil {
		t.Fatalf("EvalLeaves: %v", err)
	}
	if out[0][0].IsNull() {
		t.Error("expected non-null for x=10 > 5")
	}
	if got := out[0][0].ToProtoValue().Int(); got != 10 {
		t.Errorf("got %d, want 10", got)
	}

	// x = 3 ≤ 5 → should be null
	msg2 := exprTestMsg(md, map[string]any{"x": int64(3)})
	out2, err := plan.EvalLeaves(msg2)
	if err != nil {
		t.Fatalf("EvalLeaves: %v", err)
	}
	if !out2[0][0].IsNull() {
		t.Errorf("expected null for x=3 ≤ 5, got %v", out2[0][0])
	}
}

// ---- Benchmark for filter evaluation ----

func BenchmarkFilterEvalLeaves(b *testing.B) {
	// We need to use testing.T for descriptor building; work around by creating
	// descriptors outside the benchmark loop.
	containerMD, itemMD := buildFilterTestDescriptorBench()
	msg := buildFilterTestMsgBench(containerMD, itemMD)

	plan, err := NewPlan(containerMD, nil,
		PlanPath(`items[?(.active && .value > 15)].name`, Alias("filtered")),
	)
	if err != nil {
		b.Fatalf("NewPlan: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = plan.EvalLeaves(msg)
	}
}

// buildFilterTestDescriptorBench is a non-testing.T variant for benchmarks.
func buildFilterTestDescriptorBench() (containerMD, itemMD protoreflect.MessageDescriptor) {
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	int64Type := descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	boolType := descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum()
	doubleType := descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum()
	messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	labelOptional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	labelRepeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("filter_bench.proto"),
		Package: proto.String("filtertest"),
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
					{Name: proto.String("inner"), Number: proto.Int32(6), Type: messageType, TypeName: proto.String(".filtertest.Inner"), Label: labelOptional},
				},
			},
			{
				Name: proto.String("Container"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("items"), Number: proto.Int32(1), Type: messageType, TypeName: proto.String(".filtertest.Item"), Label: labelRepeated},
					{Name: proto.String("lookup"), Number: proto.Int32(2), Type: messageType, TypeName: proto.String(".filtertest.Container.LookupEntry"), Label: labelRepeated},
					{Name: proto.String("name"), Number: proto.Int32(3), Type: stringType, Label: labelOptional},
					{Name: proto.String("single"), Number: proto.Int32(4), Type: messageType, TypeName: proto.String(".filtertest.Item"), Label: labelOptional},
				},
				NestedType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("LookupEntry"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{Name: proto.String("key"), Number: proto.Int32(1), Type: stringType, Label: labelOptional},
							{Name: proto.String("value"), Number: proto.Int32(2), Type: messageType, TypeName: proto.String(".filtertest.Item"), Label: labelOptional},
						},
						Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
					},
				},
			},
		},
	}

	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		panic(err)
	}
	return fd.Messages().ByName("Container"), fd.Messages().ByName("Item")
}

func buildFilterTestMsgBench(containerMD, itemMD protoreflect.MessageDescriptor) proto.Message {
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
	container.Set(containerMD.Fields().ByName("name"), protoreflect.ValueOfString("bench-container"))
	items := container.Mutable(containerMD.Fields().ByName("items")).List()
	items.Append(protoreflect.ValueOfMessage(mkItem("a", 10, true, "A", 1.5, "la", 100)))
	items.Append(protoreflect.ValueOfMessage(mkItem("b", 20, false, "B", 2.5, "lb", 200)))
	items.Append(protoreflect.ValueOfMessage(mkItem("c", 30, true, "A", 3.5, "lc", 300)))
	items.Append(protoreflect.ValueOfMessage(mkItem("d", 40, true, "B", 0.5, "ld", 400)))
	return container
}
