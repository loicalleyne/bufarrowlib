package pbpath

import (
	"testing"
)

// ── Builtins at 0% coverage ─────────────────────────────────────────────

func TestPipeBuiltinRecurse(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	// recurse on a nested structure - use on a simple list
	results := helperExec(t, containerMD, msg, `{a: {b: 1}} | [recurse] | length`)
	if len(results) != 1 {
		t.Fatalf("recurse: got %d results", len(results))
	}
}

func TestPipeBuiltinRepeat(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	// repeat(. * 2) starting from 1, collect first few
	strs := helperExecStr(t, containerMD, msg, `[limit(5; 1 | repeat(. * 2))]`)
	if len(strs) != 1 {
		t.Fatalf("repeat: got %d results", len(strs))
	}
}

func TestPipeBuiltinCapture(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `"test-container" | capture("(?P<first>[a-z]+)-(?P<second>[a-z]+)")`)
	if len(results) != 1 {
		t.Fatalf("capture: got %d results", len(results))
	}
}

func TestPipeBuiltinCSV(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `["a", "b", "c"] | @csv`)
	if len(strs) != 1 {
		t.Fatalf("csv: got %d results", len(strs))
	}
}

func TestPipeBuiltinTSV(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `["a", "b", "c"] | @tsv`)
	if len(strs) != 1 {
		t.Fatalf("tsv: got %d results", len(strs))
	}
}

func TestPipeBuiltinLog(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `1 | log`)
	if len(strs) != 1 {
		t.Fatalf("log: got %d results", len(strs))
	}
}

func TestPipeBuiltinAscii(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `"A" | ascii`)
	if len(strs) != 1 || strs[0] != "65" {
		t.Fatalf("ascii: got %v; want [65]", strs)
	}

	// Empty string
	strs = helperExecStr(t, containerMD, msg, `"" | ascii`)
	if len(strs) != 1 || strs[0] != "null" {
		t.Fatalf("ascii empty: got %v; want [null]", strs)
	}
}

func TestPipeBuiltinInput(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	// input is a no-op placeholder, should return null
	strs := helperExecStr(t, containerMD, msg, `input`)
	if len(strs) != 1 || strs[0] != "null" {
		t.Fatalf("input: got %v; want [null]", strs)
	}
}

func TestPipeBuiltinInputs(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	// inputs is also a no-op placeholder
	results := helperExec(t, containerMD, msg, `[inputs]`)
	if len(results) != 1 {
		t.Fatalf("inputs: got %d results", len(results))
	}
}

func TestPipeBuiltinValuesArray(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	// values on an array
	results := helperExec(t, containerMD, msg, `[1, 2, 3] | [values]`)
	if len(results) != 1 {
		t.Fatalf("values array: got %d results", len(results))
	}
}

func TestPipeBuiltinAnyWith(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	// any(condition)
	strs := helperExecStr(t, containerMD, msg, `[1, 2, 3, 4] | any(. > 3)`)
	if len(strs) != 1 || strs[0] != "true" {
		t.Fatalf("any/1: got %v", strs)
	}
}

func TestPipeBuiltinAllWith(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[1, 2, 3, 4] | all(. > 0)`)
	if len(strs) != 1 || strs[0] != "true" {
		t.Fatalf("all/1: got %v", strs)
	}

	strs = helperExecStr(t, containerMD, msg, `[1, 2, 3, 4] | all(. > 2)`)
	if len(strs) != 1 || strs[0] != "false" {
		t.Fatalf("all/1 false: got %v", strs)
	}
}

func TestPipeBuiltinPathsFilter(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	// paths(scalars) - should filter paths to only scalar leaves
	results := helperExec(t, containerMD, msg, `.single | [paths(scalars)]`)
	if len(results) != 1 {
		t.Fatalf("paths(scalars): got %d results", len(results))
	}
}

// ── Path step coverage ──────────────────────────────────────────────────

func TestPathProtoStep(t *testing.T) {
	testMD, _ := buildTestDescriptors(t)
	pp, err := ParsePath(testMD, "nested.stringfield")
	if err != nil {
		t.Fatalf("ParsePath: %v", err)
	}
	// Access steps via the path
	if len(pp) < 2 {
		t.Fatalf("expected at least 2 steps, got %d", len(pp))
	}
	// Root step should have ProtoStep accessible
	step := pp.Index(0)
	_ = step.Kind()
}

func TestPathIndex(t *testing.T) {
	testMD, _ := buildTestDescriptors(t)

	// Test list index access
	pp, err := ParsePath(testMD, "repeats[0]")
	if err != nil {
		t.Fatalf("ParsePath: %v", err)
	}
	if len(pp) < 2 {
		t.Fatalf("expected at least 2 steps, got %d", len(pp))
	}
}

// ── Path AnyExpand ──────────────────────────────────────────────────────

func TestPathWildcard(t *testing.T) {
	testMD, _ := buildTestDescriptors(t)

	pp, err := ParsePath(testMD, "repeats[*]")
	if err != nil {
		t.Fatalf("ParsePath: %v", err)
	}
	if len(pp) < 2 {
		t.Fatalf("expected at least 2 steps, got %d", len(pp))
	}
}

// ── Value constructors and accessors additional coverage ────────────────

func TestValueKinds(t *testing.T) {
	// Ensure each kind is distinct
	if NullKind == ScalarKind || ScalarKind == ListKind || ListKind == MessageKind || MessageKind == ObjectKind {
		t.Fatal("value kinds should be distinct")
	}
}

func TestObjectValAccessors(t *testing.T) {
	o := ObjectVal([]ObjectEntry{
		{Key: "a", Value: ScalarInt64(1)},
		{Key: "b", Value: ScalarString("two")},
	})
	if o.Kind() != ObjectKind {
		t.Fatalf("Kind() = %v; want ObjectKind", o.Kind())
	}
	entries := o.Entries()
	if len(entries) != 2 {
		t.Fatalf("Entries() len = %d; want 2", len(entries))
	}
	if entries[0].Key != "a" || entries[1].Key != "b" {
		t.Fatal("Entries() keys mismatch")
	}
}

// ── Plan.AutoPromote / ExprInputEntries / InternalPath coverage ─────────

func TestPlanAutoPromote(t *testing.T) {
	testMD, _ := buildTestDescriptors(t)
	plan, err := NewPlan(testMD, []PlanOption{AutoPromote(true)},
		PlanPath("nested.stringfield"),
	)
	if err != nil {
		t.Fatalf("NewPlan with AutoPromote: %v", err)
	}
	if plan == nil {
		t.Fatal("plan is nil")
	}
}

func TestPlanExprInputEntries(t *testing.T) {
	testMD, _ := buildTestDescriptors(t)
	plan, err := NewPlan(testMD, nil,
		PlanPath("nested.stringfield"),
	)
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	// ExprInputEntries returns entries that have expr functions
	entries := plan.ExprInputEntries(0)
	// For a simple path, should be nil or empty
	_ = entries
}

func TestPlanInternalPath(t *testing.T) {
	testMD, _ := buildTestDescriptors(t)
	plan, err := NewPlan(testMD, nil,
		PlanPath("nested.stringfield", Alias("col")),
	)
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	// InternalPath returns the internal path for an entry by index
	p := plan.InternalPath(0)
	_ = p
	// Out of range should return nil
	p = plan.InternalPath(-1)
	if p != nil {
		t.Fatal("InternalPath(-1) should be nil")
	}
}

// ── PathValuesMulti ─────────────────────────────────────────────────────

func TestPathValuesMultiCoverage(t *testing.T) {
	testMD, nestedMD := buildTestDescriptors(t)

	msg1 := newTestWithNested(testMD, nestedMD, "first")
	msg2 := newTestWithNested(testMD, nestedMD, "second")

	// PathValuesMulti is a package-level function
	results, err := PathValuesMulti(testMD, msg1, PlanPath("nested.stringfield"))
	if err != nil {
		t.Fatalf("PathValuesMulti: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("PathValuesMulti: got %d results; want 1", len(results))
	}

	results, err = PathValuesMulti(testMD, msg2, PlanPath("nested.stringfield"))
	if err != nil {
		t.Fatalf("PathValuesMulti msg2: %v", err)
	}
	_ = results
}

// ── Additional low-coverage functions ───────────────────────────────────

func TestPipeObjMerge(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	// * operator for merging objects
	results := helperExec(t, containerMD, msg, `{a: 1} * {b: 2}`)
	if len(results) != 1 {
		t.Fatalf("obj merge: got %d results", len(results))
	}
}

func TestPipeObjRecursiveMerge(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `{a: {b: 1}} * {a: {c: 2}}`)
	if len(results) != 1 {
		t.Fatalf("recursive merge: got %d results", len(results))
	}
}

// ── Arithmetic ──────────────────────────────────────────────────────────

func TestPipeArithmeticOps(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		want     string
	}{
		{"add_ints", `5 + 3`, "8"},
		{"sub_ints", `10 - 3`, "7"},
		{"mul_ints", `4 * 3`, "12"},
		{"div_ints", `10 / 3`, "3"},
		{"mod_ints", `10 % 3`, "1"},
		{"add_floats", `2.5 + 1.5`, "4"},
		{"sub_floats", `5.0 - 1.5`, "3.5"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			strs := helperExecStr(t, containerMD, msg, tc.pipeline)
			if len(strs) != 1 || strs[0] != tc.want {
				t.Fatalf("got %v; want [%s]", strs, tc.want)
			}
		})
	}
}

// ── Comparison operators ────────────────────────────────────────────────

func TestPipeComparisonOps(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		want     string
	}{
		{"eq_true", `1 == 1`, "true"},
		{"eq_false", `1 == 2`, "false"},
		{"ne_true", `1 != 2`, "true"},
		{"lt_true", `1 < 2`, "true"},
		{"le_true", `2 <= 2`, "true"},
		{"gt_true", `3 > 2`, "true"},
		{"ge_true", `2 >= 2`, "true"},
		{"str_eq", `"a" == "a"`, "true"},
		{"str_lt", `"a" < "b"`, "true"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			strs := helperExecStr(t, containerMD, msg, tc.pipeline)
			if len(strs) != 1 || strs[0] != tc.want {
				t.Fatalf("got %v; want [%s]", strs, tc.want)
			}
		})
	}
}

// ── Type conversions ────────────────────────────────────────────────────

func TestPipeTypeConversions(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
	}{
		{"int_to_string", `42 | tostring`},
		{"string_to_number", `"42" | tonumber`},
		{"float_to_string", `3.14 | tostring`},
		{"bool_to_string", `true | tostring`},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			results := helperExec(t, containerMD, msg, tc.pipeline)
			if len(results) != 1 {
				t.Fatalf("got %d results; want 1", len(results))
			}
		})
	}
}

// ── Scan helpers ────────────────────────────────────────────────────────

func TestScanFloatEdgeCases(t *testing.T) {
	// These are already tested through pipe execution, but let's ensure
	// the scanner handles various number formats
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
	}{
		{"negative_int", `-5`},
		{"negative_float", `-3.14`},
		{"scientific", `1.5e2`},
		{"zero", `0`},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			results := helperExec(t, containerMD, msg, tc.pipeline)
			if len(results) != 1 {
				t.Fatalf("got %d results; want 1", len(results))
			}
		})
	}
}

// ── Value.RangeOpen / ListRangeStep3 coverage ───────────────────────────

func TestPathListRange(t *testing.T) {
	testMD, nestedMD := buildTestDescriptors(t)

	// Slice notation: repeats[0:2]
	pp, err := ParsePath(testMD, "repeats[0:2]")
	if err != nil {
		t.Fatalf("ParsePath: %v", err)
	}
	if len(pp) < 2 {
		t.Fatalf("expected >= 2 steps, got %d", len(pp))
	}

	// Build a message with repeating elements using PathValues via a plan
	msg := newTestWithNested(testMD, nestedMD, "a")
	results, err := PathValues(pp, msg)
	if err != nil {
		t.Fatalf("PathValues: %v", err)
	}
	_ = results
}
