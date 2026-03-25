package pbpath

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestNewPlan(t *testing.T) {
	testMD, _ := buildTestDescriptors(t)

	t.Run("nil md", func(t *testing.T) {
		_, err := NewPlan(nil, PlanPath("nested.stringfield"))
		if err == nil || !strings.Contains(err.Error(), "non-nil") {
			t.Fatalf("expected non-nil error, got: %v", err)
		}
	})

	t.Run("no paths", func(t *testing.T) {
		_, err := NewPlan(testMD)
		if err == nil || !strings.Contains(err.Error(), "at least one") {
			t.Fatalf("expected at-least-one error, got: %v", err)
		}
	})

	t.Run("parse error", func(t *testing.T) {
		_, err := NewPlan(testMD, PlanPath("no_such_field"))
		if err == nil {
			t.Fatalf("expected parse error, got nil")
		}
		if !strings.Contains(err.Error(), "no_such_field") {
			t.Fatalf("error should mention the bad path: %v", err)
		}
	})

	t.Run("multiple parse errors", func(t *testing.T) {
		_, err := NewPlan(testMD,
			PlanPath("bad1"),
			PlanPath("nested.stringfield"),
			PlanPath("bad2"),
		)
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "bad1") || !strings.Contains(err.Error(), "bad2") {
			t.Fatalf("error should mention both bad paths: %v", err)
		}
	})

	t.Run("valid single path", func(t *testing.T) {
		plan, err := NewPlan(testMD, PlanPath("nested.stringfield"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		entries := plan.Entries()
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		if entries[0].Name != "nested.stringfield" {
			t.Fatalf("expected name %q, got %q", "nested.stringfield", entries[0].Name)
		}
	})

	t.Run("alias overrides name", func(t *testing.T) {
		plan, err := NewPlan(testMD,
			PlanPath("nested.stringfield", Alias("col_a")),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if plan.Entries()[0].Name != "col_a" {
			t.Fatalf("expected alias col_a, got %q", plan.Entries()[0].Name)
		}
	})
}

func TestPlanEval(t *testing.T) {
	testMD, nestedMD := buildTestDescriptors(t)

	nestedFD := testMD.Fields().ByTextName("nested")
	repeatsFD := testMD.Fields().ByTextName("repeats")
	stringfieldFD := nestedMD.Fields().ByTextName("stringfield")
	_ = stringfieldFD // used in path assertions

	// Build test message:
	// Test {
	//   nested: { stringfield: "hello" }
	//   repeats: [
	//     Test{ nested: { stringfield: "a" } },
	//     Test{ nested: { stringfield: "b" } },
	//     Test{ nested: { stringfield: "c" } },
	//   ]
	// }
	simple := newTestWithNested(testMD, nestedMD, "hello")
	rep := dynamicpb.NewMessage(testMD)
	rep.Set(nestedFD, protoreflect.ValueOfMessage(newNested(nestedMD, "hello")))
	repList := rep.Mutable(repeatsFD).List()
	for _, v := range []string{"a", "b", "c"} {
		repList.Append(protoreflect.ValueOfMessage(newTestWithNested(testMD, nestedMD, v)))
	}

	t.Run("wrong message type", func(t *testing.T) {
		plan, err := NewPlan(testMD, PlanPath("nested.stringfield"))
		if err != nil {
			t.Fatal(err)
		}
		other := dynamicpb.NewMessage(nestedMD)
		_, err = plan.Eval(other)
		if err == nil || !strings.Contains(err.Error(), "want") {
			t.Fatalf("expected type mismatch error, got: %v", err)
		}
	})

	t.Run("single scalar path", func(t *testing.T) {
		plan, err := NewPlan(testMD, PlanPath("nested.stringfield"))
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.Eval(simple)
		if err != nil {
			t.Fatal(err)
		}
		if len(result) != 1 {
			t.Fatalf("expected 1 result slot, got %d", len(result))
		}
		if len(result[0]) != 1 {
			t.Fatalf("expected 1 branch, got %d", len(result[0]))
		}
		last := result[0][0].Index(-1)
		if last.Value.String() != "hello" {
			t.Fatalf("expected 'hello', got %q", last.Value.String())
		}
	})

	t.Run("shared prefix", func(t *testing.T) {
		// Two paths sharing "nested." prefix.
		plan, err := NewPlan(testMD,
			PlanPath("nested.stringfield", Alias("str")),
			PlanPath("nested.stringfield", Alias("str2")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.Eval(simple)
		if err != nil {
			t.Fatal(err)
		}
		if len(result) != 2 {
			t.Fatalf("expected 2 slots, got %d", len(result))
		}
		for i := 0; i < 2; i++ {
			if len(result[i]) != 1 {
				t.Fatalf("slot %d: expected 1 branch, got %d", i, len(result[i]))
			}
			last := result[i][0].Index(-1)
			if last.Value.String() != "hello" {
				t.Fatalf("slot %d: expected 'hello', got %q", i, last.Value.String())
			}
		}
	})

	t.Run("wildcard fan-out", func(t *testing.T) {
		plan, err := NewPlan(testMD,
			PlanPath("repeats[*].nested.stringfield"),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.Eval(rep)
		if err != nil {
			t.Fatal(err)
		}
		if len(result[0]) != 3 {
			t.Fatalf("expected 3 branches, got %d", len(result[0]))
		}
		for i, want := range []string{"a", "b", "c"} {
			got := result[0][i].Index(-1).Value.String()
			if got != want {
				t.Fatalf("branch %d: expected %q, got %q", i, want, got)
			}
		}
	})

	t.Run("range fan-out", func(t *testing.T) {
		plan, err := NewPlan(testMD,
			PlanPath("repeats[0:2].nested.stringfield"),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.Eval(rep)
		if err != nil {
			t.Fatal(err)
		}
		if len(result[0]) != 2 {
			t.Fatalf("expected 2 branches, got %d", len(result[0]))
		}
		for i, want := range []string{"a", "b"} {
			got := result[0][i].Index(-1).Value.String()
			if got != want {
				t.Fatalf("branch %d: expected %q, got %q", i, want, got)
			}
		}
	})

	t.Run("mixed wildcard and range on same field fork", func(t *testing.T) {
		// repeats[*] and repeats[0:2] should be separate trie branches.
		plan, err := NewPlan(testMD,
			PlanPath("repeats[*].nested.stringfield", Alias("all")),
			PlanPath("repeats[0:2].nested.stringfield", Alias("first_two")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.Eval(rep)
		if err != nil {
			t.Fatal(err)
		}
		if len(result[0]) != 3 {
			t.Fatalf("all: expected 3 branches, got %d", len(result[0]))
		}
		if len(result[1]) != 2 {
			t.Fatalf("first_two: expected 2 branches, got %d", len(result[1]))
		}
	})

	t.Run("empty fan-out produces empty slice", func(t *testing.T) {
		empty := dynamicpb.NewMessage(testMD)
		plan, err := NewPlan(testMD,
			PlanPath("repeats[*].nested.stringfield"),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.Eval(empty)
		if err != nil {
			t.Fatal(err)
		}
		if result[0] == nil {
			t.Fatalf("expected non-nil (empty) slice, got nil")
		}
		if len(result[0]) != 0 {
			t.Fatalf("expected 0 branches, got %d", len(result[0]))
		}
	})

	t.Run("strict clamped range errors", func(t *testing.T) {
		// repeats has 3 elements, range 0:10 clamps to 0:3.
		plan, err := NewPlan(testMD,
			PlanPath("repeats[0:10].nested.stringfield", StrictPath()),
		)
		if err != nil {
			t.Fatal(err)
		}
		_, err = plan.Eval(rep)
		if err == nil || !strings.Contains(err.Error(), "strict") {
			t.Fatalf("expected strict error, got: %v", err)
		}
	})

	t.Run("strict non-clamped range succeeds", func(t *testing.T) {
		plan, err := NewPlan(testMD,
			PlanPath("repeats[0:3].nested.stringfield", StrictPath()),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.Eval(rep)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(result[0]) != 3 {
			t.Fatalf("expected 3 branches, got %d", len(result[0]))
		}
	})

	t.Run("negative index", func(t *testing.T) {
		plan, err := NewPlan(testMD,
			PlanPath("repeats[-1].nested.stringfield"),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.Eval(rep)
		if err != nil {
			t.Fatal(err)
		}
		if len(result[0]) != 1 {
			t.Fatalf("expected 1 branch, got %d", len(result[0]))
		}
		got := result[0][0].Index(-1).Value.String()
		if got != "c" {
			t.Fatalf("expected 'c', got %q", got)
		}
	})

	t.Run("map access", func(t *testing.T) {
		// Test { strkeymap: { "k1": Nested{stringfield: "v1"} } }
		strkeymapFD := testMD.Fields().ByTextName("strkeymap")
		mm := dynamicpb.NewMessage(testMD)
		mf := mm.Mutable(strkeymapFD).Map()
		n := newNested(nestedMD, "v1")
		mf.Set(protoreflect.ValueOfString("k1").MapKey(), protoreflect.ValueOfMessage(n))

		plan, err := NewPlan(testMD,
			PlanPath(`strkeymap["k1"].stringfield`),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.Eval(mm)
		if err != nil {
			t.Fatal(err)
		}
		if len(result[0]) != 1 {
			t.Fatalf("expected 1 branch, got %d", len(result[0]))
		}
		got := result[0][0].Index(-1).Value.String()
		if got != "v1" {
			t.Fatalf("expected 'v1', got %q", got)
		}
	})

	t.Run("negative stride", func(t *testing.T) {
		plan, err := NewPlan(testMD,
			PlanPath("repeats[::-1].nested.stringfield"),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.Eval(rep)
		if err != nil {
			t.Fatal(err)
		}
		if len(result[0]) != 3 {
			t.Fatalf("expected 3 branches, got %d", len(result[0]))
		}
		for i, want := range []string{"c", "b", "a"} {
			got := result[0][i].Index(-1).Value.String()
			if got != want {
				t.Fatalf("branch %d: expected %q, got %q", i, want, got)
			}
		}
	})
}

func TestStepEqual(t *testing.T) {
	testMD, nestedMD := buildTestDescriptors(t)
	nestedFD := testMD.Fields().ByTextName("nested")
	repeatsFD := testMD.Fields().ByTextName("repeats")
	strkeymapFD := testMD.Fields().ByTextName("strkeymap")
	stringfieldFD := nestedMD.Fields().ByTextName("stringfield")

	tcs := []struct {
		name string
		a, b Step
		want bool
	}{
		{"root same", Root(testMD), Root(testMD), true},
		{"root diff", Root(testMD), Root(nestedMD), false},
		{"field same", FieldAccess(nestedFD), FieldAccess(nestedFD), true},
		{"field diff", FieldAccess(nestedFD), FieldAccess(repeatsFD), false},
		{"list idx same", ListIndex(0), ListIndex(0), true},
		{"list idx diff", ListIndex(0), ListIndex(1), false},
		{"list idx neg", ListIndex(-1), ListIndex(-1), true},
		{"list idx neg diff", ListIndex(-1), ListIndex(-2), false},
		{"wildcard", ListWildcard(), ListWildcard(), true},
		{"range same", ListRange(0, 3), ListRange(0, 3), true},
		{"range diff start", ListRange(0, 3), ListRange(1, 3), false},
		{"range diff end", ListRange(0, 3), ListRange(0, 4), false},
		{"range vs wildcard", ListRange(0, 3), ListWildcard(), false},
		{"field vs wildcard", FieldAccess(nestedFD), ListWildcard(), false},
		{"map same", MapIndex(protoreflect.ValueOfString("k").MapKey()), MapIndex(protoreflect.ValueOfString("k").MapKey()), true},
		{"map diff", MapIndex(protoreflect.ValueOfString("k").MapKey()), MapIndex(protoreflect.ValueOfString("j").MapKey()), false},
		{"field same stringfield", FieldAccess(stringfieldFD), FieldAccess(stringfieldFD), true},
		{"strkeymapFD", FieldAccess(strkeymapFD), FieldAccess(strkeymapFD), true},
		{"stride same", ListRangeStep3(0, 3, 2, false, false), ListRangeStep3(0, 3, 2, false, false), true},
		{"stride diff", ListRangeStep3(0, 3, 2, false, false), ListRangeStep3(0, 3, 1, false, false), false},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := stepEqual(tc.a, tc.b)
			if got != tc.want {
				t.Fatalf("stepEqual = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPathValuesMulti(t *testing.T) {
	testMD, nestedMD := buildTestDescriptors(t)
	msg := newTestWithNested(testMD, nestedMD, "multi")

	result, err := PathValuesMulti(testMD, msg,
		PlanPath("nested.stringfield", Alias("a")),
		PlanPath("nested.stringfield", Alias("b")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 slots, got %d", len(result))
	}
	for i := 0; i < 2; i++ {
		got := result[i][0].Index(-1).Value.String()
		if got != "multi" {
			t.Fatalf("slot %d: expected 'multi', got %q", i, got)
		}
	}
}

func TestPlanEntries(t *testing.T) {
	testMD, _ := buildTestDescriptors(t)
	plan, err := NewPlan(testMD,
		PlanPath("nested.stringfield", Alias("alias_a")),
		PlanPath("repeats[*].nested.stringfield"),
	)
	if err != nil {
		t.Fatal(err)
	}
	entries := plan.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].Name != "alias_a" {
		t.Fatalf("expected alias_a, got %q", entries[0].Name)
	}
	if entries[1].Name != "repeats[*].nested.stringfield" {
		t.Fatalf("expected raw path, got %q", entries[1].Name)
	}
	// Verify that the Path field is populated.
	if len(entries[0].Path) == 0 || len(entries[1].Path) == 0 {
		t.Fatal("expected non-empty paths in entries")
	}
}

func TestPlanEvalConsistency(t *testing.T) {
	// Verify that Plan.Eval and PathValues produce equivalent results
	// for scalar and fan-out paths.
	testMD, nestedMD := buildTestDescriptors(t)
	repeatsFD := testMD.Fields().ByTextName("repeats")

	msg := dynamicpb.NewMessage(testMD)
	msg.Set(testMD.Fields().ByTextName("nested"), protoreflect.ValueOfMessage(newNested(nestedMD, "top")))
	list := msg.Mutable(repeatsFD).List()
	for _, v := range []string{"x", "y", "z"} {
		list.Append(protoreflect.ValueOfMessage(newTestWithNested(testMD, nestedMD, v)))
	}

	paths := []string{
		"nested.stringfield",
		"repeats[*].nested.stringfield",
		"repeats[0:2].nested.stringfield",
		"repeats[-1].nested.stringfield",
	}

	plan, err := NewPlan(testMD, func() []PlanPathSpec {
		specs := make([]PlanPathSpec, len(paths))
		for i, p := range paths {
			specs[i] = PlanPath(p)
		}
		return specs
	}()...)
	if err != nil {
		t.Fatal(err)
	}

	planResults, err := plan.Eval(msg)
	if err != nil {
		t.Fatal(err)
	}

	// Compare with PathValues.
	for i, pathStr := range paths {
		pp, err := ParsePath(testMD, pathStr)
		if err != nil {
			t.Fatalf("path %q: parse: %v", pathStr, err)
		}
		pvResults, err := PathValues(pp, msg)
		if err != nil {
			t.Fatalf("path %q: PathValues: %v", pathStr, err)
		}

		// Compare last values.
		if len(planResults[i]) != len(pvResults) {
			t.Fatalf("path %q: plan produced %d branches, PathValues produced %d",
				pathStr, len(planResults[i]), len(pvResults))
		}
		for j := range planResults[i] {
			planLast := planResults[i][j].Index(-1).Value
			pvLast := pvResults[j].Index(-1).Value
			if diff := cmp.Diff(planLast.Interface(), pvLast.Interface()); diff != "" {
				t.Fatalf("path %q branch %d: mismatch (-plan +pv):\n%s", pathStr, j, diff)
			}
		}
	}
}
