package pbpath

import (
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ── NewQuery / Plan accessor ────────────────────────────────────────────

func TestNewQuery(t *testing.T) {
	testMD, _ := buildTestDescriptors(t)

	plan, err := NewPlan(testMD, nil, PlanPath("nested.stringfield"))
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}

	q := NewQuery(plan)
	if q == nil {
		t.Fatal("NewQuery returned nil")
	}
	if q.Plan() != plan {
		t.Fatal("Query.Plan() does not match the input plan")
	}
}

// ── Query.Run ───────────────────────────────────────────────────────────

func TestQueryRun(t *testing.T) {
	testMD, nestedMD := buildTestDescriptors(t)

	plan, err := NewPlan(testMD, nil,
		PlanPath("nested.stringfield", Alias("col_a")),
	)
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}

	msg := newTestWithNested(testMD, nestedMD, "hello")

	q := NewQuery(plan)
	rs, err := q.Run(msg)
	if err != nil {
		t.Fatalf("Query.Run: %v", err)
	}

	if rs.Len() != 1 {
		t.Fatalf("expected 1 result entry, got %d", rs.Len())
	}
	if !rs.Has("col_a") {
		t.Fatal("ResultSet.Has(col_a) = false; want true")
	}
	r := rs.Get("col_a")
	if r.Len() != 1 {
		t.Fatalf("expected 1 value, got %d", r.Len())
	}
	if got := r.String(); got != "hello" {
		t.Fatalf("expected %q, got %q", "hello", got)
	}
}

func TestQueryRunEmptyMessage(t *testing.T) {
	testMD, _ := buildTestDescriptors(t)

	plan, err := NewPlan(testMD, nil,
		PlanPath("nested.stringfield"),
	)
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}

	msg := dynamicpb.NewMessage(testMD)
	q := NewQuery(plan)
	rs, err := q.Run(msg)
	if err != nil {
		t.Fatalf("Query.Run: %v", err)
	}

	r := rs.Get("nested.stringfield")
	if got := r.String(); got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
}

// ── Query.RunConcurrent ─────────────────────────────────────────────────

func TestQueryRunConcurrent(t *testing.T) {
	testMD, nestedMD := buildTestDescriptors(t)

	plan, err := NewPlan(testMD, nil,
		PlanPath("nested.stringfield", Alias("col_a")),
	)
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}

	q := NewQuery(plan)

	const goroutines = 8
	var wg sync.WaitGroup
	wg.Add(goroutines)
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(val string) {
			defer wg.Done()
			msg := newTestWithNested(testMD, nestedMD, val)
			rs, err := q.RunConcurrent(msg)
			if err != nil {
				errs <- err
				return
			}
			r := rs.Get("col_a")
			if got := r.String(); got != val {
				errs <- err
			}
		}("value")
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatalf("concurrent error: %v", e)
	}
}

// ── ResultSet accessors ─────────────────────────────────────────────────

func TestResultSetAccessors(t *testing.T) {
	testMD, nestedMD := buildTestDescriptors(t)

	plan, err := NewPlan(testMD, nil,
		PlanPath("nested.stringfield", Alias("first")),
	)
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}

	msg := newTestWithNested(testMD, nestedMD, "test-value")
	q := NewQuery(plan)
	rs, err := q.Run(msg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Len
	if rs.Len() != 1 {
		t.Fatalf("Len() = %d; want 1", rs.Len())
	}

	// Has
	if !rs.Has("first") {
		t.Fatal("Has(first) = false; want true")
	}
	if rs.Has("nonexistent") {
		t.Fatal("Has(nonexistent) = true; want false")
	}

	// Get missing key returns empty Result
	empty := rs.Get("no_such_key")
	if !empty.IsEmpty() {
		t.Fatal("Get for missing key should be empty")
	}

	// At
	name, result := rs.At(0)
	if name != "first" {
		t.Fatalf("At(0) name = %q; want %q", name, "first")
	}
	if result.IsEmpty() {
		t.Fatal("At(0) result should not be empty")
	}

	// Names
	names := rs.Names()
	if len(names) != 1 || names[0] != "first" {
		t.Fatalf("Names() = %v; want [first]", names)
	}

	// All iterator
	count := 0
	for n, r := range rs.All() {
		if n != "first" {
			t.Fatalf("All() yielded name %q; want first", n)
		}
		if r.IsEmpty() {
			t.Fatal("All() yielded empty result")
		}
		count++
	}
	if count != 1 {
		t.Fatalf("All() yielded %d pairs; want 1", count)
	}
}

// ── QueryEval convenience ───────────────────────────────────────────────

func TestQueryEval(t *testing.T) {
	testMD, nestedMD := buildTestDescriptors(t)
	msg := newTestWithNested(testMD, nestedMD, "qeval")

	r, err := QueryEval(testMD, "nested.stringfield", msg)
	if err != nil {
		t.Fatalf("QueryEval: %v", err)
	}
	if got := r.String(); got != "qeval" {
		t.Fatalf("QueryEval result = %q; want %q", got, "qeval")
	}
}

func TestQueryEvalBadPath(t *testing.T) {
	testMD, nestedMD := buildTestDescriptors(t)
	msg := newTestWithNested(testMD, nestedMD, "x")

	_, err := QueryEval(testMD, "nonexistent_field", msg)
	if err == nil {
		t.Fatal("expected error for bad path, got nil")
	}
	if !strings.Contains(err.Error(), "QueryEval") {
		t.Fatalf("error should mention QueryEval: %v", err)
	}
}

// ── Multi-path queries ──────────────────────────────────────────────────

func TestQueryMultiPath(t *testing.T) {
	containerMD, _ := buildFilterTestDescriptor(t)
	itemMD := containerMD.Fields().ByName("items").Message()
	msg := buildFilterTestMsg(t, containerMD, itemMD)

	plan, err := NewPlan(containerMD, nil,
		PlanPath("name", Alias("container_name")),
		PlanPath("single.name", Alias("single_name")),
		PlanPath("single.value", Alias("single_value")),
	)
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}

	q := NewQuery(plan)
	rs, err := q.Run(msg.(proto.Message))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if rs.Len() != 3 {
		t.Fatalf("Len() = %d; want 3", rs.Len())
	}

	if got := rs.Get("container_name").String(); got != "test-container" {
		t.Fatalf("container_name = %q; want %q", got, "test-container")
	}
	if got := rs.Get("single_name").String(); got != "solo" {
		t.Fatalf("single_name = %q; want %q", got, "solo")
	}
	if got := rs.Get("single_value").Int64(); got != 99 {
		t.Fatalf("single_value = %d; want 99", got)
	}

	names := rs.Names()
	if len(names) != 3 {
		t.Fatalf("Names() len = %d; want 3", len(names))
	}
}

// ── Zero ResultSet ──────────────────────────────────────────────────────

func TestResultSetZeroValue(t *testing.T) {
	var rs ResultSet
	if rs.Len() != 0 {
		t.Fatalf("zero ResultSet Len() = %d; want 0", rs.Len())
	}
	if rs.Has("anything") {
		t.Fatal("zero ResultSet Has() = true; want false")
	}
	r := rs.Get("anything")
	if !r.IsEmpty() {
		t.Fatal("zero ResultSet Get() should return empty Result")
	}
	names := rs.Names()
	if len(names) != 0 {
		t.Fatalf("zero ResultSet Names() = %v; want nil", names)
	}
	count := 0
	for range rs.All() {
		count++
	}
	if count != 0 {
		t.Fatalf("zero ResultSet All() yielded %d; want 0", count)
	}
}

// ── buildFilterTestMsg helper returns proto.Message ─────────────────────

func helperBuildContainerMsg(t *testing.T) (protoreflect.MessageDescriptor, proto.Message) {
	t.Helper()
	containerMD, _ := buildFilterTestDescriptor(t)
	itemMD := containerMD.Fields().ByName("items").Message()
	return containerMD, buildFilterTestMsg(t, containerMD, itemMD)
}
