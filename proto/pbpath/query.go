package pbpath

import (
	"fmt"
	"iter"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Query is a pre-compiled, reusable query object built from a [Plan].
// It wraps the plan and presents evaluation results through the typed
// [ResultSet] / [Result] API instead of raw protoreflect.Value slices.
//
// Use [NewQuery] to create a Query from an existing plan, or
// [QueryEval] for a one-shot evaluation without pre-compilation.
type Query struct {
	plan *Plan
}

// NewQuery creates a [Query] that wraps plan. The plan must already be
// compiled via [NewPlan]. The Query does not own the plan; the caller is
// responsible for ensuring the plan outlives the query.
func NewQuery(plan *Plan) *Query {
	return &Query{plan: plan}
}

// Plan returns the underlying [Plan].
func (q *Query) Plan() *Plan { return q.plan }

// Run evaluates the query against msg and returns a [ResultSet] keyed by
// each path entry's name (alias or path string).
//
// Run reuses the plan's internal scratch buffers (via [Plan.EvalLeaves])
// and is therefore NOT safe for concurrent use. Use [RunConcurrent] when
// calling from multiple goroutines.
func (q *Query) Run(msg proto.Message) (ResultSet, error) {
	raw, err := q.plan.EvalLeaves(msg)
	if err != nil {
		return ResultSet{}, err
	}
	return buildResultSet(q.plan, raw), nil
}

// RunConcurrent is like [Run] but allocates fresh buffers per call,
// making it safe for concurrent use by multiple goroutines.
func (q *Query) RunConcurrent(msg proto.Message) (ResultSet, error) {
	raw, err := q.plan.EvalLeavesConcurrent(msg)
	if err != nil {
		return ResultSet{}, err
	}
	return buildResultSet(q.plan, raw), nil
}

// buildResultSet converts raw EvalLeaves output into a ResultSet.
func buildResultSet(plan *Plan, raw [][]Value) ResultSet {
	entries := plan.Entries()
	rs := ResultSet{
		results: make([]namedResult, len(entries)),
		index:   make(map[string]int, len(entries)),
	}
	for i, entry := range entries {
		rs.results[i] = namedResult{
			name:   entry.Name,
			result: NewResult(raw[i]),
		}
		rs.index[entry.Name] = i
	}
	return rs
}

// ── ResultSet ───────────────────────────────────────────────────────────

// namedResult pairs an entry name with its result.
type namedResult struct {
	name   string
	result Result
}

// ResultSet is an ordered collection of named [Result] values returned by
// a [Query.Run] call. Results can be accessed by name or iterated in order.
//
// The zero value is a valid, empty result set.
type ResultSet struct {
	results []namedResult
	index   map[string]int // name → position
}

// Len returns the number of entries in the result set.
func (rs ResultSet) Len() int { return len(rs.results) }

// Get returns the [Result] for the given entry name.
// If the name does not exist, an empty [Result] is returned.
func (rs ResultSet) Get(name string) Result {
	if idx, ok := rs.index[name]; ok {
		return rs.results[idx].result
	}
	return Result{}
}

// Has reports whether the result set contains an entry with the given name.
func (rs ResultSet) Has(name string) bool {
	_, ok := rs.index[name]
	return ok
}

// At returns the [Result] and name at position i. Panics if i is out of range.
func (rs ResultSet) At(i int) (name string, result Result) {
	nr := rs.results[i]
	return nr.name, nr.result
}

// Names returns the entry names in order.
func (rs ResultSet) Names() []string {
	out := make([]string, len(rs.results))
	for i, nr := range rs.results {
		out[i] = nr.name
	}
	return out
}

// All returns an iterator over all (name, result) pairs in order.
// Intended for use with Go range-over-func.
func (rs ResultSet) All() iter.Seq2[string, Result] {
	return func(yield func(string, Result) bool) {
		for _, nr := range rs.results {
			if !yield(nr.name, nr.result) {
				return
			}
		}
	}
}

// ── One-shot convenience ────────────────────────────────────────────────

// QueryEval is a convenience function that compiles a single path against md,
// evaluates it against msg, and returns the typed [Result].
//
// For repeated evaluation of the same path against many messages, prefer
// [NewPlan] + [NewQuery] + [Query.Run].
func QueryEval(md protoreflect.MessageDescriptor, path string, msg proto.Message) (Result, error) {
	plan, err := NewPlan(md, nil, PlanPath(path))
	if err != nil {
		return Result{}, fmt.Errorf("pbpath.QueryEval: %w", err)
	}
	raw, err := plan.EvalLeavesConcurrent(msg)
	if err != nil {
		return Result{}, fmt.Errorf("pbpath.QueryEval: %w", err)
	}
	if len(raw) == 0 {
		return Result{}, nil
	}
	return NewResult(raw[0]), nil
}
