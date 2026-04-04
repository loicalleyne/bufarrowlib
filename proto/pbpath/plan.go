package pbpath

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Plan is an immutable, pre-compiled bundle of [Path] values ready for
// repeated evaluation against messages of a single type.
//
// Paths that share a common prefix are traversed once through the shared
// segment, then forked — giving an O(1) cost per shared step rather than
// O(P) where P is the number of paths.
//
// [Plan.Eval] and [Plan.EvalLeavesConcurrent] are safe for concurrent use
// by multiple goroutines.
//
// [Plan.EvalLeaves] reuses internal scratch buffers and is NOT safe for
// concurrent use; it is the preferred method when called from a single
// goroutine (e.g. inside [Transcoder.AppendDenorm]).
type Plan struct {
	root    *planNode
	md      protoreflect.MessageDescriptor
	entries []planEntry
	cfg     planConfig

	// userCount is the number of user-visible entries (first N in entries).
	// entries[userCount:] are hidden leaf paths used by Expr trees.
	userCount int

	// scratch is reused by EvalLeaves to avoid per-call allocations.
	// It is lazily initialised on first EvalLeaves call.
	scratch *leafScratch
}

// leafBranch is a lightweight branch tracked during EvalLeaves traversal.
// Unlike [Values], it carries only the current cursor value — not the full
// path chain — eliminating O(depth × branches) clonePath/cloneValues allocs.
type leafBranch struct {
	cursor  Value // the value at the current trie node
	clamped bool  // range/index was clamped during traversal
}

// leafScratch holds pre-allocated buffers for [Plan.EvalLeaves].
type leafScratch struct {
	out  [][]Value     // one slot per path entry
	seed [1]leafBranch // avoid heap alloc for the single root branch
}

// PlanEntry exposes metadata about one compiled path inside a [Plan].
type PlanEntry struct {
	// Name is the alias if one was set via [Alias], otherwise the original
	// path string.
	Name string
	// Path is the compiled [Path]. Nil for Expr-only entries.
	Path Path
	// OutputKind is the protoreflect.Kind of the expression's result,
	// or zero when no [WithExpr] was set (raw path leaf kind) or when the
	// Expr is pass-through (same kind as input leaf).
	OutputKind protoreflect.Kind
	// HasExpr is true when this entry was created with [WithExpr].
	HasExpr bool
}

// planEntry is the internal per-path state.
type planEntry struct {
	name   string
	path   Path
	strict bool
	expr   Expr // optional computed expression (nil → raw path leaf)
}

// planNode is a node in the traversal trie.
type planNode struct {
	step     Step
	desc     protoreflect.Descriptor // schema cursor at this point
	children []*planNode
	leafIDs  []int // indices into Plan.entries for paths that terminate here
}

// ---- PlanPathSpec and PlanOption ----

// PlanPathSpec pairs a raw path string with per-path options.
// Create one with [PlanPath].
type PlanPathSpec struct {
	path string
	opts planEntryOpts
}

type planEntryOpts struct {
	strict bool
	alias  string
	expr   Expr // optional expression to evaluate after path resolution
}

// EntryOption configures a single path entry inside a [Plan].
type EntryOption func(*planEntryOpts)

// StrictPath returns an [EntryOption] that makes this path's evaluation return
// an error when a range or index is clamped due to the list being shorter
// than the requested bound. Without StrictPath, out-of-bounds accesses are
// silently clamped or skipped.
func StrictPath() EntryOption { return func(o *planEntryOpts) { o.strict = true } }

// Alias returns an [EntryOption] that gives this path entry a human-readable
// name. The alias is returned by [Plan.Entries] and is useful for mapping
// paths to output column names.
func Alias(name string) EntryOption { return func(o *planEntryOpts) { o.alias = name } }

// WithExpr returns an [EntryOption] that attaches a composable [Expr] to a path
// entry. The expr tree's leaf [PathRef] nodes are resolved against the plan's
// trie during compilation; at evaluation time the function tree is applied to
// the resolved leaf values.
//
// When WithExpr is set the path string passed to [PlanPath] is ignored for
// traversal purposes — all paths come from the Expr tree's leaves. The
// PlanPath path string (or [Alias]) is still used as the entry name.
func WithExpr(expr Expr) EntryOption { return func(o *planEntryOpts) { o.expr = expr } }

// PlanPath creates a [PlanPathSpec] pairing a path string with per-path options.
func PlanPath(path string, opts ...EntryOption) PlanPathSpec {
	var o planEntryOpts
	for _, fn := range opts {
		fn(&o)
	}
	return PlanPathSpec{path: path, opts: o}
}

// ---- Plan-level options ----

// planConfig holds plan-level configuration.
type planConfig struct {
	autoPromote bool
}

// PlanOption configures plan-level behaviour for [NewPlan].
type PlanOption func(*planConfig)

// AutoPromote returns a [PlanOption] that sets the default auto-promotion
// behaviour for [Cond] expressions. When true, Cond branches with
// mismatched output kinds are automatically promoted to a common type.
// Individual Cond expressions can override this setting.
func AutoPromote(on bool) PlanOption { return func(c *planConfig) { c.autoPromote = on } }

// ---- Construction ----

// NewPlan compiles one or more path strings against md into an immutable [Plan].
// Parsing and trie construction happen once; [Plan.Eval] is the hot path.
//
// opts may be nil when no plan-level options are needed.
// All paths must be rooted at the same message descriptor md.
// Returns an error that bundles all parse failures.
func NewPlan(md protoreflect.MessageDescriptor, opts []PlanOption, paths ...PlanPathSpec) (*Plan, error) {
	if md == nil {
		return nil, fmt.Errorf("pbpath.NewPlan: message descriptor must be non-nil")
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("pbpath.NewPlan: at least one path is required")
	}

	var cfg planConfig
	for _, o := range opts {
		o(&cfg)
	}

	userCount := len(paths)

	// Collect all path specs: user-visible first, then hidden Expr leaf paths.
	// Use a map to deduplicate hidden leaf paths that appear in multiple Exprs
	// (or that duplicate a user-visible path).
	allSpecs := make([]PlanPathSpec, userCount)
	copy(allSpecs, paths)

	// pathIndex maps raw path strings to their entry index (for dedup).
	pathIndex := make(map[string]int, userCount)
	for i, spec := range paths {
		if spec.opts.expr == nil {
			pathIndex[spec.path] = i
		}
	}

	// For each Expr entry, collect leaf paths and resolve entry indices.
	for i, spec := range paths {
		if spec.opts.expr == nil {
			continue
		}
		leaves := resolvePathExprs(spec.opts.expr)
		for _, leaf := range leaves {
			if idx, ok := pathIndex[leaf.path]; ok {
				leaf.entryIdx = idx
				continue
			}
			// New hidden leaf path — add to the end.
			idx := len(allSpecs)
			pathIndex[leaf.path] = idx
			allSpecs = append(allSpecs, PlanPath(leaf.path))
			leaf.entryIdx = idx
		}
		_ = i // suppress unused
	}

	p := &Plan{md: md, entries: make([]planEntry, len(allSpecs)), cfg: cfg, userCount: userCount}

	// Parse all paths, collecting errors.
	// Entries with an Expr are not parsed — their leaf paths were already
	// added as hidden entries in allSpecs.
	var errs []string
	parsed := make([]Path, len(allSpecs))
	for i, spec := range allSpecs {
		if spec.opts.expr != nil {
			// Expr entry: use alias (or path string) as name only.
			name := spec.opts.alias
			if name == "" {
				name = spec.path
			}
			p.entries[i] = planEntry{
				name: name,
				expr: spec.opts.expr,
			}
			continue
		}
		pp, err := ParsePath(md, spec.path)
		if err != nil {
			errs = append(errs, fmt.Sprintf("path %d (%q): %v", i, spec.path, err))
			continue
		}
		parsed[i] = pp
		name := spec.opts.alias
		if name == "" {
			name = spec.path
		}
		p.entries[i] = planEntry{
			name:   name,
			path:   pp,
			strict: spec.opts.strict,
			expr:   spec.opts.expr,
		}
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("pbpath.NewPlan: %s", strings.Join(errs, "; "))
	}

	// Resolve EnumDescriptors for any FuncEnumName nodes in user Exprs.
	// This must happen after paths are parsed so we can look up terminal FDs.
	if err := resolveEnumDescs(p.entries[:userCount], parsed); err != nil {
		return nil, err
	}

	// Build the trie. The root planNode represents the RootStep.
	// Find the first parsed path to get the RootStep.
	var rootPath Path
	for _, pp := range parsed {
		if len(pp) > 0 {
			rootPath = pp
			break
		}
	}
	if len(rootPath) == 0 {
		return nil, fmt.Errorf("pbpath.NewPlan: no parseable paths (all entries are Expr-only with no leaf paths)")
	}
	p.root = &planNode{
		step: rootPath[0],
		desc: md,
	}
	for i, pp := range parsed {
		if len(pp) == 0 {
			continue // Expr entry — no path to insert
		}
		if err := p.insertPath(pp, i); err != nil {
			return nil, fmt.Errorf("pbpath.NewPlan: path %d: %v", i, err)
		}
	}
	return p, nil
}

// insertPath inserts a parsed path into the trie, creating nodes as needed.
func (p *Plan) insertPath(pp Path, leafID int) error {
	cur := p.root
	// Skip the root step (index 0) — it's already the trie root.
	for i := 1; i < len(pp); i++ {
		step := pp[i]
		// Try to find an existing child with the same step.
		var found *planNode
		for _, ch := range cur.children {
			if stepEqual(ch.step, step) {
				found = ch
				break
			}
		}
		if found == nil {
			// Compute the descriptor for this new node.
			desc, err := advanceDesc(cur.desc, step)
			if err != nil {
				return err
			}
			found = &planNode{step: step, desc: desc}
			cur.children = append(cur.children, found)
		}
		cur = found
	}
	cur.leafIDs = append(cur.leafIDs, leafID)
	return nil
}

// advanceDesc advances a schema descriptor by one step, mirroring the
// descriptor-tracking logic in [PathValues].
func advanceDesc(desc protoreflect.Descriptor, step Step) (protoreflect.Descriptor, error) {
	switch step.Kind() {
	case FieldAccessStep:
		if f, ok := desc.(protoreflect.FieldDescriptor); ok {
			desc = f.Message()
		}
		md, ok := desc.(protoreflect.MessageDescriptor)
		if !ok {
			return nil, fmt.Errorf("expected MessageDescriptor, got %T", desc)
		}
		fd := step.FieldDescriptor()
		d := md.Fields().ByNumber(fd.Number())
		if d == nil {
			return nil, fmt.Errorf("message missing field %v", fd.Number())
		}
		return d, nil

	case ListIndexStep, ListWildcardStep, ListRangeStep:
		fd, ok := desc.(protoreflect.FieldDescriptor)
		if !ok {
			return nil, fmt.Errorf("expected FieldDescriptor for list step, got %T", desc)
		}
		// After indexing into a list, the descriptor is the element type.
		// For message lists, that's fd.Message(); for scalar lists, fd itself
		// remains the descriptor (but there won't be further field accesses).
		if fd.Message() != nil {
			return fd.Message(), nil
		}
		return fd, nil

	case MapIndexStep:
		fd, ok := desc.(protoreflect.FieldDescriptor)
		if !ok {
			return nil, fmt.Errorf("expected FieldDescriptor for map step, got %T", desc)
		}
		return fd.MapValue(), nil

	case AnyExpandStep:
		return step.MessageDescriptor(), nil

	case FilterStep:
		// A FilterStep does not change the schema cursor — it merely
		// filters branches. The descriptor stays the same.
		return desc, nil

	case MapWildcardStep:
		fd, ok := desc.(protoreflect.FieldDescriptor)
		if !ok {
			return nil, fmt.Errorf("expected FieldDescriptor for map wildcard step, got %T", desc)
		}
		if fd.MapValue().Message() != nil {
			return fd.MapValue().Message(), nil
		}
		return fd.MapValue(), nil

	default:
		return nil, fmt.Errorf("unsupported step kind %d in trie construction", step.Kind())
	}
}

// ---- Step equality ----

// stepEqual reports whether two steps are structurally identical and should
// be merged in the trie.
func stepEqual(a, b Step) bool {
	if a.Kind() != b.Kind() {
		return false
	}
	switch a.Kind() {
	case RootStep:
		return a.MessageDescriptor().FullName() == b.MessageDescriptor().FullName()
	case FieldAccessStep:
		return a.FieldDescriptor().Number() == b.FieldDescriptor().Number()
	case ListIndexStep:
		return a.ListIndex() == b.ListIndex()
	case MapIndexStep:
		return a.MapIndex().Interface() == b.MapIndex().Interface()
	case AnyExpandStep:
		return a.MessageDescriptor().FullName() == b.MessageDescriptor().FullName()
	case ListWildcardStep:
		return true // all wildcards are equal
	case ListRangeStep:
		return a.RangeStart() == b.RangeStart() &&
			a.RangeEnd() == b.RangeEnd() &&
			a.RangeStep() == b.RangeStep() &&
			a.StartOmitted() == b.StartOmitted() &&
			a.EndOmitted() == b.EndOmitted()
	case FilterStep:
		// FilterSteps are never merged in the trie — each is unique
		// because predicates can differ even with the same syntax.
		return false
	case MapWildcardStep:
		return true // all map wildcards are equal
	default:
		return false
	}
}

// ---- Introspection ----

// Entries returns metadata for each compiled path, in the order they were
// provided to [NewPlan].
func (p *Plan) Entries() []PlanEntry {
	out := make([]PlanEntry, p.userCount)
	for i := range p.userCount {
		e := p.entries[i]
		var ok protoreflect.Kind
		if e.expr != nil {
			ok = e.expr.outputKind()
		}
		out[i] = PlanEntry{Name: e.name, Path: e.path, OutputKind: ok, HasExpr: e.expr != nil}
	}
	return out
}

// ExprInputEntries returns the plan-entry indices of all leaf [PathRef] nodes
// referenced by the Expr on user-visible entry idx. These are indices into
// the full internal entries slice (which may include hidden leaf paths appended
// after the user-visible entries).
//
// Returns nil when the entry has no Expr. Callers can use [Plan.InternalPath]
// to retrieve the compiled path for each returned index.
func (p *Plan) ExprInputEntries(idx int) []int {
	if idx < 0 || idx >= p.userCount {
		return nil
	}
	e := p.entries[idx]
	if e.expr == nil {
		return nil
	}
	leaves := resolvePathExprs(e.expr)
	out := make([]int, len(leaves))
	for i, leaf := range leaves {
		out[i] = leaf.entryIdx
	}
	return out
}

// InternalPath returns the compiled [Path] for the given internal entry index.
// Unlike [Entries] (which exposes only user-visible entries), this method can
// access hidden leaf paths that were added to the trie for [Expr] evaluation.
// Returns nil if idx is out of range.
func (p *Plan) InternalPath(idx int) Path {
	if idx < 0 || idx >= len(p.entries) {
		return nil
	}
	return p.entries[idx].path
}

// ---- Evaluation ----

// Eval traverses msg along all compiled paths simultaneously.
// Paths sharing a prefix are traversed once through the shared segment.
//
// Returns a slice of []Values indexed by entry position (matching the order
// paths were provided to [NewPlan]). Each []Values may contain multiple
// entries when the path fans out via wildcards, ranges, or slices.
//
// Per-path [StrictPath] options are checked at leaf nodes: if any branch
// was clamped during traversal and the path is strict, an error is returned.
func (p *Plan) Eval(m proto.Message) ([][]Values, error) {
	// Validate root.
	got := m.ProtoReflect().Descriptor().FullName()
	want := p.md.FullName()
	if got != want {
		return nil, fmt.Errorf("pbpath.Plan.Eval: got message type %s, want %s", got, want)
	}

	out := make([][]Values, len(p.entries))
	seed := []Values{{
		Path:   Path{p.root.step},
		Values: []protoreflect.Value{protoreflect.ValueOf(m.ProtoReflect())},
	}}

	if err := p.root.exec(seed, p, out); err != nil {
		return nil, err
	}
	// Return only user-visible entries; hidden Expr leaf paths are internal.
	return out[:p.userCount], nil
}

// ---- Leaf-only evaluation (optimised hot path) ----

// EvalLeaves traverses msg along all compiled paths simultaneously, returning
// only the leaf (last) value for each branch — not the full path/values chain.
//
// This is significantly cheaper than [Plan.Eval] because it avoids
// clonePath/cloneValues allocations entirely, tracking only a single cursor
// value per branch per trie step.
//
// The returned slice is indexed by entry position (matching [NewPlan] order).
// Each inner slice contains one [Value] per fan-out branch.
// An empty inner slice means the path produced no values for this message
// (left-join null).
//
// EvalLeaves reuses internal scratch buffers and is NOT safe for concurrent
// use. Use [Plan.EvalLeavesConcurrent] when calling from multiple goroutines.
func (p *Plan) EvalLeaves(m proto.Message) ([][]Value, error) {
	got := m.ProtoReflect().Descriptor().FullName()
	want := p.md.FullName()
	if got != want {
		return nil, fmt.Errorf("pbpath.Plan.EvalLeaves: got message type %s, want %s", got, want)
	}

	// Lazily initialise scratch.
	if p.scratch == nil {
		p.scratch = &leafScratch{
			out: make([][]Value, len(p.entries)),
		}
	}
	s := p.scratch

	// Reset output slots (all entries, including hidden).
	for i := range s.out {
		s.out[i] = s.out[i][:0]
	}

	// Seed with root message.
	s.seed[0] = leafBranch{cursor: MessageVal(m.ProtoReflect())}
	if err := p.root.execLeaves(s.seed[:], p, s.out); err != nil {
		return nil, err
	}

	// Post-process Expr entries: evaluate the expression tree using the
	// resolved leaf values from the trie traversal.
	p.evalExprs(s.out)

	// Return only user-visible entries.
	return s.out[:p.userCount], nil
}

// Clone returns a shallow copy of p with its own scratch buffer.
// The trie, entries, and schema are shared (all immutable after construction);
// only the mutable [scratch] field is reset so that the clone's
// [Plan.EvalLeaves] calls do not race with the original or other clones.
//
// Use Clone when creating independent workers (e.g. [Transcoder.Clone]) that
// each need to call [Plan.EvalLeaves] without synchronisation.
func (p *Plan) Clone() *Plan {
	c := *p        // shallow copy — trie and entries are immutable, safe to share
	c.scratch = nil // fresh scratch; lazily initialised on first EvalLeaves call
	return &c
}

// EvalLeavesConcurrent is like [Plan.EvalLeaves] but allocates fresh buffers
// per call, making it safe for concurrent use by multiple goroutines.
func (p *Plan) EvalLeavesConcurrent(m proto.Message) ([][]Value, error) {
	got := m.ProtoReflect().Descriptor().FullName()
	want := p.md.FullName()
	if got != want {
		return nil, fmt.Errorf("pbpath.Plan.EvalLeavesConcurrent: got message type %s, want %s", got, want)
	}

	out := make([][]Value, len(p.entries))
	seed := [1]leafBranch{{cursor: MessageVal(m.ProtoReflect())}}
	if err := p.root.execLeaves(seed[:], p, out); err != nil {
		return nil, err
	}

	p.evalExprs(out)

	return out[:p.userCount], nil
}

// resolveEnumDescs walks user Expr trees looking for funcEnumName nodes.
// For each, it resolves the child leaf's parsed Path to find the terminal
// FieldDescriptor, obtains its EnumDescriptor, and stores it on the funcExpr.
// Returns an error if the leaf field is not an enum.
func resolveEnumDescs(entries []planEntry, parsed []Path) error {
	for _, e := range entries {
		if e.expr == nil {
			continue
		}
		if err := walkResolveEnumDesc(e.expr, parsed); err != nil {
			return err
		}
	}
	return nil
}

func walkResolveEnumDesc(expr Expr, parsed []Path) error {
	fe, ok := expr.(*funcExpr)
	if !ok {
		return nil // leaf — nothing to do
	}
	// Recurse into children first.
	for _, kid := range fe.kids {
		if err := walkResolveEnumDesc(kid, parsed); err != nil {
			return err
		}
	}
	if fe.kind != funcEnumName {
		return nil
	}
	// The single child should be a pathExpr (or eventually resolve to one).
	leaves := resolvePathExprs(fe.kids[0])
	if len(leaves) == 0 {
		return fmt.Errorf("pbpath.NewPlan: FuncEnumName: child has no resolvable path")
	}
	leaf := leaves[0]
	pp := parsed[leaf.entryIdx]
	if len(pp) == 0 {
		return fmt.Errorf("pbpath.NewPlan: FuncEnumName: leaf %q has no parsed path", leaf.path)
	}
	// Find the terminal FieldAccessStep.
	var fd protoreflect.FieldDescriptor
	for i := len(pp) - 1; i >= 0; i-- {
		if pp[i].Kind() == FieldAccessStep {
			fd = pp[i].FieldDescriptor()
			break
		}
	}
	if fd == nil {
		return fmt.Errorf("pbpath.NewPlan: FuncEnumName: leaf %q has no terminal FieldAccessStep", leaf.path)
	}
	ed := fd.Enum()
	if ed == nil {
		return fmt.Errorf("pbpath.NewPlan: FuncEnumName: field %q is not an enum (kind: %v)", fd.Name(), fd.Kind())
	}
	fe.enumDesc = ed
	return nil
}

// evalExprs post-processes all user-visible entries that have an Expr,
// replacing the raw leaf values with the expression result.
func (p *Plan) evalExprs(out [][]Value) {
	for i := range p.userCount {
		e := p.entries[i]
		if e.expr == nil {
			continue
		}
		// Determine branch count: the maximum fan-out of any leaf this
		// expression depends on. For single-valued leaves this is 1.
		maxBranches := 1
		for _, leaf := range resolvePathExprs(e.expr) {
			if n := len(out[leaf.entryIdx]); n > maxBranches {
				maxBranches = n
			}
		}
		result := make([]Value, 0, maxBranches)
		for bi := range maxBranches {
			result = append(result, e.expr.eval(out, bi))
		}
		out[i] = result
	}
}

// execLeaves is the leaf-only analogue of exec.
func (n *planNode) execLeaves(branches []leafBranch, plan *Plan, out [][]Value) error {
	// Snapshot leaf values for any paths terminating at this node.
	for _, id := range n.leafIDs {
		entry := plan.entries[id]
		for _, b := range branches {
			if entry.strict && b.clamped {
				return fmt.Errorf("pbpath.Plan.EvalLeaves: path %q: range or index was clamped (strict mode)", entry.name)
			}
		}
		for _, b := range branches {
			out[id] = append(out[id], b.cursor)
		}
	}

	// Recurse into children.
	for _, child := range n.children {
		next, err := applyStepLeaves(child.step, branches)
		if err != nil {
			return err
		}
		if len(next) == 0 {
			child.execLeavesEmpty(plan, out)
			continue
		}
		if err := child.execLeaves(next, plan, out); err != nil {
			return err
		}
	}
	return nil
}

// execLeavesEmpty marks all leaf paths under this node as having empty results.
func (n *planNode) execLeavesEmpty(plan *Plan, out [][]Value) {
	for _, id := range n.leafIDs {
		if out[id] == nil {
			out[id] = []Value{} // explicitly empty, not nil
		}
	}
	for _, child := range n.children {
		child.execLeavesEmpty(plan, out)
	}
}

// applyStepLeaves applies a single step to every leaf branch, returning new
// branches. This is the leaf-only analogue of applyStep — it tracks only the
// cursor value, not the full path/values chain.
//
// The cursor is a [Value]; for field access, list operations, and map
// operations it unwraps to the appropriate protoreflect type.
func applyStepLeaves(step Step, branches []leafBranch) ([]leafBranch, error) {
	// Pre-allocate with a small constant capacity that fits on the stack
	// (Go 1.26+). Most steps produce 1 branch per input branch.
	var next []leafBranch

	switch step.Kind() {
	case FieldAccessStep:
		fd := step.FieldDescriptor()
		for _, b := range branches {
			msg := b.cursor.Message()
			if msg == nil {
				msg = b.cursor.ProtoValue().Message()
			}
			val := msg.Get(fd)
			next = append(next, leafBranch{cursor: FromProtoValue(val), clamped: b.clamped})
		}

	case ListIndexStep:
		idx := step.ListIndex()
		for _, b := range branches {
			list := b.cursor.ProtoValue().List()
			resolved := idx
			if resolved < 0 {
				resolved = list.Len() + resolved
			}
			if resolved < 0 || resolved >= list.Len() {
				continue
			}
			next = append(next, leafBranch{cursor: FromProtoValue(list.Get(resolved)), clamped: b.clamped})
		}

	case MapIndexStep:
		for _, b := range branches {
			val := b.cursor.ProtoValue().Map().Get(step.MapIndex())
			if !val.IsValid() {
				continue
			}
			next = append(next, leafBranch{cursor: FromProtoValue(val), clamped: b.clamped})
		}

	case AnyExpandStep:
		for _, b := range branches {
			next = append(next, leafBranch{cursor: b.cursor, clamped: b.clamped})
		}

	case ListWildcardStep:
		for _, b := range branches {
			list := b.cursor.ProtoValue().List()
			for j := 0; j < list.Len(); j++ {
				next = append(next, leafBranch{cursor: FromProtoValue(list.Get(j)), clamped: b.clamped})
			}
		}

	case ListRangeStep:
		stride := step.RangeStep()
		if stride == 0 {
			return nil, fmt.Errorf("slice step must not be zero")
		}
		for _, b := range branches {
			list := b.cursor.ProtoValue().List()
			length := list.Len()
			wasClamped := b.clamped

			start, end := resolveSliceBounds(step, length, stride)

			if !step.StartOmitted() {
				rawStart := step.RangeStart()
				if rawStart < 0 {
					rawStart = length + rawStart
				}
				if rawStart < 0 || rawStart > length {
					wasClamped = true
				}
			}
			if !step.EndOmitted() {
				rawEnd := step.RangeEnd()
				if rawEnd < 0 {
					rawEnd = length + rawEnd
				}
				if stride > 0 && rawEnd > length {
					wasClamped = true
				}
			}

			if stride > 0 {
				for j := start; j < end; j += stride {
					next = append(next, leafBranch{cursor: FromProtoValue(list.Get(j)), clamped: wasClamped})
				}
			} else {
				for j := start; j > end && j >= 0; j += stride {
					next = append(next, leafBranch{cursor: FromProtoValue(list.Get(j)), clamped: wasClamped})
				}
			}
		}

	case FilterStep:
		pred := step.Predicate()
		if pred == nil {
			return nil, fmt.Errorf("FilterStep has nil predicate")
		}
		for _, b := range branches {
			// Evaluate the predicate against the cursor.
			// filterPathExpr nodes expect the cursor in leafValues[0][0].
			filterLeaves := [][]Value{{b.cursor}}
			result := pred.eval(filterLeaves, 0)
			if isNonZeroValue(result) {
				next = append(next, b) // keep this branch
			}
		}

	case MapWildcardStep:
		for _, b := range branches {
			m := b.cursor.ProtoValue().Map()
			m.Range(func(_ protoreflect.MapKey, v protoreflect.Value) bool {
				next = append(next, leafBranch{cursor: FromProtoValue(v), clamped: b.clamped})
				return true
			})
		}

	default:
		return nil, fmt.Errorf("unsupported step kind %d", step.Kind())
	}
	return next, nil
}

// exec recursively walks the trie, applying steps and collecting results.
func (n *planNode) exec(branches []Values, plan *Plan, out [][]Values) error {
	// Snapshot results for any paths that terminate at this node.
	for _, id := range n.leafIDs {
		entry := plan.entries[id]
		for _, b := range branches {
			if entry.strict && b.clamped {
				return fmt.Errorf("pbpath.Plan.Eval: path %q: range or index was clamped (strict mode)", entry.name)
			}
		}
		// Clone branches into the output slot.
		cloned := make([]Values, len(branches))
		copy(cloned, branches)
		out[id] = cloned
	}

	// Recurse into children.
	for _, child := range n.children {
		next, err := applyStep(child.step, branches)
		if err != nil {
			return err
		}
		if len(next) == 0 {
			// All branches pruned. Still snapshot empty results for leaves.
			if err := child.execEmpty(plan, out); err != nil {
				return err
			}
			continue
		}
		if err := child.exec(next, plan, out); err != nil {
			return err
		}
	}
	return nil
}

// execEmpty recursively marks all leaf paths under this node as having
// empty results (no branches survived). This ensures every leaf gets an
// entry in the output even when fan-out produces zero elements.
func (n *planNode) execEmpty(plan *Plan, out [][]Values) error {
	for _, id := range n.leafIDs {
		if out[id] == nil {
			out[id] = []Values{} // explicitly empty, not nil
		}
	}
	for _, child := range n.children {
		if err := child.execEmpty(plan, out); err != nil {
			return err
		}
	}
	return nil
}

// ---- Shared traversal step application ----

// applyStep applies a single step to every branch, returning new branches.
// This is the lenient (non-strict) traversal used by [Plan.Eval]; clamping
// sets the clamped flag on affected branches rather than erroring.
func applyStep(step Step, branches []Values) ([]Values, error) {
	var next []Values
	switch step.Kind() {
	case FieldAccessStep:
		fd := step.FieldDescriptor()
		for _, v := range branches {
			cursor := v.Values[len(v.Values)-1]
			val := cursor.Message().Get(fd)
			next = append(next, Values{
				Path:    append(clonePath(v.Path), step),
				Values:  append(cloneValues(v.Values), val),
				clamped: v.clamped,
			})
		}

	case ListIndexStep:
		idx := step.ListIndex()
		for _, v := range branches {
			cursor := v.Values[len(v.Values)-1]
			list := cursor.List()
			resolved := idx
			if resolved < 0 {
				resolved = list.Len() + resolved
			}
			if resolved < 0 || resolved >= list.Len() {
				// Lenient: skip this branch, mark as clamped.
				continue
			}
			concreteStep := ListIndex(resolved)
			val := list.Get(resolved)
			next = append(next, Values{
				Path:    append(clonePath(v.Path), concreteStep),
				Values:  append(cloneValues(v.Values), val),
				clamped: v.clamped,
			})
		}

	case MapIndexStep:
		for _, v := range branches {
			cursor := v.Values[len(v.Values)-1]
			val := cursor.Map().Get(step.MapIndex())
			if !val.IsValid() {
				continue // skip missing keys leniently
			}
			next = append(next, Values{
				Path:    append(clonePath(v.Path), step),
				Values:  append(cloneValues(v.Values), val),
				clamped: v.clamped,
			})
		}

	case AnyExpandStep:
		for _, v := range branches {
			cursor := v.Values[len(v.Values)-1]
			next = append(next, Values{
				Path:    append(clonePath(v.Path), step),
				Values:  append(cloneValues(v.Values), cursor),
				clamped: v.clamped,
			})
		}

	case ListWildcardStep:
		for _, v := range branches {
			cursor := v.Values[len(v.Values)-1]
			list := cursor.List()
			for j := 0; j < list.Len(); j++ {
				concreteStep := ListIndex(j)
				val := list.Get(j)
				next = append(next, Values{
					Path:    append(clonePath(v.Path), concreteStep),
					Values:  append(cloneValues(v.Values), val),
					clamped: v.clamped,
				})
			}
		}

	case ListRangeStep:
		stride := step.RangeStep()
		if stride == 0 {
			return nil, fmt.Errorf("slice step must not be zero")
		}
		for _, v := range branches {
			cursor := v.Values[len(v.Values)-1]
			list := cursor.List()
			length := list.Len()
			wasClamped := v.clamped

			start, end := resolveSliceBounds(step, length, stride)

			// Detect clamping.
			if !step.StartOmitted() {
				rawStart := step.RangeStart()
				if rawStart < 0 {
					rawStart = length + rawStart
				}
				if rawStart < 0 || rawStart > length {
					wasClamped = true
				}
			}
			if !step.EndOmitted() {
				rawEnd := step.RangeEnd()
				if rawEnd < 0 {
					rawEnd = length + rawEnd
				}
				if stride > 0 && rawEnd > length {
					wasClamped = true
				}
			}

			if stride > 0 {
				for j := start; j < end; j += stride {
					concreteStep := ListIndex(j)
					val := list.Get(j)
					next = append(next, Values{
						Path:    append(clonePath(v.Path), concreteStep),
						Values:  append(cloneValues(v.Values), val),
						clamped: wasClamped,
					})
				}
			} else {
				for j := start; j > end && j >= 0; j += stride {
					concreteStep := ListIndex(j)
					val := list.Get(j)
					next = append(next, Values{
						Path:    append(clonePath(v.Path), concreteStep),
						Values:  append(cloneValues(v.Values), val),
						clamped: wasClamped,
					})
				}
			}
		}

	case FilterStep:
		pred := step.Predicate()
		if pred == nil {
			return nil, fmt.Errorf("FilterStep has nil predicate")
		}
		for _, v := range branches {
			cursor := v.Values[len(v.Values)-1]
			// Evaluate the predicate against the cursor.
			filterLeaves := [][]Value{{FromProtoValue(cursor)}}
			result := pred.eval(filterLeaves, 0)
			if isNonZeroValue(result) {
				next = append(next, Values{
					Path:    clonePath(v.Path),
					Values:  cloneValues(v.Values),
					clamped: v.clamped,
				})
			}
		}

	case MapWildcardStep:
		for _, v := range branches {
			cursor := v.Values[len(v.Values)-1]
			m := cursor.Map()
			m.Range(func(k protoreflect.MapKey, val protoreflect.Value) bool {
				next = append(next, Values{
					Path:    append(clonePath(v.Path), MapIndex(k)),
					Values:  append(cloneValues(v.Values), val),
					clamped: v.clamped,
				})
				return true
			})
		}

	default:
		return nil, fmt.Errorf("unsupported step kind %d", step.Kind())
	}
	return next, nil
}

// resolveSliceBounds computes the effective start and end for a ListRangeStep,
// applying Python-style defaults and clamping.
func resolveSliceBounds(step Step, length, stride int) (start, end int) {
	if step.StartOmitted() {
		if stride > 0 {
			start = 0
		} else {
			start = length - 1
		}
	} else {
		start = step.RangeStart()
		if start < 0 {
			start = length + start
		}
	}

	if step.EndOmitted() {
		if stride > 0 {
			end = length
		} else {
			end = -(length + 1)
		}
	} else {
		end = step.RangeEnd()
		if end < 0 {
			end = length + end
		}
	}

	// Clamp.
	if stride > 0 {
		if start < 0 {
			start = 0
		}
		if start > length {
			start = length
		}
		if end > length {
			end = length
		}
	} else {
		if start >= length {
			start = length - 1
		}
		if start < -1 {
			start = -1
		}
		if end < -(length + 1) {
			end = -(length + 1)
		}
	}
	return
}

// clonePath returns a copy of p with capacity for one additional step,
// so that a subsequent append does not trigger a reallocation.
func clonePath(p Path) Path {
	out := make(Path, len(p), len(p)+1)
	copy(out, p)
	return out
}

// cloneValues returns a copy of v with capacity for one additional value,
// so that a subsequent append does not trigger a reallocation.
func cloneValues(v []protoreflect.Value) []protoreflect.Value {
	out := make([]protoreflect.Value, len(v), len(v)+1)
	copy(out, v)
	return out
}

// ---- Convenience ----

// PathValuesMulti is a convenience wrapper that compiles a [Plan] from the
// given path specs and immediately evaluates it against msg.
// For repeated evaluation of the same paths against many messages, prefer
// [NewPlan] + [Plan.Eval].
func PathValuesMulti(md protoreflect.MessageDescriptor, m proto.Message, paths ...PlanPathSpec) ([][]Values, error) {
	plan, err := NewPlan(md, nil, paths...)
	if err != nil {
		return nil, err
	}
	return plan.Eval(m)
}
