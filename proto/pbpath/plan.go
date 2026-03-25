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
// A Plan is safe for concurrent use by multiple goroutines.
type Plan struct {
	root    *planNode
	md      protoreflect.MessageDescriptor
	entries []planEntry
}

// PlanEntry exposes metadata about one compiled path inside a [Plan].
type PlanEntry struct {
	// Name is the alias if one was set via [Alias], otherwise the original
	// path string.
	Name string
	// Path is the compiled [Path].
	Path Path
}

// planEntry is the internal per-path state.
type planEntry struct {
	name   string
	path   Path
	strict bool
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
}

// PlanOption configures a single path entry inside a [Plan].
type PlanOption func(*planEntryOpts)

// StrictPath returns a [PlanOption] that makes this path's evaluation return
// an error when a range or index is clamped due to the list being shorter
// than the requested bound. Without StrictPath, out-of-bounds accesses are
// silently clamped or skipped.
func StrictPath() PlanOption { return func(o *planEntryOpts) { o.strict = true } }

// Alias returns a [PlanOption] that gives this path entry a human-readable
// name. The alias is returned by [Plan.Entries] and is useful for mapping
// paths to output column names.
func Alias(name string) PlanOption { return func(o *planEntryOpts) { o.alias = name } }

// PlanPath creates a [PlanPathSpec] pairing a path string with per-path options.
func PlanPath(path string, opts ...PlanOption) PlanPathSpec {
	var o planEntryOpts
	for _, fn := range opts {
		fn(&o)
	}
	return PlanPathSpec{path: path, opts: o}
}

// ---- Construction ----

// NewPlan compiles one or more path strings against md into an immutable [Plan].
// Parsing and trie construction happen once; [Plan.Eval] is the hot path.
//
// All paths must be rooted at the same message descriptor md.
// Returns an error that bundles all parse failures.
func NewPlan(md protoreflect.MessageDescriptor, paths ...PlanPathSpec) (*Plan, error) {
	if md == nil {
		return nil, fmt.Errorf("pbpath.NewPlan: message descriptor must be non-nil")
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("pbpath.NewPlan: at least one path is required")
	}

	p := &Plan{md: md, entries: make([]planEntry, len(paths))}

	// Parse all paths, collecting errors.
	var errs []string
	parsed := make([]Path, len(paths))
	for i, spec := range paths {
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
		p.entries[i] = planEntry{name: name, path: pp, strict: spec.opts.strict}
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("pbpath.NewPlan: %s", strings.Join(errs, "; "))
	}

	// Build the trie. The root planNode represents the RootStep.
	p.root = &planNode{
		step: parsed[0][0], // RootStep — same for all paths
		desc: md,
	}
	for i, pp := range parsed {
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
	default:
		return false
	}
}

// ---- Introspection ----

// Entries returns metadata for each compiled path, in the order they were
// provided to [NewPlan].
func (p *Plan) Entries() []PlanEntry {
	out := make([]PlanEntry, len(p.entries))
	for i, e := range p.entries {
		out[i] = PlanEntry{Name: e.name, Path: e.path}
	}
	return out
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
	return out, nil
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

// clonePath returns a copy of p.
func clonePath(p Path) Path {
	out := make(Path, len(p))
	copy(out, p)
	return out
}

// cloneValues returns a copy of v.
func cloneValues(v []protoreflect.Value) []protoreflect.Value {
	out := make([]protoreflect.Value, len(v))
	copy(out, v)
	return out
}

// ---- Convenience ----

// PathValuesMulti is a convenience wrapper that compiles a [Plan] from the
// given path specs and immediately evaluates it against msg.
// For repeated evaluation of the same paths against many messages, prefer
// [NewPlan] + [Plan.Eval].
func PathValuesMulti(md protoreflect.MessageDescriptor, m proto.Message, paths ...PlanPathSpec) ([][]Values, error) {
	plan, err := NewPlan(md, paths...)
	if err != nil {
		return nil, err
	}
	return plan.Eval(m)
}
