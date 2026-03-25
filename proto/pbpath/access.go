package pbpath

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// PathOption configures the behaviour of [PathValues].
type PathOption func(*pathOpts)

type pathOpts struct {
	strict bool
}

// Strict returns a [PathOption] that makes [PathValues] return an error when a
// negative index or range bound resolves to an out-of-bounds position.
// Without Strict, out-of-bounds accesses are silently clamped or skipped.
func Strict() PathOption { return func(o *pathOpts) { o.strict = true } }

// PathValues returns the values along a path in message m.
//
// When the path contains a [ListWildcardStep] or [ListRangeStep] the function
// fans out: one [Values] is produced for every matching list element (cartesian
// product when multiple fan-out steps are nested).
//
// A single [ListIndexStep] with a negative index is resolved relative to the
// list length (e.g. -1 → last element).
func PathValues(p Path, m proto.Message, opts ...PathOption) ([]Values, error) {
	var o pathOpts
	for _, fn := range opts {
		fn(&o)
	}

	seed := Values{
		Path:   Path{p[0]},
		Values: []protoreflect.Value{protoreflect.ValueOf(m.ProtoReflect())},
	}

	// Validate root.
	root := p[0]
	if root.Kind() != RootStep {
		return nil, fmt.Errorf("first step must be RootStep, got %d", root.Kind())
	}
	got := root.MessageDescriptor().FullName()
	want := m.ProtoReflect().Descriptor().FullName()
	if got != want {
		return nil, fmt.Errorf("got root type %s, want %s", got, want)
	}

	results := []Values{seed}
	desc := protoreflect.Descriptor(m.ProtoReflect().Descriptor())

	for i := 1; i < len(p); i++ {
		step := p[i]
		var next []Values

		switch step.Kind() {
		case FieldAccessStep:
			if f, ok := desc.(protoreflect.FieldDescriptor); ok {
				desc = f.Message()
			}
			md, ok := desc.(protoreflect.MessageDescriptor)
			if !ok {
				return nil, fmt.Errorf("%d: cursor has descriptor %T, want MessageDescriptor", i, desc)
			}
			fd := step.FieldDescriptor()
			desc = md.Fields().ByNumber(fd.Number())
			if desc == nil {
				return nil, fmt.Errorf("%d: cursor message missing field %v", i, fd.Number())
			}
			for _, v := range results {
				cursor := v.Values[len(v.Values)-1]
				val := cursor.Message().Get(fd)
				next = append(next, Values{
					Path:   append(append(Path{}, v.Path...), step),
					Values: append(append([]protoreflect.Value{}, v.Values...), val),
				})
			}
			results = next

		case ListIndexStep:
			fd, ok := desc.(protoreflect.FieldDescriptor)
			if !ok {
				return nil, fmt.Errorf("%d: cursor has descriptor %T, want FieldDescriptor", i, desc)
			}
			if !fd.IsList() {
				return nil, fmt.Errorf("%d: cursor descriptor is not a list", i)
			}
			desc = fd.Message()
			idx := step.ListIndex()
			for _, v := range results {
				cursor := v.Values[len(v.Values)-1]
				list := cursor.List()
				resolved := idx
				if resolved < 0 {
					resolved = list.Len() + resolved
				}
				if resolved < 0 || resolved >= list.Len() {
					if o.strict {
						return nil, fmt.Errorf("%d: list index %d out of range (len %d)", i, idx, list.Len())
					}
					continue // skip this branch
				}
				// Emit a concrete ListIndex step with the resolved index.
				concreteStep := ListIndex(resolved)
				val := list.Get(resolved)
				next = append(next, Values{
					Path:   append(append(Path{}, v.Path...), concreteStep),
					Values: append(append([]protoreflect.Value{}, v.Values...), val),
				})
			}
			results = next

		case MapIndexStep:
			fd, ok := desc.(protoreflect.FieldDescriptor)
			if !ok {
				return nil, fmt.Errorf("%d: cursor has descriptor %T, want FieldDescriptor", i, desc)
			}
			if !fd.IsMap() {
				return nil, fmt.Errorf("%d: cursor descriptor is not a map", i)
			}
			for _, v := range results {
				cursor := v.Values[len(v.Values)-1]
				val := cursor.Map().Get(step.MapIndex())
				if !val.IsValid() {
					return nil, fmt.Errorf("%d: cursor map missing key %v", i, step.MapIndex())
				}
				next = append(next, Values{
					Path:   append(append(Path{}, v.Path...), step),
					Values: append(append([]protoreflect.Value{}, v.Values...), val),
				})
			}
			desc = fd.MapValue()
			results = next

		case AnyExpandStep:
			if desc.FullName() != step.MessageDescriptor().FullName() {
				return nil, fmt.Errorf("%d: cursor any expansion at %T is not %v", i, desc, step.MessageDescriptor().FullName())
			}
			desc = step.MessageDescriptor()
			for _, v := range results {
				cursor := v.Values[len(v.Values)-1]
				next = append(next, Values{
					Path:   append(append(Path{}, v.Path...), step),
					Values: append(append([]protoreflect.Value{}, v.Values...), cursor),
				})
			}
			results = next

		case ListWildcardStep:
			fd, ok := desc.(protoreflect.FieldDescriptor)
			if !ok {
				return nil, fmt.Errorf("%d: cursor has descriptor %T, want FieldDescriptor", i, desc)
			}
			if !fd.IsList() {
				return nil, fmt.Errorf("%d: cursor descriptor is not a list", i)
			}
			desc = fd.Message()
			for _, v := range results {
				cursor := v.Values[len(v.Values)-1]
				list := cursor.List()
				for j := 0; j < list.Len(); j++ {
					concreteStep := ListIndex(j)
					val := list.Get(j)
					next = append(next, Values{
						Path:   append(append(Path{}, v.Path...), concreteStep),
						Values: append(append([]protoreflect.Value{}, v.Values...), val),
					})
				}
			}
			results = next

		case ListRangeStep:
			fd, ok := desc.(protoreflect.FieldDescriptor)
			if !ok {
				return nil, fmt.Errorf("%d: cursor has descriptor %T, want FieldDescriptor", i, desc)
			}
			if !fd.IsList() {
				return nil, fmt.Errorf("%d: cursor descriptor is not a list", i)
			}
			desc = fd.Message()
			stride := step.RangeStep()
			if stride == 0 {
				return nil, fmt.Errorf("%d: slice step must not be zero", i)
			}
			for _, v := range results {
				cursor := v.Values[len(v.Values)-1]
				list := cursor.List()
				length := list.Len()

				// Resolve start — default depends on step sign.
				var start int
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

				// Resolve end — default depends on step sign.
				var end int
				if step.EndOmitted() {
					if stride > 0 {
						end = length
					} else {
						end = -(length + 1) // sentinel: one before index 0
					}
				} else {
					end = step.RangeEnd()
					if end < 0 {
						end = length + end
					}
				}

				// Clamp bounds to valid range based on step direction.
				if stride > 0 {
					if start < 0 {
						if o.strict {
							return nil, fmt.Errorf("%d: range start %d out of range (len %d)", i, step.RangeStart(), length)
						}
						start = 0
					}
					if start > length {
						if o.strict {
							return nil, fmt.Errorf("%d: range start %d out of range (len %d)", i, step.RangeStart(), length)
						}
						start = length
					}
					if end > length {
						if o.strict {
							return nil, fmt.Errorf("%d: range end %d out of range (len %d)", i, step.RangeEnd(), length)
						}
						end = length
					}
					for j := start; j < end; j += stride {
						concreteStep := ListIndex(j)
						val := list.Get(j)
						next = append(next, Values{
							Path:   append(append(Path{}, v.Path...), concreteStep),
							Values: append(append([]protoreflect.Value{}, v.Values...), val),
						})
					}
				} else {
					// Negative stride: iterate from high to low.
					if start >= length {
						if o.strict {
							return nil, fmt.Errorf("%d: range start %d out of range (len %d)", i, step.RangeStart(), length)
						}
						start = length - 1
					}
					if start < -1 {
						start = -1 // will produce no iterations
					}
					// end is exclusive lower bound; clamp to -(length+1).
					if end < -(length + 1) {
						end = -(length + 1)
					}
					for j := start; j > end && j >= 0; j += stride {
						concreteStep := ListIndex(j)
						val := list.Get(j)
						next = append(next, Values{
							Path:   append(append(Path{}, v.Path...), concreteStep),
							Values: append(append([]protoreflect.Value{}, v.Values...), val),
						})
					}
				}
			}
			results = next

		case RootStep:
			return nil, fmt.Errorf("root step at index %d. Must be at index 0", i)
		case UnknownAccessStep:
			return nil, fmt.Errorf("%d: unknown access step", i)
		default:
			return nil, fmt.Errorf("%d: unsupported step kind %d", i, step.Kind())
		}

		if len(results) == 0 && !o.strict {
			// All branches were pruned (e.g. empty list, out-of-range).
			return nil, nil
		}
	}
	return results, nil
}
