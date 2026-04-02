package pbpath

import (
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// ── Object construction: {key: pipeline, ...} ──────────────────────────

// pipeObjEntry describes one entry in an object constructor. If keyExpr is
// non-nil the key is dynamic (computed from the expression); otherwise the
// literal staticKey is used.
type pipeObjEntry struct {
	staticKey string    // used when keyExpr == nil
	keyExpr   *Pipeline // dynamic key: (expr)
	value     *Pipeline // value pipeline (may be nil for shorthand {name})
}

// pipeObjectConstruct builds an ObjectKind value from its entry specs.
type pipeObjectConstruct struct {
	entries []pipeObjEntry
}

func (p *pipeObjectConstruct) exec(ctx *PipeContext, input Value) ([]Value, error) {
	var out []ObjectEntry
	for _, e := range p.entries {
		// Resolve key(s).
		var keys []string
		if e.keyExpr != nil {
			kVals, err := e.keyExpr.execWith(ctx, []Value{input})
			if err != nil {
				return nil, fmt.Errorf("object key: %w", err)
			}
			for _, kv := range kVals {
				keys = append(keys, valueToStringValue(kv))
			}
		} else {
			keys = []string{e.staticKey}
		}

		// Resolve value(s).
		for _, k := range keys {
			if e.value != nil {
				vVals, err := e.value.execWith(ctx, []Value{input})
				if err != nil {
					return nil, fmt.Errorf("object value for key %q: %w", k, err)
				}
				for _, vv := range vVals {
					out = append(out, ObjectEntry{Key: k, Value: vv})
				}
			} else {
				// Shorthand: {name} means {name: .name}
				accessed, err := accessField(ctx, input, k)
				if err != nil {
					return nil, err
				}
				for _, av := range accessed {
					out = append(out, ObjectEntry{Key: k, Value: av})
				}
			}
		}
	}
	return []Value{ObjectVal(out)}, nil
}

// accessField performs a field lookup on the input for the shorthand syntax.
func accessField(_ *PipeContext, input Value, name string) ([]Value, error) {
	switch input.Kind() {
	case MessageKind:
		if input.Message() == nil {
			return []Value{Null()}, nil
		}
		fd := input.Message().Descriptor().Fields().ByTextName(name)
		if fd == nil {
			return nil, fmt.Errorf("field %q not found in %s", name, input.Message().Descriptor().FullName())
		}
		val := input.Message().Get(fd)
		if fd.IsList() {
			list := val.List()
			elems := make([]Value, list.Len())
			for j := 0; j < list.Len(); j++ {
				elems[j] = FromProtoValue(list.Get(j))
			}
			return []Value{ListVal(elems)}, nil
		}
		if fd.IsMap() {
			var items []Value
			val.Map().Range(func(_ protoreflect.MapKey, v protoreflect.Value) bool {
				items = append(items, FromProtoValue(v))
				return true
			})
			return []Value{ListVal(items)}, nil
		}
		return []Value{FromProtoValue(val)}, nil
	case ObjectKind:
		for _, e := range input.Entries() {
			if e.Key == name {
				return []Value{e.Value}, nil
			}
		}
		return []Value{Null()}, nil
	default:
		return nil, fmt.Errorf("cannot access field %q on %s", name, typeNameOf(input))
	}
}

// ── Object-aware extensions to existing builtins ────────────────────────

// builtinObjKeys returns the keys of an ObjectKind value as a list of strings.
func builtinObjKeys(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() != ObjectKind {
		return nil, fmt.Errorf("keys: cannot get keys of %s", typeNameOf(input))
	}
	seen := map[string]bool{}
	var out []Value
	for _, e := range input.Entries() {
		if !seen[e.Key] {
			seen[e.Key] = true
			out = append(out, ScalarString(e.Key))
		}
	}
	return []Value{ListVal(out)}, nil
}

// builtinObjValues returns the values of an ObjectKind value as a list.
func builtinObjValues(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() != ObjectKind {
		return nil, fmt.Errorf("values: cannot get values of %s", typeNameOf(input))
	}
	out := make([]Value, len(input.Entries()))
	for i, e := range input.Entries() {
		out[i] = e.Value
	}
	return []Value{ListVal(out)}, nil
}

// builtinObjHas checks whether an object has a given key.
func builtinObjHas(_ *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	if input.Kind() != ObjectKind {
		return nil, fmt.Errorf("has: expected object, got %s", typeNameOf(input))
	}
	argVals, err := arg.execWith(nil, []Value{input})
	if err != nil {
		return nil, err
	}
	key := ""
	if len(argVals) > 0 {
		key = valueToStringValue(argVals[0])
	}
	for _, e := range input.Entries() {
		if e.Key == key {
			return []Value{ScalarBool(true)}, nil
		}
	}
	return []Value{ScalarBool(false)}, nil
}

// builtinToEntries converts {k1:v1, k2:v2, ...} → [{key:k1, value:v1}, ...].
// Since we don't have real objects for the inner entries, we represent each
// entry as an ObjectKind value with "key" and "value" entries.
func builtinToEntries(_ *PipeContext, input Value) ([]Value, error) {
	switch input.Kind() {
	case ObjectKind:
		var out []Value
		for _, e := range input.Entries() {
			entry := ObjectVal([]ObjectEntry{
				{Key: "key", Value: ScalarString(e.Key)},
				{Key: "value", Value: e.Value},
			})
			out = append(out, entry)
		}
		return []Value{ListVal(out)}, nil
	case MessageKind:
		if input.Message() == nil {
			return []Value{ListVal(nil)}, nil
		}
		var out []Value
		input.Message().Range(func(fd protoreflect.FieldDescriptor, val protoreflect.Value) bool {
			entry := ObjectVal([]ObjectEntry{
				{Key: "key", Value: ScalarString(string(fd.Name()))},
				{Key: "value", Value: FromProtoValue(val)},
			})
			out = append(out, entry)
			return true
		})
		return []Value{ListVal(out)}, nil
	default:
		return nil, fmt.Errorf("to_entries: expected object, got %s", typeNameOf(input))
	}
}

// builtinFromEntries converts [{key:k, value:v}, ...] → {k:v, ...}.
// Each list element must be an ObjectKind with "key" and "value" entries,
// or a list of [key, value].
func builtinFromEntries(_ *PipeContext, input Value) ([]Value, error) {
	if input.Kind() != ListKind {
		return nil, fmt.Errorf("from_entries: expected array, got %s", typeNameOf(input))
	}
	var entries []ObjectEntry
	for _, item := range input.List() {
		switch item.Kind() {
		case ObjectKind:
			k := objLookup(item, "key")
			if k.IsNull() {
				k = objLookup(item, "name") // jq also accepts "name"
			}
			v := objLookup(item, "value")
			entries = append(entries, ObjectEntry{
				Key:   valueToStringValue(k),
				Value: v,
			})
		case ListKind:
			elems := item.List()
			if len(elems) >= 2 {
				entries = append(entries, ObjectEntry{
					Key:   valueToStringValue(elems[0]),
					Value: elems[1],
				})
			}
		default:
			return nil, fmt.Errorf("from_entries: array element must be object or array, got %s", typeNameOf(item))
		}
	}
	return []Value{ObjectVal(entries)}, nil
}

// builtinWithEntries implements with_entries(f) = to_entries | map(f) | from_entries.
func builtinWithEntries(ctx *PipeContext, input Value, f *Pipeline) ([]Value, error) {
	// Step 1: to_entries.
	teVals, err := builtinToEntries(ctx, input)
	if err != nil {
		return nil, err
	}
	if len(teVals) == 0 || teVals[0].Kind() != ListKind {
		return []Value{ObjectVal(nil)}, nil
	}
	list := teVals[0].List()

	// Step 2: map(f).
	var mapped []Value
	for _, item := range list {
		out, err := f.execWith(ctx, []Value{item})
		if err != nil {
			return nil, fmt.Errorf("with_entries: %w", err)
		}
		mapped = append(mapped, out...)
	}
	mappedList := ListVal(mapped)

	// Step 3: from_entries.
	return builtinFromEntries(ctx, mappedList)
}

// builtinGetpath retrieves a value at a path (list of keys/indices).
func builtinGetpath(ctx *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	argVals, err := arg.execWith(ctx, []Value{input})
	if err != nil {
		return nil, err
	}
	if len(argVals) == 0 || argVals[0].Kind() != ListKind {
		return nil, fmt.Errorf("getpath: expected array argument")
	}
	cur := input
	for _, seg := range argVals[0].List() {
		cur = objOrListGet(cur, seg)
	}
	return []Value{cur}, nil
}

// builtinSetpath sets a value at a path.
func builtinSetpath(ctx *PipeContext, input Value, pathArg *Pipeline, valArg *Pipeline) ([]Value, error) {
	pathVals, err := pathArg.execWith(ctx, []Value{input})
	if err != nil {
		return nil, err
	}
	if len(pathVals) == 0 || pathVals[0].Kind() != ListKind {
		return nil, fmt.Errorf("setpath: expected array path argument")
	}
	newVals, err := valArg.execWith(ctx, []Value{input})
	if err != nil {
		return nil, err
	}
	newVal := Null()
	if len(newVals) > 0 {
		newVal = newVals[0]
	}
	return []Value{objSetpath(input, pathVals[0].List(), newVal)}, nil
}

// builtinDelpaths deletes values at multiple paths.
func builtinDelpaths(ctx *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	argVals, err := arg.execWith(ctx, []Value{input})
	if err != nil {
		return nil, err
	}
	if len(argVals) == 0 || argVals[0].Kind() != ListKind {
		return nil, fmt.Errorf("delpaths: expected array of paths")
	}
	result := input
	// Process paths in reverse to avoid index shifting issues.
	paths := argVals[0].List()
	for i := len(paths) - 1; i >= 0; i-- {
		p := paths[i]
		if p.Kind() != ListKind {
			continue
		}
		result = objDelpath(result, p.List())
	}
	return []Value{result}, nil
}

// builtinObjIn checks whether a key (the input) is in the given object.
// Usage: .key | in(obj) — jq semantics.
func builtinObjIn(ctx *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
	argVals, err := arg.execWith(ctx, []Value{input})
	if err != nil {
		return nil, err
	}
	if len(argVals) == 0 {
		return []Value{ScalarBool(false)}, nil
	}
	obj := argVals[0]
	key := valueToStringValue(input)
	switch obj.Kind() {
	case ObjectKind:
		for _, e := range obj.Entries() {
			if e.Key == key {
				return []Value{ScalarBool(true)}, nil
			}
		}
		return []Value{ScalarBool(false)}, nil
	case MessageKind:
		if obj.Message() == nil {
			return []Value{ScalarBool(false)}, nil
		}
		fd := obj.Message().Descriptor().Fields().ByTextName(key)
		return []Value{ScalarBool(fd != nil)}, nil
	default:
		return []Value{ScalarBool(false)}, nil
	}
}

// ── Object helpers ──────────────────────────────────────────────────────

// objLookup finds a key in an ObjectKind value. Returns Null() if absent.
func objLookup(v Value, key string) Value {
	if v.Kind() != ObjectKind {
		return Null()
	}
	for _, e := range v.Entries() {
		if e.Key == key {
			return e.Value
		}
	}
	return Null()
}

// objOrListGet navigates one segment into an object or list.
func objOrListGet(v Value, seg Value) Value {
	switch v.Kind() {
	case ObjectKind:
		key := valueToStringValue(seg)
		return objLookup(v, key)
	case ListKind:
		if idx, ok := toInt64Value(seg); ok {
			return v.Index(int(idx))
		}
		return Null()
	case MessageKind:
		if v.Message() == nil {
			return Null()
		}
		key := valueToStringValue(seg)
		fd := v.Message().Descriptor().Fields().ByTextName(key)
		if fd == nil {
			return Null()
		}
		return FromProtoValue(v.Message().Get(fd))
	default:
		return Null()
	}
}

// objSetpath returns a copy of v with the value at path set to newVal.
// Only works on ObjectKind values and lists. Other kinds are returned unchanged.
func objSetpath(v Value, path []Value, newVal Value) Value {
	if len(path) == 0 {
		return newVal
	}
	seg := path[0]
	rest := path[1:]

	switch v.Kind() {
	case ObjectKind:
		key := valueToStringValue(seg)
		entries := make([]ObjectEntry, 0, len(v.Entries())+1)
		found := false
		for _, e := range v.Entries() {
			if e.Key == key {
				entries = append(entries, ObjectEntry{Key: key, Value: objSetpath(e.Value, rest, newVal)})
				found = true
			} else {
				entries = append(entries, e)
			}
		}
		if !found {
			entries = append(entries, ObjectEntry{Key: key, Value: objSetpath(Null(), rest, newVal)})
		}
		return ObjectVal(entries)
	case ListKind:
		if idx, ok := toInt64Value(seg); ok {
			items := v.List()
			n := int(idx)
			if n < 0 {
				n = len(items) + n
			}
			if n < 0 || n >= len(items) {
				return v // out of range — leave unchanged
			}
			cp := make([]Value, len(items))
			copy(cp, items)
			cp[n] = objSetpath(cp[n], rest, newVal)
			return ListVal(cp)
		}
		return v
	case NullKind:
		// Build structure from null, like jq.
		if _, ok := toInt64Value(seg); ok {
			return v // can't index into null
		}
		key := valueToStringValue(seg)
		return ObjectVal([]ObjectEntry{
			{Key: key, Value: objSetpath(Null(), rest, newVal)},
		})
	default:
		return v
	}
}

// objDelpath returns a copy of v with the element at path removed.
func objDelpath(v Value, path []Value) Value {
	if len(path) == 0 {
		return Null()
	}
	seg := path[0]
	rest := path[1:]

	switch v.Kind() {
	case ObjectKind:
		key := valueToStringValue(seg)
		var entries []ObjectEntry
		for _, e := range v.Entries() {
			if e.Key == key {
				if len(rest) > 0 {
					entries = append(entries, ObjectEntry{Key: key, Value: objDelpath(e.Value, rest)})
				}
				// else: skip this entry (delete it)
			} else {
				entries = append(entries, e)
			}
		}
		return ObjectVal(entries)
	case ListKind:
		if idx, ok := toInt64Value(seg); ok {
			items := v.List()
			n := int(idx)
			if n < 0 {
				n = len(items) + n
			}
			if n < 0 || n >= len(items) {
				return v
			}
			if len(rest) > 0 {
				cp := make([]Value, len(items))
				copy(cp, items)
				cp[n] = objDelpath(cp[n], rest)
				return ListVal(cp)
			}
			// Remove element at index.
			cp := make([]Value, 0, len(items)-1)
			cp = append(cp, items[:n]...)
			cp = append(cp, items[n+1:]...)
			return ListVal(cp)
		}
		return v
	default:
		return v
	}
}

// objMerge merges two object-like values. Right-hand entries override left-hand
// entries with the same key. Used for the + operator on objects.
func objMerge(left, right Value) Value {
	if left.Kind() != ObjectKind || right.Kind() != ObjectKind {
		return right // fallback
	}
	// Build result: left entries (overridden if key appears in right) + new right entries.
	rKeys := map[string]Value{}
	for _, e := range right.Entries() {
		rKeys[e.Key] = e.Value
	}
	seen := map[string]bool{}
	var out []ObjectEntry
	for _, e := range left.Entries() {
		if v, ok := rKeys[e.Key]; ok {
			out = append(out, ObjectEntry{Key: e.Key, Value: v})
		} else {
			out = append(out, e)
		}
		seen[e.Key] = true
	}
	for _, e := range right.Entries() {
		if !seen[e.Key] {
			out = append(out, e)
		}
	}
	return ObjectVal(out)
}

// objRecursiveMerge deeply merges two objects. If both sides have the same key
// with object values, they are merged recursively. Used for * on objects.
func objRecursiveMerge(left, right Value) Value {
	if left.Kind() != ObjectKind || right.Kind() != ObjectKind {
		return right
	}
	rMap := map[string]Value{}
	for _, e := range right.Entries() {
		rMap[e.Key] = e.Value
	}
	seen := map[string]bool{}
	var out []ObjectEntry
	for _, e := range left.Entries() {
		if rv, ok := rMap[e.Key]; ok {
			if e.Value.Kind() == ObjectKind && rv.Kind() == ObjectKind {
				out = append(out, ObjectEntry{Key: e.Key, Value: objRecursiveMerge(e.Value, rv)})
			} else {
				out = append(out, ObjectEntry{Key: e.Key, Value: rv})
			}
		} else {
			out = append(out, e)
		}
		seen[e.Key] = true
	}
	for _, e := range right.Entries() {
		if !seen[e.Key] {
			out = append(out, e)
		}
	}
	return ObjectVal(out)
}

// ── Registration ────────────────────────────────────────────────────────

func init() {
	// Zero-arg builtins.
	pipeBuiltins["to_entries"] = builtinToEntries
	pipeBuiltins["from_entries"] = builtinFromEntries

	// 1-arg builtins.
	pipeFuncsWith1Arg["has"] = func(ctx *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
		return builtinObjHas(ctx, input, arg)
	}
	pipeFuncsWith1Arg["in"] = func(ctx *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
		return builtinObjIn(ctx, input, arg)
	}
	pipeFuncsWith1Arg["with_entries"] = func(ctx *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
		return builtinWithEntries(ctx, input, arg)
	}
	pipeFuncsWith1Arg["getpath"] = func(ctx *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
		return builtinGetpath(ctx, input, arg)
	}
	pipeFuncsWith1Arg["delpaths"] = func(ctx *PipeContext, input Value, arg *Pipeline) ([]Value, error) {
		return builtinDelpaths(ctx, input, arg)
	}

	// 2-arg builtins.
	pipeFuncsWith2Args["setpath"] = func(ctx *PipeContext, input Value, arg1 *Pipeline, arg2 *Pipeline) ([]Value, error) {
		return builtinSetpath(ctx, input, arg1, arg2)
	}
}

// ── Object-aware iteration and field access ─────────────────────────────

// ── Integration: patch existing builtins to handle ObjectKind ───────────

// We patch existing builtins inline where they switch on Kind. Since Go
// doesn't support method patching, the approach is to modify the original
// functions in pipe.go. However, to keep the diff minimal, we instead add
// ObjectKind cases via wrapper functions here and update the originals.

// wrapBuiltinKeys wraps the original keys builtin to add ObjectKind support.
func wrapBuiltinKeys(orig func(*PipeContext, Value) ([]Value, error)) func(*PipeContext, Value) ([]Value, error) {
	return func(ctx *PipeContext, input Value) ([]Value, error) {
		if input.Kind() == ObjectKind {
			return builtinObjKeys(ctx, input)
		}
		return orig(ctx, input)
	}
}

// wrapBuiltinValues wraps the original values builtin.
func wrapBuiltinValues(orig func(*PipeContext, Value) ([]Value, error)) func(*PipeContext, Value) ([]Value, error) {
	return func(ctx *PipeContext, input Value) ([]Value, error) {
		if input.Kind() == ObjectKind {
			return builtinObjValues(ctx, input)
		}
		return orig(ctx, input)
	}
}

// wrapBuiltinLength wraps the original length builtin.
func wrapBuiltinLength(orig func(*PipeContext, Value) ([]Value, error)) func(*PipeContext, Value) ([]Value, error) {
	return func(ctx *PipeContext, input Value) ([]Value, error) {
		if input.Kind() == ObjectKind {
			return []Value{ScalarInt64(int64(len(input.Entries())))}, nil
		}
		return orig(ctx, input)
	}
}

// wrapBuiltinAdd wraps the original add builtin.
func wrapBuiltinAdd(orig func(*PipeContext, Value) ([]Value, error)) func(*PipeContext, Value) ([]Value, error) {
	return func(ctx *PipeContext, input Value) ([]Value, error) {
		if input.Kind() == ListKind && len(input.List()) > 0 && input.List()[0].Kind() == ObjectKind {
			result := input.List()[0]
			for _, item := range input.List()[1:] {
				if item.Kind() == ObjectKind {
					result = objMerge(result, item)
				}
			}
			return []Value{result}, nil
		}
		return orig(ctx, input)
	}
}

// wrapBuiltinType wraps the original type builtin.
func wrapBuiltinType(orig func(*PipeContext, Value) ([]Value, error)) func(*PipeContext, Value) ([]Value, error) {
	return func(ctx *PipeContext, input Value) ([]Value, error) {
		if input.Kind() == ObjectKind {
			return []Value{ScalarString("object")}, nil
		}
		return orig(ctx, input)
	}
}

func init() {
	// Wrap existing builtins to handle ObjectKind transparently.
	pipeBuiltins["keys"] = wrapBuiltinKeys(pipeBuiltins["keys"])
	pipeBuiltins["values"] = wrapBuiltinValues(pipeBuiltins["values"])
	pipeBuiltins["length"] = wrapBuiltinLength(pipeBuiltins["length"])
	pipeBuiltins["add"] = wrapBuiltinAdd(pipeBuiltins["add"])
	pipeBuiltins["type"] = wrapBuiltinType(pipeBuiltins["type"])
}
