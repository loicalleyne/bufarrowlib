package pbpath

import (
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Expr represents a composable expression in a [Plan].
//
// An Expr tree is built from leaf [PathRef] nodes (referencing protobuf
// field paths) and interior function nodes (created via Func* constructors).
// The tree is validated and resolved at [NewPlan] time; evaluation happens
// inside [Plan.EvalLeaves].
//
// Expr is intentionally an opaque interface — construct instances via
// [PathRef], [FuncCoalesce], [FuncAdd], and friends.
type Expr interface {
	// inputPaths returns all leaf path strings that this expression
	// (and its children) depend on, in depth-first order.
	inputPaths() []string

	// outputKind returns the protoreflect.Kind of the expression's result.
	// A zero value means "same kind as the input leaf" (pass-through).
	// Non-zero means the function changes the output type (e.g. Len→Int64Kind,
	// Has→BoolKind), which triggers an Arrow type override in the denormalizer.
	outputKind() protoreflect.Kind

	// eval evaluates the expression given the resolved leaf values from
	// Plan.EvalLeaves. leafValues is indexed by the resolved entry index
	// assigned during plan compilation.
	//
	// branchIdx selects which fan-out branch to read from multi-valued leaves.
	eval(leafValues [][]Value, branchIdx int) Value

	// children returns the direct child expressions, or nil for leaves.
	children() []Expr
}

// ---- Leaf: path reference ----

// pathExpr is a leaf Expr that references a protobuf field path.
// The entryIdx is resolved during NewPlan compilation.
type pathExpr struct {
	path     string // raw path string (e.g. "user.id")
	entryIdx int    // resolved index into Plan.entries / EvalLeaves output; -1 until resolved
}

// PathRef creates a leaf [Expr] referencing a protobuf field path.
// The path is parsed and validated when the enclosing [PlanPathSpec] is
// compiled by [NewPlan].
func PathRef(path string) Expr {
	return &pathExpr{path: path, entryIdx: -1}
}

func (e *pathExpr) inputPaths() []string { return []string{e.path} }
func (e *pathExpr) outputKind() protoreflect.Kind { return 0 } // pass-through
func (e *pathExpr) children() []Expr               { return nil }

func (e *pathExpr) eval(leafValues [][]Value, branchIdx int) Value {
	vals := leafValues[e.entryIdx]
	if branchIdx >= len(vals) {
		return Value{} // null / out-of-bounds
	}
	return vals[branchIdx]
}

// ---- Interior: function application ----

// funcKind identifies a built-in function. Using an enum + switch dispatch
// avoids interface-call overhead in the hot path.
type funcKind int

const (
	funcCoalesce funcKind = iota + 1
	funcDefault
	funcCond
	funcHas
	funcLen
	funcAdd
	funcSub
	funcMul
	funcDiv
	funcMod
	funcConcat

	// Wave 2 — predicates
	funcEq
	funcNe
	funcLt
	funcLe
	funcGt
	funcGe

	// Wave 2 — string functions
	funcUpper
	funcLower
	funcTrim
	funcTrimPrefix
	funcTrimSuffix

	// Wave 2 — math functions
	funcAbs
	funcCeil
	funcFloor
	funcRound
	funcMin
	funcMax

	// Wave 2 — cast functions
	funcCastInt
	funcCastFloat
	funcCastString

	// Wave 2 — timestamp functions
	funcStrptime
	funcTryStrptime
	funcAge
	funcExtractYear
	funcExtractMonth
	funcExtractDay
	funcExtractHour
	funcExtractMinute
	funcExtractSecond

	// Wave 3 — ETL functions
	funcHash
	funcEpochToDate
	funcDatePart
	funcBucket
	funcMask
	funcCoerce
	funcEnumName
	funcSum
	funcDistinct
	funcListConcat

	// Wave 4 — filter / logic functions
	funcSelect  // mid-traversal filter: pass-through if predicate is truthy, null otherwise
	funcAnd     // logical AND of two boolean children
	funcOr      // logical OR of two boolean children
	funcNot     // logical NOT of a boolean child
)

// funcExpr is an interior Expr node that applies a function to child
// expressions. The [8]protoreflect.Value scratch array avoids heap
// allocation when the function has ≤8 children.
type funcExpr struct {
	kind         funcKind
	kids         []Expr
	literal      Value                            // for Default: the fallback literal; Coerce: ifTrue; Mask: keepFirst (as int64)
	separator    string                           // for Concat: separator; TrimPrefix/Suffix: affix; Strptime: format; DatePart: part name; Mask: mask char; ListConcat: separator
	autoPromote  *bool                            // for Cond: per-expr override; nil = use plan default
	outKind      protoreflect.Kind                // cached output kind (0 = pass-through)
	literal2     Value                            // for Coerce: ifFalse
	intParam     int                              // for Bucket: size; Mask: keepLast
	enumDesc     protoreflect.EnumDescriptor      // for EnumName: resolved at plan compile time
}

func (e *funcExpr) outputKind() protoreflect.Kind { return e.outKind }
func (e *funcExpr) children() []Expr               { return e.kids }

func (e *funcExpr) inputPaths() []string {
	var paths []string
	for _, kid := range e.kids {
		paths = append(paths, kid.inputPaths()...)
	}
	return paths
}

// ---- Public constructors ----

// FuncCoalesce returns the first non-zero child value.
// All children must resolve to the same protoreflect.Kind.
func FuncCoalesce(children ...Expr) Expr {
	return &funcExpr{kind: funcCoalesce, kids: children}
}

// FuncDefault returns the child value if non-zero, otherwise the literal.
func FuncDefault(child Expr, literal Value) Expr {
	return &funcExpr{kind: funcDefault, kids: []Expr{child}, literal: literal}
}

// FuncCond evaluates predicate (child 0); if its value is non-zero, returns
// child 1 (then), otherwise child 2 (else).
// Use [CondWithAutoPromote] to override the plan-level auto-promote setting.
func FuncCond(predicate, then, els Expr) Expr {
	return &funcExpr{kind: funcCond, kids: []Expr{predicate, then, els}}
}

// CondWithAutoPromote returns a [FuncCond] with an explicit auto-promote
// override. When on is true and the then/else branches have different
// output kinds, the result is promoted to the wider type.
func CondWithAutoPromote(on bool, predicate, then, els Expr) Expr {
	return &funcExpr{kind: funcCond, kids: []Expr{predicate, then, els}, autoPromote: &on}
}

// FuncHas returns a Bool indicating whether the child path is set (non-zero).
// Output kind: BoolKind.
func FuncHas(child Expr) Expr {
	return &funcExpr{kind: funcHas, kids: []Expr{child}, outKind: protoreflect.BoolKind}
}

// FuncLen returns the length of a repeated/map/bytes/string field as Int64.
// Output kind: Int64Kind.
func FuncLen(child Expr) Expr {
	return &funcExpr{kind: funcLen, kids: []Expr{child}, outKind: protoreflect.Int64Kind}
}

// FuncAdd returns the sum of two numeric children (a + b).
// Go-style type promotion: mixed int/float promotes to float.
func FuncAdd(a, b Expr) Expr {
	return &funcExpr{kind: funcAdd, kids: []Expr{a, b}}
}

// FuncSub returns the difference of two numeric children (a - b).
func FuncSub(a, b Expr) Expr {
	return &funcExpr{kind: funcSub, kids: []Expr{a, b}}
}

// FuncMul returns the product of two numeric children (a * b).
func FuncMul(a, b Expr) Expr {
	return &funcExpr{kind: funcMul, kids: []Expr{a, b}}
}

// FuncDiv returns the quotient of two numeric children (a / b).
// Integer division truncates toward zero. Division by zero returns zero.
func FuncDiv(a, b Expr) Expr {
	return &funcExpr{kind: funcDiv, kids: []Expr{a, b}}
}

// FuncMod returns the remainder of two integer children (a % b).
// Mod by zero returns zero.
func FuncMod(a, b Expr) Expr {
	return &funcExpr{kind: funcMod, kids: []Expr{a, b}}
}

// FuncConcat joins the string representations of all children with sep.
// Output kind: StringKind.
func FuncConcat(sep string, children ...Expr) Expr {
	return &funcExpr{kind: funcConcat, kids: children, separator: sep, outKind: protoreflect.StringKind}
}

// ---- Predicate constructors ----

// FuncEq returns a Bool indicating whether a == b.
// Numeric operands use Go-style int→float promotion; strings use lexicographic comparison.
func FuncEq(a, b Expr) Expr {
	return &funcExpr{kind: funcEq, kids: []Expr{a, b}, outKind: protoreflect.BoolKind}
}

// FuncNe returns a Bool indicating whether a != b.
func FuncNe(a, b Expr) Expr {
	return &funcExpr{kind: funcNe, kids: []Expr{a, b}, outKind: protoreflect.BoolKind}
}

// FuncLt returns a Bool indicating whether a < b.
func FuncLt(a, b Expr) Expr {
	return &funcExpr{kind: funcLt, kids: []Expr{a, b}, outKind: protoreflect.BoolKind}
}

// FuncLe returns a Bool indicating whether a <= b.
func FuncLe(a, b Expr) Expr {
	return &funcExpr{kind: funcLe, kids: []Expr{a, b}, outKind: protoreflect.BoolKind}
}

// FuncGt returns a Bool indicating whether a > b.
func FuncGt(a, b Expr) Expr {
	return &funcExpr{kind: funcGt, kids: []Expr{a, b}, outKind: protoreflect.BoolKind}
}

// FuncGe returns a Bool indicating whether a >= b.
func FuncGe(a, b Expr) Expr {
	return &funcExpr{kind: funcGe, kids: []Expr{a, b}, outKind: protoreflect.BoolKind}
}

// ---- String function constructors ----

// FuncUpper returns the string value converted to upper case.
// Output kind: StringKind.
func FuncUpper(child Expr) Expr {
	return &funcExpr{kind: funcUpper, kids: []Expr{child}, outKind: protoreflect.StringKind}
}

// FuncLower returns the string value converted to lower case.
// Output kind: StringKind.
func FuncLower(child Expr) Expr {
	return &funcExpr{kind: funcLower, kids: []Expr{child}, outKind: protoreflect.StringKind}
}

// FuncTrim returns the string with leading and trailing whitespace removed.
// Output kind: StringKind.
func FuncTrim(child Expr) Expr {
	return &funcExpr{kind: funcTrim, kids: []Expr{child}, outKind: protoreflect.StringKind}
}

// FuncTrimPrefix returns the string with the given prefix removed (if present).
// The prefix is stored as the separator field.
// Output kind: StringKind.
func FuncTrimPrefix(child Expr, prefix string) Expr {
	return &funcExpr{kind: funcTrimPrefix, kids: []Expr{child}, separator: prefix, outKind: protoreflect.StringKind}
}

// FuncTrimSuffix returns the string with the given suffix removed (if present).
// The suffix is stored as the separator field.
// Output kind: StringKind.
func FuncTrimSuffix(child Expr, suffix string) Expr {
	return &funcExpr{kind: funcTrimSuffix, kids: []Expr{child}, separator: suffix, outKind: protoreflect.StringKind}
}

// ---- Math function constructors ----

// FuncAbs returns the absolute value of a numeric child.
// Preserves int vs float kind.
func FuncAbs(child Expr) Expr {
	return &funcExpr{kind: funcAbs, kids: []Expr{child}}
}

// FuncCeil returns the ceiling of a float child. No-op for integers.
// Preserves int vs float kind.
func FuncCeil(child Expr) Expr {
	return &funcExpr{kind: funcCeil, kids: []Expr{child}}
}

// FuncFloor returns the floor of a float child. No-op for integers.
// Preserves int vs float kind.
func FuncFloor(child Expr) Expr {
	return &funcExpr{kind: funcFloor, kids: []Expr{child}}
}

// FuncRound returns the nearest integer value of a float child (banker's rounding).
// No-op for integers. Preserves int vs float kind.
func FuncRound(child Expr) Expr {
	return &funcExpr{kind: funcRound, kids: []Expr{child}}
}

// FuncMin returns the smaller of two numeric children.
// Go-style int→float promotion for mixed types.
func FuncMin(a, b Expr) Expr {
	return &funcExpr{kind: funcMin, kids: []Expr{a, b}}
}

// FuncMax returns the larger of two numeric children.
// Go-style int→float promotion for mixed types.
func FuncMax(a, b Expr) Expr {
	return &funcExpr{kind: funcMax, kids: []Expr{a, b}}
}

// ---- Cast function constructors ----

// FuncCastInt casts the child to Int64.
// float→int64 (truncate), string→int64 (parse), bool→0/1.
// Output kind: Int64Kind.
func FuncCastInt(child Expr) Expr {
	return &funcExpr{kind: funcCastInt, kids: []Expr{child}, outKind: protoreflect.Int64Kind}
}

// FuncCastFloat casts the child to Float64 (Double).
// int→float64, string→float64 (parse), bool→0.0/1.0.
// Output kind: DoubleKind.
func FuncCastFloat(child Expr) Expr {
	return &funcExpr{kind: funcCastFloat, kids: []Expr{child}, outKind: protoreflect.DoubleKind}
}

// FuncCastString casts the child to String using [valueToString].
// Output kind: StringKind.
func FuncCastString(child Expr) Expr {
	return &funcExpr{kind: funcCastString, kids: []Expr{child}, outKind: protoreflect.StringKind}
}

// ---- Timestamp function constructors ----

// FuncStrptime parses a string child into a Unix-millisecond timestamp (Int64)
// using the given format. If the format contains '%' specifiers, it is
// interpreted as a DuckDB strptime format; otherwise it is treated as a Go
// [time.Parse] layout.
//
// Returns an invalid value on parse failure. The format is stored in the
// separator field.
// Output kind: Int64Kind.
func FuncStrptime(format string, child Expr) Expr {
	return &funcExpr{kind: funcStrptime, kids: []Expr{child}, separator: format, outKind: protoreflect.Int64Kind}
}

// FuncTryStrptime is like [FuncStrptime] but returns zero (epoch) instead of
// an invalid value on parse failure.
// Output kind: Int64Kind.
func FuncTryStrptime(format string, child Expr) Expr {
	return &funcExpr{kind: funcTryStrptime, kids: []Expr{child}, separator: format, outKind: protoreflect.Int64Kind}
}

// FuncAge computes the duration in milliseconds between timestamps.
//   - 1 argument: now − child (age of the timestamp).
//   - 2 arguments: child[0] − child[1] (difference between two timestamps).
//
// Children must be Int64 Unix-millisecond values (e.g. from Strptime).
// Output kind: Int64Kind.
func FuncAge(children ...Expr) Expr {
	return &funcExpr{kind: funcAge, kids: children, outKind: protoreflect.Int64Kind}
}

// FuncExtractYear extracts the year from a Unix-millisecond timestamp (Int64).
// Output kind: Int64Kind.
func FuncExtractYear(child Expr) Expr {
	return &funcExpr{kind: funcExtractYear, kids: []Expr{child}, outKind: protoreflect.Int64Kind}
}

// FuncExtractMonth extracts the month (1–12) from a Unix-millisecond timestamp.
// Output kind: Int64Kind.
func FuncExtractMonth(child Expr) Expr {
	return &funcExpr{kind: funcExtractMonth, kids: []Expr{child}, outKind: protoreflect.Int64Kind}
}

// FuncExtractDay extracts the day of month (1–31) from a Unix-millisecond timestamp.
// Output kind: Int64Kind.
func FuncExtractDay(child Expr) Expr {
	return &funcExpr{kind: funcExtractDay, kids: []Expr{child}, outKind: protoreflect.Int64Kind}
}

// FuncExtractHour extracts the hour (0–23) from a Unix-millisecond timestamp.
// Output kind: Int64Kind.
func FuncExtractHour(child Expr) Expr {
	return &funcExpr{kind: funcExtractHour, kids: []Expr{child}, outKind: protoreflect.Int64Kind}
}

// FuncExtractMinute extracts the minute (0–59) from a Unix-millisecond timestamp.
// Output kind: Int64Kind.
func FuncExtractMinute(child Expr) Expr {
	return &funcExpr{kind: funcExtractMinute, kids: []Expr{child}, outKind: protoreflect.Int64Kind}
}

// FuncExtractSecond extracts the second (0–59) from a Unix-millisecond timestamp.
// Output kind: Int64Kind.
func FuncExtractSecond(child Expr) Expr {
	return &funcExpr{kind: funcExtractSecond, kids: []Expr{child}, outKind: protoreflect.Int64Kind}
}

// ---- Wave 3 — ETL function constructors ----

// FuncHash returns an FNV-1a 64-bit hash of the child's string representation.
// Output kind: Int64Kind.
func FuncHash(children ...Expr) Expr {
	return &funcExpr{kind: funcHash, kids: children, outKind: protoreflect.Int64Kind}
}

// FuncEpochToDate converts a Unix-epoch-second timestamp (Int64) to a
// day-offset (Int32) by dividing by 86 400. Useful for date-only columns.
// Output kind: Int32Kind.
// TODO: ideally maps to Arrow Date32 in typemap.go — deferred.
func FuncEpochToDate(child Expr) Expr {
	return &funcExpr{kind: funcEpochToDate, kids: []Expr{child}, outKind: protoreflect.Int32Kind}
}

// FuncDatePart extracts a calendar component from a Unix-epoch-second
// timestamp. Supported parts: "year", "month", "day", "hour", "minute",
// "second". The part name is stored in the separator field.
// Output kind: Int64Kind.
func FuncDatePart(part string, child Expr) Expr {
	return &funcExpr{kind: funcDatePart, kids: []Expr{child}, separator: part, outKind: protoreflect.Int64Kind}
}

// FuncBucket floors the integer child value to the nearest multiple of size.
// result = value − value%size. Output kind: pass-through.
func FuncBucket(child Expr, size int) Expr {
	return &funcExpr{kind: funcBucket, kids: []Expr{child}, intParam: size}
}

// FuncMask redacts the interior of a string, keeping keepFirst leading
// characters and keepLast trailing characters. The interior is replaced
// with repetitions of maskChar (default "*"). Output kind: StringKind.
func FuncMask(child Expr, keepFirst, keepLast int, maskChar string) Expr {
	if maskChar == "" {
		maskChar = "*"
	}
	return &funcExpr{
		kind:      funcMask,
		kids:      []Expr{child},
		literal:   ScalarInt64(int64(keepFirst)),
		intParam:  keepLast,
		separator: maskChar,
		outKind:   protoreflect.StringKind,
	}
}

// FuncCoerce maps a boolean child to one of two literal values.
// If the child is non-zero (true), ifTrue is returned; otherwise ifFalse.
// Output kind: StringKind.
func FuncCoerce(child Expr, ifTrue, ifFalse Value) Expr {
	return &funcExpr{
		kind:     funcCoerce,
		kids:     []Expr{child},
		literal:  ifTrue,
		literal2: ifFalse,
		outKind:  protoreflect.StringKind,
	}
}

// FuncEnumName maps an enum-typed field to its string name.
// The [protoreflect.EnumDescriptor] is resolved at [NewPlan] time by
// inspecting the child leaf's terminal field descriptor.
// Output kind: StringKind.
func FuncEnumName(child Expr) Expr {
	return &funcExpr{kind: funcEnumName, kids: []Expr{child}, outKind: protoreflect.StringKind}
}

// FuncSum is an aggregate that sums all fan-out branches of the child.
// The result is the same for every branch. Output kind: pass-through.
func FuncSum(child Expr) Expr {
	return &funcExpr{kind: funcSum, kids: []Expr{child}}
}

// FuncDistinct is an aggregate that counts the number of distinct values
// across all fan-out branches of the child.
// Output kind: Int64Kind.
func FuncDistinct(child Expr) Expr {
	return &funcExpr{kind: funcDistinct, kids: []Expr{child}, outKind: protoreflect.Int64Kind}
}

// FuncListConcat is an aggregate that joins the string representation of
// all fan-out branches of the child with the given separator.
// Output kind: StringKind.
func FuncListConcat(child Expr, sep string) Expr {
	return &funcExpr{kind: funcListConcat, kids: []Expr{child}, separator: sep, outKind: protoreflect.StringKind}
}

// ---- Wave 4 — Filter / logic constructors ----

// FuncSelect evaluates a predicate child and, if truthy, returns the value
// of the second child (the "input" being filtered). If the predicate is falsy,
// returns null. This gives jq-style `| select(pred)` semantics.
//
// Usage: FuncSelect(predicate, inputPathRef)
// The first child is the boolean predicate; the second is the value to
// pass through (typically the same path being filtered).
// Output kind: pass-through (same as the input child).
func FuncSelect(predicate, input Expr) Expr {
	return &funcExpr{kind: funcSelect, kids: []Expr{predicate, input}}
}

// FuncAnd returns the logical AND of two boolean children.
// Both children should evaluate to boolean-like values; the result is true
// only when both are truthy (non-null, non-zero).
// Output kind: BoolKind.
func FuncAnd(a, b Expr) Expr {
	return &funcExpr{kind: funcAnd, kids: []Expr{a, b}, outKind: protoreflect.BoolKind}
}

// FuncOr returns the logical OR of two boolean children.
// The result is true when at least one child is truthy.
// Output kind: BoolKind.
func FuncOr(a, b Expr) Expr {
	return &funcExpr{kind: funcOr, kids: []Expr{a, b}, outKind: protoreflect.BoolKind}
}

// FuncNot returns the logical NOT of a boolean child.
// The result is true when the child is falsy (null, zero, false, empty).
// Output kind: BoolKind.
func FuncNot(child Expr) Expr {
	return &funcExpr{kind: funcNot, kids: []Expr{child}, outKind: protoreflect.BoolKind}
}

// ---- Filter predicate expressions ----

// filterPathExpr is a leaf [Expr] that references a field path relative to
// the current cursor message (used inside filter predicates like [?(.name == "x")]).
// Unlike [pathExpr], which references a path from the root, filterPathExpr
// is evaluated at traversal time against the per-branch cursor value.
//
// The field descriptors are resolved at parse time (when the message descriptor
// is known), so eval is a direct field lookup chain with no parsing overhead.
type filterPathExpr struct {
	relPath string                         // relative path string for display (e.g. "name" or "inner.id")
	fields  []protoreflect.FieldDescriptor // resolved chain of field descriptors
}

// FilterPathRef creates a leaf [Expr] referencing a field path relative to
// the current message cursor. Used inside filter predicates.
// The fields parameter is the resolved chain of field descriptors from the
// cursor message to the target field.
func FilterPathRef(relPath string, fields ...protoreflect.FieldDescriptor) Expr {
	return &filterPathExpr{relPath: relPath, fields: fields}
}

func (e *filterPathExpr) inputPaths() []string           { return nil } // not a root-relative path
func (e *filterPathExpr) outputKind() protoreflect.Kind { return 0 }
func (e *filterPathExpr) children() []Expr               { return nil }

// eval evaluates the filter path against the cursor message stored in
// leafValues[0][branchIdx] (by convention, filter evaluation passes the
// cursor as a single-element slice at index 0).
//
// The traversal walks the resolved field descriptor chain from the cursor
// message to the target field, performing direct protoreflect.Message.Get()
// calls without any Plan compilation overhead.
func (e *filterPathExpr) eval(leafValues [][]Value, branchIdx int) Value {
	if len(leafValues) == 0 || branchIdx >= len(leafValues[0]) {
		return Null()
	}
	cursor := leafValues[0][branchIdx]
	if cursor.Kind() != MessageKind || cursor.Message() == nil {
		return Null()
	}
	msg := cursor.Message()
	// Walk the field chain.
	for i, fd := range e.fields {
		val := msg.Get(fd)
		if i == len(e.fields)-1 {
			// Last field — return the value.
			return FromProtoValue(val)
		}
		// Intermediate field — must be a message to continue traversal.
		if fd.Message() == nil {
			return Null() // non-message intermediate → can't traverse further
		}
		msg = val.Message()
	}
	return Null()
}

// literalExpr is a leaf [Expr] that always returns a fixed [Value].
// Used for literal constants inside filter predicates (e.g. the "x" in
// [?(.name == "x")] or the 42 in [?(.count > 42)]).
type literalExpr struct {
	val     Value
	litKind protoreflect.Kind // output kind for Arrow type inference
}

// Literal creates a leaf [Expr] that always returns val.
// The kind parameter determines the output kind for Arrow type inference;
// pass 0 for automatic (inferred from val's protoreflect.Value).
func Literal(val Value, kind protoreflect.Kind) Expr {
	return &literalExpr{val: val, litKind: kind}
}

func (e *literalExpr) inputPaths() []string           { return nil }
func (e *literalExpr) outputKind() protoreflect.Kind   { return e.litKind }
func (e *literalExpr) children() []Expr                 { return nil }
func (e *literalExpr) eval(_ [][]Value, _ int) Value { return e.val }

// resolvePathExprs recursively walks the Expr tree and collects all
// pathExpr leaves. Returns the list of paths found.
func resolvePathExprs(e Expr) []*pathExpr {
	switch v := e.(type) {
	case *pathExpr:
		return []*pathExpr{v}
	case *funcExpr:
		var all []*pathExpr
		for _, kid := range v.kids {
			all = append(all, resolvePathExprs(kid)...)
		}
		return all
	default:
		return nil
	}
}
