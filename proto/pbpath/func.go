package pbpath

import (
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// funcEval dispatches funcExpr evaluation.
// It is called from funcExpr.eval with the resolved leaf slices and the
// current branch index. The [8]Value scratch avoids a heap allocation
// when arity ≤ 8.
func (e *funcExpr) eval(leafValues [][]protoreflect.Value, branchIdx int) protoreflect.Value {
	switch e.kind {
	case funcCoalesce:
		return evalCoalesce(e, leafValues, branchIdx)
	case funcDefault:
		return evalDefault(e, leafValues, branchIdx)
	case funcCond:
		return evalCond(e, leafValues, branchIdx)
	case funcHas:
		return evalHas(e, leafValues, branchIdx)
	case funcLen:
		return evalLen(e, leafValues, branchIdx)
	case funcAdd:
		return evalArith(e, leafValues, branchIdx, funcAdd)
	case funcSub:
		return evalArith(e, leafValues, branchIdx, funcSub)
	case funcMul:
		return evalArith(e, leafValues, branchIdx, funcMul)
	case funcDiv:
		return evalArith(e, leafValues, branchIdx, funcDiv)
	case funcMod:
		return evalArith(e, leafValues, branchIdx, funcMod)
	case funcConcat:
		return evalConcat(e, leafValues, branchIdx)
	// Wave 2 — predicates
	case funcEq, funcNe, funcLt, funcLe, funcGt, funcGe:
		return evalCompare(e, leafValues, branchIdx)
	// Wave 2 — string functions
	case funcUpper:
		return evalStringUnary(e, leafValues, branchIdx, strings.ToUpper)
	case funcLower:
		return evalStringUnary(e, leafValues, branchIdx, strings.ToLower)
	case funcTrim:
		return evalStringUnary(e, leafValues, branchIdx, strings.TrimSpace)
	case funcTrimPrefix:
		return evalTrimFix(e, leafValues, branchIdx, strings.TrimPrefix)
	case funcTrimSuffix:
		return evalTrimFix(e, leafValues, branchIdx, strings.TrimSuffix)
	// Wave 2 — math functions
	case funcAbs:
		return evalMathUnary(e, leafValues, branchIdx, funcAbs)
	case funcCeil:
		return evalMathUnary(e, leafValues, branchIdx, funcCeil)
	case funcFloor:
		return evalMathUnary(e, leafValues, branchIdx, funcFloor)
	case funcRound:
		return evalMathUnary(e, leafValues, branchIdx, funcRound)
	case funcMin:
		return evalMinMax(e, leafValues, branchIdx, true)
	case funcMax:
		return evalMinMax(e, leafValues, branchIdx, false)
	// Wave 2 — cast functions
	case funcCastInt:
		return evalCastInt(e, leafValues, branchIdx)
	case funcCastFloat:
		return evalCastFloat(e, leafValues, branchIdx)
	case funcCastString:
		return evalCastString(e, leafValues, branchIdx)
	// Wave 2 — timestamp functions
	case funcStrptime:
		return evalStrptime(e, leafValues, branchIdx, false)
	case funcTryStrptime:
		return evalStrptime(e, leafValues, branchIdx, true)
	case funcAge:
		return evalAge(e, leafValues, branchIdx)
	case funcExtractYear, funcExtractMonth, funcExtractDay,
		funcExtractHour, funcExtractMinute, funcExtractSecond:
		return evalExtract(e, leafValues, branchIdx)
	// Wave 3 — ETL functions
	case funcHash:
		return evalHash(e, leafValues, branchIdx)
	case funcEpochToDate:
		return evalEpochToDate(e, leafValues, branchIdx)
	case funcDatePart:
		return evalDatePart(e, leafValues, branchIdx)
	case funcBucket:
		return evalBucket(e, leafValues, branchIdx)
	case funcMask:
		return evalMask(e, leafValues, branchIdx)
	case funcCoerce:
		return evalCoerce(e, leafValues, branchIdx)
	case funcEnumName:
		return evalEnumName(e, leafValues, branchIdx)
	case funcSum:
		return evalSum(e, leafValues, branchIdx)
	case funcDistinct:
		return evalDistinct(e, leafValues, branchIdx)
	case funcListConcat:
		return evalListConcat(e, leafValues, branchIdx)
	default:
		return protoreflect.Value{} // unknown function → null
	}
}

// ---- Coalesce ----

// evalCoalesce returns the first child value that is non-zero.
// In proto3 semantics, an unset scalar field returns the default (e.g. 0 for
// int64) which is a valid protoreflect.Value. Coalesce skips zero values,
// treating them as "null".
func evalCoalesce(e *funcExpr, lv [][]protoreflect.Value, bi int) protoreflect.Value {
	for _, kid := range e.kids {
		v := kid.eval(lv, bi)
		if isNonZero(v) {
			return v
		}
	}
	return protoreflect.Value{}
}

// ---- Default ----

// evalDefault returns the child value if non-zero, otherwise the literal.
// See [evalCoalesce] for the rationale on using isNonZero.
func evalDefault(e *funcExpr, lv [][]protoreflect.Value, bi int) protoreflect.Value {
	v := e.kids[0].eval(lv, bi)
	if isNonZero(v) {
		return v
	}
	return e.literal
}

// ---- Cond (if/then/else) ----

// evalCond evaluates predicate → if non-zero returns then-branch, else else-branch.
func evalCond(e *funcExpr, lv [][]protoreflect.Value, bi int) protoreflect.Value {
	pred := e.kids[0].eval(lv, bi)
	if isNonZero(pred) {
		return e.kids[1].eval(lv, bi)
	}
	return e.kids[2].eval(lv, bi)
}

// ---- Has ----

// evalHas returns Bool true if the child evaluates to a valid, non-zero value.
func evalHas(e *funcExpr, lv [][]protoreflect.Value, bi int) protoreflect.Value {
	v := e.kids[0].eval(lv, bi)
	return protoreflect.ValueOfBool(v.IsValid() && isNonZero(v))
}

// ---- Len ----

// evalLen returns the length of a repeated/map/string/bytes field as Int64.
// Scalars return 0, invalid values return 0.
func evalLen(e *funcExpr, lv [][]protoreflect.Value, bi int) protoreflect.Value {
	v := e.kids[0].eval(lv, bi)
	if !v.IsValid() {
		return protoreflect.ValueOfInt64(0)
	}
	// Try List, Map, String, Bytes — in approximate frequency order.
	switch iface := v.Interface().(type) {
	case protoreflect.List:
		return protoreflect.ValueOfInt64(int64(iface.Len()))
	case protoreflect.Map:
		return protoreflect.ValueOfInt64(int64(iface.Len()))
	case string:
		return protoreflect.ValueOfInt64(int64(len(iface)))
	case []byte:
		return protoreflect.ValueOfInt64(int64(len(iface)))
	default:
		return protoreflect.ValueOfInt64(0)
	}
}

// ---- Arithmetic (Add, Sub, Mul, Div, Mod) ----

// evalArith handles all binary arithmetic operations. If either operand is
// invalid (null), the result is invalid.
//
// Type promotion rules (Go-style):
//   - Both int  → int result
//   - Mixed int/float → float result (int is promoted)
//   - Both float → float result
//   - Non-numeric → invalid
func evalArith(e *funcExpr, lv [][]protoreflect.Value, bi int, op funcKind) protoreflect.Value {
	a := e.kids[0].eval(lv, bi)
	b := e.kids[1].eval(lv, bi)
	if !a.IsValid() || !b.IsValid() {
		return protoreflect.Value{}
	}

	aInt, aIsInt := toInt64(a)
	bInt, bIsInt := toInt64(b)

	if aIsInt && bIsInt {
		return intArith(aInt, bInt, op)
	}

	// At least one is float (or promote int to float).
	aFloat, aOk := toFloat64(a, aInt, aIsInt)
	bFloat, bOk := toFloat64(b, bInt, bIsInt)
	if !aOk || !bOk {
		return protoreflect.Value{} // non-numeric
	}
	return floatArith(aFloat, bFloat, op)
}

func intArith(a, b int64, op funcKind) protoreflect.Value {
	switch op {
	case funcAdd:
		return protoreflect.ValueOfInt64(a + b)
	case funcSub:
		return protoreflect.ValueOfInt64(a - b)
	case funcMul:
		return protoreflect.ValueOfInt64(a * b)
	case funcDiv:
		if b == 0 {
			return protoreflect.ValueOfInt64(0)
		}
		return protoreflect.ValueOfInt64(a / b) // truncating
	case funcMod:
		if b == 0 {
			return protoreflect.ValueOfInt64(0)
		}
		return protoreflect.ValueOfInt64(a % b)
	default:
		return protoreflect.Value{}
	}
}

func floatArith(a, b float64, op funcKind) protoreflect.Value {
	switch op {
	case funcAdd:
		return protoreflect.ValueOfFloat64(a + b)
	case funcSub:
		return protoreflect.ValueOfFloat64(a - b)
	case funcMul:
		return protoreflect.ValueOfFloat64(a * b)
	case funcDiv:
		if b == 0 {
			return protoreflect.ValueOfFloat64(0)
		}
		return protoreflect.ValueOfFloat64(a / b)
	case funcMod:
		if b == 0 {
			return protoreflect.ValueOfFloat64(0)
		}
		return protoreflect.ValueOfFloat64(math.Mod(a, b))
	default:
		return protoreflect.Value{}
	}
}

// ---- Concat ----

// evalConcat joins the string representations of all children with the
// separator. If any child is invalid, it is treated as an empty string.
func evalConcat(e *funcExpr, lv [][]protoreflect.Value, bi int) protoreflect.Value {
	var b strings.Builder
	for i, kid := range e.kids {
		if i > 0 && e.separator != "" {
			b.WriteString(e.separator)
		}
		v := kid.eval(lv, bi)
		if v.IsValid() {
			b.WriteString(valueToString(v))
		}
	}
	return protoreflect.ValueOfString(b.String())
}

// ---- Helpers ----

// compareResult is the outcome of a type-aware comparison: -1, 0, or 1.
// cmpOk is false when the operands are non-comparable (e.g. different
// non-numeric/non-string types).
func compareValues(a, b protoreflect.Value) (cmp int, ok bool) {
	if !a.IsValid() || !b.IsValid() {
		return 0, false
	}

	// string vs string
	aStr, aIsStr := a.Interface().(string)
	bStr, bIsStr := b.Interface().(string)
	if aIsStr && bIsStr {
		switch {
		case aStr < bStr:
			return -1, true
		case aStr > bStr:
			return 1, true
		default:
			return 0, true
		}
	}

	// bool vs bool
	aBool, aIsBool := a.Interface().(bool)
	bBool, bIsBool := b.Interface().(bool)
	if aIsBool && bIsBool {
		ai, bi := int64(0), int64(0)
		if aBool {
			ai = 1
		}
		if bBool {
			bi = 1
		}
		switch {
		case ai < bi:
			return -1, true
		case ai > bi:
			return 1, true
		default:
			return 0, true
		}
	}

	// numeric comparison with int→float promotion
	aInt, aIsInt := toInt64(a)
	bInt, bIsInt := toInt64(b)

	if aIsInt && bIsInt {
		switch {
		case aInt < bInt:
			return -1, true
		case aInt > bInt:
			return 1, true
		default:
			return 0, true
		}
	}

	aFloat, aOk := toFloat64(a, aInt, aIsInt)
	bFloat, bOk := toFloat64(b, bInt, bIsInt)
	if aOk && bOk {
		switch {
		case aFloat < bFloat:
			return -1, true
		case aFloat > bFloat:
			return 1, true
		default:
			return 0, true
		}
	}
	return 0, false
}

// ---- Compare (Eq, Ne, Lt, Le, Gt, Ge) ----

// evalCompare handles all binary comparison operations. If either operand is
// invalid or the types are non-comparable, returns false.
func evalCompare(e *funcExpr, lv [][]protoreflect.Value, bi int) protoreflect.Value {
	a := e.kids[0].eval(lv, bi)
	b := e.kids[1].eval(lv, bi)

	cmp, ok := compareValues(a, b)
	if !ok {
		return protoreflect.ValueOfBool(false)
	}

	var result bool
	switch e.kind {
	case funcEq:
		result = cmp == 0
	case funcNe:
		result = cmp != 0
	case funcLt:
		result = cmp < 0
	case funcLe:
		result = cmp <= 0
	case funcGt:
		result = cmp > 0
	case funcGe:
		result = cmp >= 0
	}
	return protoreflect.ValueOfBool(result)
}

// ---- String functions ----

// evalStringUnary applies a unary string→string function.
// If the child is not a string, converts via valueToString first.
func evalStringUnary(e *funcExpr, lv [][]protoreflect.Value, bi int, fn func(string) string) protoreflect.Value {
	v := e.kids[0].eval(lv, bi)
	if !v.IsValid() {
		return protoreflect.ValueOfString("")
	}
	s, ok := v.Interface().(string)
	if !ok {
		s = valueToString(v)
	}
	return protoreflect.ValueOfString(fn(s))
}

// evalTrimFix applies TrimPrefix or TrimSuffix. The fix string is stored
// in funcExpr.separator.
func evalTrimFix(e *funcExpr, lv [][]protoreflect.Value, bi int, fn func(string, string) string) protoreflect.Value {
	v := e.kids[0].eval(lv, bi)
	if !v.IsValid() {
		return protoreflect.ValueOfString("")
	}
	s, ok := v.Interface().(string)
	if !ok {
		s = valueToString(v)
	}
	return protoreflect.ValueOfString(fn(s, e.separator))
}

// ---- Math functions (unary) ----

// evalMathUnary applies Abs, Ceil, Floor, or Round. Preserves int vs float kind.
func evalMathUnary(e *funcExpr, lv [][]protoreflect.Value, bi int, op funcKind) protoreflect.Value {
	v := e.kids[0].eval(lv, bi)
	if !v.IsValid() {
		return protoreflect.Value{}
	}

	if i, ok := toInt64(v); ok {
		switch op {
		case funcAbs:
			if i < 0 {
				i = -i
			}
			return protoreflect.ValueOfInt64(i)
		case funcCeil, funcFloor, funcRound:
			// No-op for integers.
			return protoreflect.ValueOfInt64(i)
		}
	}

	switch iface := v.Interface().(type) {
	case float32:
		f := float64(iface)
		return protoreflect.ValueOfFloat64(applyMathFloat(f, op))
	case float64:
		return protoreflect.ValueOfFloat64(applyMathFloat(iface, op))
	default:
		return protoreflect.Value{} // non-numeric
	}
}

func applyMathFloat(f float64, op funcKind) float64 {
	switch op {
	case funcAbs:
		return math.Abs(f)
	case funcCeil:
		return math.Ceil(f)
	case funcFloor:
		return math.Floor(f)
	case funcRound:
		return math.RoundToEven(f)
	default:
		return f
	}
}

// ---- Min / Max ----

// evalMinMax returns the minimum (isMin=true) or maximum (isMin=false) of two
// numeric children. Go-style int→float promotion for mixed types.
func evalMinMax(e *funcExpr, lv [][]protoreflect.Value, bi int, isMin bool) protoreflect.Value {
	a := e.kids[0].eval(lv, bi)
	b := e.kids[1].eval(lv, bi)
	if !a.IsValid() || !b.IsValid() {
		return protoreflect.Value{}
	}

	aInt, aIsInt := toInt64(a)
	bInt, bIsInt := toInt64(b)

	if aIsInt && bIsInt {
		if isMin {
			if aInt < bInt {
				return protoreflect.ValueOfInt64(aInt)
			}
			return protoreflect.ValueOfInt64(bInt)
		}
		if aInt > bInt {
			return protoreflect.ValueOfInt64(aInt)
		}
		return protoreflect.ValueOfInt64(bInt)
	}

	aFloat, aOk := toFloat64(a, aInt, aIsInt)
	bFloat, bOk := toFloat64(b, bInt, bIsInt)
	if !aOk || !bOk {
		return protoreflect.Value{}
	}
	if isMin {
		return protoreflect.ValueOfFloat64(math.Min(aFloat, bFloat))
	}
	return protoreflect.ValueOfFloat64(math.Max(aFloat, bFloat))
}

// ---- Cast functions ----

// evalCastInt casts the child value to Int64.
func evalCastInt(e *funcExpr, lv [][]protoreflect.Value, bi int) protoreflect.Value {
	v := e.kids[0].eval(lv, bi)
	if !v.IsValid() {
		return protoreflect.Value{}
	}
	// Already int?
	if i, ok := toInt64(v); ok {
		return protoreflect.ValueOfInt64(i)
	}
	switch iface := v.Interface().(type) {
	case float32:
		return protoreflect.ValueOfInt64(int64(iface))
	case float64:
		return protoreflect.ValueOfInt64(int64(iface))
	case string:
		n, err := strconv.ParseInt(iface, 10, 64)
		if err != nil {
			// Try float parse then truncate.
			f, err2 := strconv.ParseFloat(iface, 64)
			if err2 != nil {
				return protoreflect.Value{} // unparseable
			}
			return protoreflect.ValueOfInt64(int64(f))
		}
		return protoreflect.ValueOfInt64(n)
	default:
		return protoreflect.Value{}
	}
}

// evalCastFloat casts the child value to Float64 (DoubleKind).
func evalCastFloat(e *funcExpr, lv [][]protoreflect.Value, bi int) protoreflect.Value {
	v := e.kids[0].eval(lv, bi)
	if !v.IsValid() {
		return protoreflect.Value{}
	}
	if i, ok := toInt64(v); ok {
		return protoreflect.ValueOfFloat64(float64(i))
	}
	switch iface := v.Interface().(type) {
	case float32:
		return protoreflect.ValueOfFloat64(float64(iface))
	case float64:
		return protoreflect.ValueOfFloat64(iface)
	case string:
		f, err := strconv.ParseFloat(iface, 64)
		if err != nil {
			return protoreflect.Value{} // unparseable
		}
		return protoreflect.ValueOfFloat64(f)
	default:
		return protoreflect.Value{}
	}
}

// evalCastString casts the child value to String.
func evalCastString(e *funcExpr, lv [][]protoreflect.Value, bi int) protoreflect.Value {
	v := e.kids[0].eval(lv, bi)
	if !v.IsValid() {
		return protoreflect.ValueOfString("")
	}
	return protoreflect.ValueOfString(valueToString(v))
}

// ---- Timestamp functions ----

// evalStrptime parses a string child into a Unix-millisecond Int64 timestamp.
// When tryMode is true, parse failures return zero (epoch) instead of invalid.
func evalStrptime(e *funcExpr, lv [][]protoreflect.Value, bi int, tryMode bool) protoreflect.Value {
	v := e.kids[0].eval(lv, bi)
	if !v.IsValid() {
		if tryMode {
			return protoreflect.ValueOfInt64(0)
		}
		return protoreflect.Value{}
	}
	s, ok := v.Interface().(string)
	if !ok {
		s = valueToString(v)
	}

	t, err := ParseStrptime(e.separator, s)
	if err != nil {
		if tryMode {
			return protoreflect.ValueOfInt64(0)
		}
		return protoreflect.Value{}
	}
	return protoreflect.ValueOfInt64(t.UnixMilli())
}

// evalAge computes the duration between timestamps in milliseconds.
// 1 child: now − child. 2 children: child[0] − child[1].
func evalAge(e *funcExpr, lv [][]protoreflect.Value, bi int) protoreflect.Value {
	switch len(e.kids) {
	case 1:
		v := e.kids[0].eval(lv, bi)
		if !v.IsValid() {
			return protoreflect.Value{}
		}
		ts, ok := toInt64(v)
		if !ok {
			return protoreflect.Value{}
		}
		nowMs := time.Now().UnixMilli()
		return protoreflect.ValueOfInt64(nowMs - ts)
	case 2:
		a := e.kids[0].eval(lv, bi)
		b := e.kids[1].eval(lv, bi)
		if !a.IsValid() || !b.IsValid() {
			return protoreflect.Value{}
		}
		aTs, aOk := toInt64(a)
		bTs, bOk := toInt64(b)
		if !aOk || !bOk {
			return protoreflect.Value{}
		}
		return protoreflect.ValueOfInt64(aTs - bTs)
	default:
		return protoreflect.Value{}
	}
}

// evalExtract extracts a date/time component from a Unix-millisecond timestamp.
func evalExtract(e *funcExpr, lv [][]protoreflect.Value, bi int) protoreflect.Value {
	v := e.kids[0].eval(lv, bi)
	if !v.IsValid() {
		return protoreflect.Value{}
	}
	ms, ok := toInt64(v)
	if !ok {
		return protoreflect.Value{}
	}
	t := time.UnixMilli(ms).UTC()
	var result int64
	switch e.kind {
	case funcExtractYear:
		result = int64(t.Year())
	case funcExtractMonth:
		result = int64(t.Month())
	case funcExtractDay:
		result = int64(t.Day())
	case funcExtractHour:
		result = int64(t.Hour())
	case funcExtractMinute:
		result = int64(t.Minute())
	case funcExtractSecond:
		result = int64(t.Second())
	}
	return protoreflect.ValueOfInt64(result)
}

// ---- Shared helpers ----

// isNonZero returns true when v is valid and not the protobuf zero value
// for its kind.
func isNonZero(v protoreflect.Value) bool {
	if !v.IsValid() {
		return false
	}
	switch iface := v.Interface().(type) {
	case bool:
		return iface
	case int32:
		return iface != 0
	case int64:
		return iface != 0
	case uint32:
		return iface != 0
	case uint64:
		return iface != 0
	case float32:
		return iface != 0
	case float64:
		return iface != 0
	case string:
		return iface != ""
	case []byte:
		return len(iface) != 0
	case protoreflect.EnumNumber:
		return iface != 0
	case protoreflect.List:
		return iface.Len() > 0
	case protoreflect.Map:
		return iface.Len() > 0
	case protoreflect.Message:
		return true // a set message is always non-zero
	default:
		return true
	}
}

// toInt64 attempts to extract an integer value. Returns (val, true) for
// int32, int64, uint32, uint64, sint32, sint64, sfixed32/64, fixed32/64,
// enum, and bool.
func toInt64(v protoreflect.Value) (int64, bool) {
	switch iface := v.Interface().(type) {
	case int32:
		return int64(iface), true
	case int64:
		return iface, true
	case uint32:
		return int64(iface), true
	case uint64:
		return int64(iface), true
	case protoreflect.EnumNumber:
		return int64(iface), true
	case bool:
		if iface {
			return 1, true
		}
		return 0, true
	default:
		return 0, false
	}
}

// toFloat64 attempts to extract or promote to float64. If the value was
// already parsed as int64, it promotes from that. Otherwise tries the
// float interface types.
func toFloat64(v protoreflect.Value, asInt int64, wasInt bool) (float64, bool) {
	if wasInt {
		return float64(asInt), true
	}
	switch iface := v.Interface().(type) {
	case float32:
		return float64(iface), true
	case float64:
		return iface, true
	default:
		return 0, false
	}
}

// valueToString converts a protoreflect.Value to its string representation
// for use in Concat.
func valueToString(v protoreflect.Value) string {
	switch iface := v.Interface().(type) {
	case string:
		return iface
	case bool:
		if iface {
			return "true"
		}
		return "false"
	case int32:
		return strconv.FormatInt(int64(iface), 10)
	case int64:
		return strconv.FormatInt(iface, 10)
	case uint32:
		return strconv.FormatUint(uint64(iface), 10)
	case uint64:
		return strconv.FormatUint(iface, 10)
	case float32:
		return strconv.FormatFloat(float64(iface), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(iface, 'f', -1, 64)
	case []byte:
		return fmt.Sprintf("%x", iface)
	case protoreflect.EnumNumber:
		return strconv.FormatInt(int64(iface), 10)
	case protoreflect.Message:
		return "<message>"
	case protoreflect.List:
		return "<list>"
	case protoreflect.Map:
		return "<map>"
	default:
		return fmt.Sprintf("%v", iface)
	}
}

// ════════════════════════════════════════════════════════════════════════
// Wave 3 — ETL functions
// ════════════════════════════════════════════════════════════════════════

// ---- Hash ----

// evalHash computes an FNV-1a 64-bit hash of the concatenated string
// representations of all children (no separator).
func evalHash(e *funcExpr, leafValues [][]protoreflect.Value, branchIdx int) protoreflect.Value {
	h := fnv.New64a()
	for _, kid := range e.kids {
		v := kid.eval(leafValues, branchIdx)
		if !v.IsValid() {
			continue
		}
		_, _ = h.Write([]byte(valueToString(v)))
	}
	return protoreflect.ValueOfInt64(int64(h.Sum64()))
}

// ---- EpochToDate ----

// evalEpochToDate converts a Unix-epoch-second value to a day offset
// (seconds / 86400) as Int32.
func evalEpochToDate(e *funcExpr, leafValues [][]protoreflect.Value, branchIdx int) protoreflect.Value {
	v := e.kids[0].eval(leafValues, branchIdx)
	if !v.IsValid() {
		return protoreflect.Value{}
	}
	sec, ok := toInt64(v)
	if !ok {
		return protoreflect.Value{}
	}
	return protoreflect.ValueOfInt32(int32(sec / 86400))
}

// ---- DatePart ----

// evalDatePart extracts a calendar component from a Unix-epoch-second value.
// The component name is in e.separator.
func evalDatePart(e *funcExpr, leafValues [][]protoreflect.Value, branchIdx int) protoreflect.Value {
	v := e.kids[0].eval(leafValues, branchIdx)
	if !v.IsValid() {
		return protoreflect.Value{}
	}
	sec, ok := toInt64(v)
	if !ok {
		return protoreflect.Value{}
	}
	t := time.Unix(sec, 0).UTC()
	switch strings.ToLower(e.separator) {
	case "year":
		return protoreflect.ValueOfInt64(int64(t.Year()))
	case "month":
		return protoreflect.ValueOfInt64(int64(t.Month()))
	case "day":
		return protoreflect.ValueOfInt64(int64(t.Day()))
	case "hour":
		return protoreflect.ValueOfInt64(int64(t.Hour()))
	case "minute":
		return protoreflect.ValueOfInt64(int64(t.Minute()))
	case "second":
		return protoreflect.ValueOfInt64(int64(t.Second()))
	default:
		return protoreflect.Value{} // unknown part → null
	}
}

// ---- Bucket ----

// evalBucket floors the child value to the nearest multiple of e.intParam.
// result = value − value%size.
func evalBucket(e *funcExpr, leafValues [][]protoreflect.Value, branchIdx int) protoreflect.Value {
	v := e.kids[0].eval(leafValues, branchIdx)
	if !v.IsValid() {
		return protoreflect.Value{}
	}
	n, ok := toInt64(v)
	if !ok {
		return protoreflect.Value{}
	}
	size := int64(e.intParam)
	if size <= 0 {
		return v // no bucketing
	}
	bucketed := n - n%size
	return protoreflect.ValueOfInt64(bucketed)
}

// ---- Mask ----

// evalMask redacts the interior of a string value.
// Keeps keepFirst leading and keepLast trailing runes; fills the middle
// with the mask character (e.separator). keepFirst is stored in e.literal
// (as int64), keepLast in e.intParam.
func evalMask(e *funcExpr, leafValues [][]protoreflect.Value, branchIdx int) protoreflect.Value {
	v := e.kids[0].eval(leafValues, branchIdx)
	if !v.IsValid() {
		return protoreflect.Value{}
	}
	s := valueToString(v)
	runes := []rune(s)
	total := len(runes)

	keepFirst := int(e.literal.Int())
	keepLast := e.intParam
	if keepFirst < 0 {
		keepFirst = 0
	}
	if keepLast < 0 {
		keepLast = 0
	}
	if keepFirst+keepLast >= total {
		return protoreflect.ValueOfString(s) // nothing to mask
	}

	maskLen := total - keepFirst - keepLast
	maskRune := []rune(e.separator)
	if len(maskRune) == 0 {
		maskRune = []rune{'*'}
	}
	var b strings.Builder
	b.Grow(total)
	for i := 0; i < keepFirst; i++ {
		b.WriteRune(runes[i])
	}
	for i := 0; i < maskLen; i++ {
		b.WriteRune(maskRune[0])
	}
	for i := total - keepLast; i < total; i++ {
		b.WriteRune(runes[i])
	}
	return protoreflect.ValueOfString(b.String())
}

// ---- Coerce ----

// evalCoerce maps a boolean child to one of two literal values.
// Non-zero → e.literal (ifTrue); zero → e.literal2 (ifFalse).
func evalCoerce(e *funcExpr, leafValues [][]protoreflect.Value, branchIdx int) protoreflect.Value {
	v := e.kids[0].eval(leafValues, branchIdx)
	if isNonZero(v) {
		return e.literal
	}
	return e.literal2
}

// ---- EnumName ----

// evalEnumName maps an enum numeric value to its string name using the
// EnumDescriptor resolved at plan compile time and stored in e.enumDesc.
func evalEnumName(e *funcExpr, leafValues [][]protoreflect.Value, branchIdx int) protoreflect.Value {
	v := e.kids[0].eval(leafValues, branchIdx)
	if !v.IsValid() {
		return protoreflect.Value{}
	}
	if e.enumDesc == nil {
		return protoreflect.Value{} // not resolved — should not happen
	}
	var num protoreflect.EnumNumber
	switch iface := v.Interface().(type) {
	case protoreflect.EnumNumber:
		num = iface
	case int32:
		num = protoreflect.EnumNumber(iface)
	case int64:
		num = protoreflect.EnumNumber(iface)
	default:
		return protoreflect.Value{} // not an enum-compatible type
	}
	evd := e.enumDesc.Values().ByNumber(num)
	if evd == nil {
		return protoreflect.Value{} // unknown enum value
	}
	return protoreflect.ValueOfString(string(evd.Name()))
}

// ---- Sum (aggregate) ----

// evalSum sums all fan-out branches of the single child leaf.
// Returns the same total for every branchIdx.
func evalSum(e *funcExpr, leafValues [][]protoreflect.Value, _ int) protoreflect.Value {
	child := e.kids[0]
	// Collect all branches of the child.
	leaves := resolvePathExprs(child)
	if len(leaves) == 0 {
		return protoreflect.Value{}
	}
	// Determine total branches from the first leaf.
	vals := leafValues[leaves[0].entryIdx]
	if len(vals) == 0 {
		return protoreflect.Value{}
	}
	var sumI int64
	var sumF float64
	isFloat := false
	for bi := range len(vals) {
		v := child.eval(leafValues, bi)
		if !v.IsValid() {
			continue
		}
		n, okI := toInt64(v)
		if okI {
			if isFloat {
				sumF += float64(n)
			} else {
				sumI += n
			}
			continue
		}
		f, okF := toFloat64(v, 0, false)
		if okF {
			if !isFloat {
				isFloat = true
				sumF = float64(sumI)
			}
			sumF += f
		}
	}
	if isFloat {
		return protoreflect.ValueOfFloat64(sumF)
	}
	return protoreflect.ValueOfInt64(sumI)
}

// ---- Distinct (aggregate) ----

// evalDistinct counts distinct values across all fan-out branches.
// Returns the same count for every branchIdx.
func evalDistinct(e *funcExpr, leafValues [][]protoreflect.Value, _ int) protoreflect.Value {
	child := e.kids[0]
	leaves := resolvePathExprs(child)
	if len(leaves) == 0 {
		return protoreflect.ValueOfInt64(0)
	}
	vals := leafValues[leaves[0].entryIdx]
	seen := make(map[string]struct{}, len(vals))
	for bi := range len(vals) {
		v := child.eval(leafValues, bi)
		if !v.IsValid() {
			continue
		}
		seen[valueToString(v)] = struct{}{}
	}
	return protoreflect.ValueOfInt64(int64(len(seen)))
}

// ---- ListConcat (aggregate) ----

// evalListConcat joins the string representation of all fan-out branches
// with e.separator. Returns the same string for every branchIdx.
func evalListConcat(e *funcExpr, leafValues [][]protoreflect.Value, _ int) protoreflect.Value {
	child := e.kids[0]
	leaves := resolvePathExprs(child)
	if len(leaves) == 0 {
		return protoreflect.ValueOfString("")
	}
	vals := leafValues[leaves[0].entryIdx]
	parts := make([]string, 0, len(vals))
	for bi := range len(vals) {
		v := child.eval(leafValues, bi)
		if !v.IsValid() {
			continue
		}
		parts = append(parts, valueToString(v))
	}
	return protoreflect.ValueOfString(strings.Join(parts, e.separator))
}
