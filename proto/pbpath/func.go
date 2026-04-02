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
// current branch index.
func (e *funcExpr) eval(leafValues [][]Value, branchIdx int) Value {
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
	// Wave 4 — filter / logic functions
	case funcSelect:
		return evalSelect(e, leafValues, branchIdx)
	case funcAnd:
		return evalAnd(e, leafValues, branchIdx)
	case funcOr:
		return evalOr(e, leafValues, branchIdx)
	case funcNot:
		return evalNot(e, leafValues, branchIdx)
	default:
		return Value{} // unknown function → null
	}
}

// ---- Coalesce ----

// evalCoalesce returns the first child value that is non-zero.
// In proto3 semantics, an unset scalar field returns the default (e.g. 0 for
// int64) which is a valid protoreflect.Value. Coalesce skips zero values,
// treating them as "null".
func evalCoalesce(e *funcExpr, lv [][]Value, bi int) Value {
	for _, kid := range e.kids {
		v := kid.eval(lv, bi)
		if isNonZeroValue(v) {
			return v
		}
	}
	return Value{}
}

// ---- Default ----

// evalDefault returns the child value if non-zero, otherwise the literal.
// See [evalCoalesce] for the rationale on using isNonZeroValue.
func evalDefault(e *funcExpr, lv [][]Value, bi int) Value {
	v := e.kids[0].eval(lv, bi)
	if isNonZeroValue(v) {
		return v
	}
	return e.literal
}

// ---- Cond (if/then/else) ----

// evalCond evaluates predicate → if non-zero returns then-branch, else else-branch.
func evalCond(e *funcExpr, lv [][]Value, bi int) Value {
	pred := e.kids[0].eval(lv, bi)
	if isNonZeroValue(pred) {
		return e.kids[1].eval(lv, bi)
	}
	return e.kids[2].eval(lv, bi)
}

// ---- Has ----

// evalHas returns Bool true if the child evaluates to a valid, non-zero value.
func evalHas(e *funcExpr, lv [][]Value, bi int) Value {
	v := e.kids[0].eval(lv, bi)
	return ScalarBool(!v.IsNull() && isNonZeroValue(v))
}

// ---- Len ----

// evalLen returns the length of a repeated/map/string/bytes field as Int64.
// Scalars return 0, null values return 0.
func evalLen(e *funcExpr, lv [][]Value, bi int) Value {
	v := e.kids[0].eval(lv, bi)
	if v.IsNull() {
		return ScalarInt64(0)
	}
	// List-kind values have a direct length.
	if v.kind == ListKind {
		return ScalarInt64(int64(len(v.list)))
	}
	// For scalars wrapping proto List/Map/String/Bytes.
	if v.kind == ScalarKind {
		switch iface := v.scalar.Interface().(type) {
		case protoreflect.List:
			return ScalarInt64(int64(iface.Len()))
		case protoreflect.Map:
			return ScalarInt64(int64(iface.Len()))
		case string:
			return ScalarInt64(int64(len(iface)))
		case []byte:
			return ScalarInt64(int64(len(iface)))
		}
	}
	return ScalarInt64(0)
}

// ---- Arithmetic (Add, Sub, Mul, Div, Mod) ----

// evalArith handles all binary arithmetic operations. If either operand is
// null, the result is null.
//
// Type promotion rules (Go-style):
//   - Both int  → int result
//   - Mixed int/float → float result (int is promoted)
//   - Both float → float result
//   - Non-numeric → null
func evalArith(e *funcExpr, lv [][]Value, bi int, op funcKind) Value {
	a := e.kids[0].eval(lv, bi)
	b := e.kids[1].eval(lv, bi)
	if a.IsNull() || b.IsNull() {
		return Value{}
	}

	aInt, aIsInt := toInt64Value(a)
	bInt, bIsInt := toInt64Value(b)

	if aIsInt && bIsInt {
		return intArithV(aInt, bInt, op)
	}

	// At least one is float (or promote int to float).
	aFloat, aOk := toFloat64Value(a, aInt, aIsInt)
	bFloat, bOk := toFloat64Value(b, bInt, bIsInt)
	if !aOk || !bOk {
		return Value{} // non-numeric
	}
	return floatArithV(aFloat, bFloat, op)
}

// intArithV performs integer arithmetic and returns the result as a [Value].
func intArithV(a, b int64, op funcKind) Value {
	switch op {
	case funcAdd:
		return ScalarInt64(a + b)
	case funcSub:
		return ScalarInt64(a - b)
	case funcMul:
		return ScalarInt64(a * b)
	case funcDiv:
		if b == 0 {
			return ScalarInt64(0)
		}
		return ScalarInt64(a / b) // truncating
	case funcMod:
		if b == 0 {
			return ScalarInt64(0)
		}
		return ScalarInt64(a % b)
	default:
		return Value{}
	}
}

// floatArithV performs floating-point arithmetic and returns the result as a [Value].
func floatArithV(a, b float64, op funcKind) Value {
	switch op {
	case funcAdd:
		return ScalarFloat64(a + b)
	case funcSub:
		return ScalarFloat64(a - b)
	case funcMul:
		return ScalarFloat64(a * b)
	case funcDiv:
		if b == 0 {
			return ScalarFloat64(0)
		}
		return ScalarFloat64(a / b)
	case funcMod:
		if b == 0 {
			return ScalarFloat64(0)
		}
		return ScalarFloat64(math.Mod(a, b))
	default:
		return Value{}
	}
}

// ---- Concat ----

// evalConcat joins the string representations of all children with the
// separator. If any child is null, it is treated as an empty string.
func evalConcat(e *funcExpr, lv [][]Value, bi int) Value {
	var b strings.Builder
	for i, kid := range e.kids {
		if i > 0 && e.separator != "" {
			b.WriteString(e.separator)
		}
		v := kid.eval(lv, bi)
		if !v.IsNull() {
			b.WriteString(valueToStringValue(v))
		}
	}
	return ScalarString(b.String())
}

// ---- Helpers ----

// compareResult is the outcome of a type-aware comparison: -1, 0, or 1.
// cmpOk is false when the operands are non-comparable (e.g. different
// non-numeric/non-string types).
func compareValuesV(a, b Value) (cmp int, ok bool) {
	if a.IsNull() || b.IsNull() {
		return 0, false
	}
	// Both must be scalars for comparison.
	ai := scalarInterface(a)
	bi := scalarInterface(b)
	if ai == nil || bi == nil {
		return 0, false
	}

	// string vs string
	aStr, aIsStr := ai.(string)
	bStr, bIsStr := bi.(string)
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
	aBool, aIsBool := ai.(bool)
	bBool, bIsBool := bi.(bool)
	if aIsBool && bIsBool {
		aI, bI := int64(0), int64(0)
		if aBool {
			aI = 1
		}
		if bBool {
			bI = 1
		}
		switch {
		case aI < bI:
			return -1, true
		case aI > bI:
			return 1, true
		default:
			return 0, true
		}
	}

	// numeric comparison with int→float promotion
	aInt, aIsInt := toInt64Value(a)
	bInt, bIsInt := toInt64Value(b)

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

	aFloat, aOk := toFloat64Value(a, aInt, aIsInt)
	bFloat, bOk := toFloat64Value(b, bInt, bIsInt)
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
// null or the types are non-comparable, returns false.
func evalCompare(e *funcExpr, lv [][]Value, bi int) Value {
	a := e.kids[0].eval(lv, bi)
	b := e.kids[1].eval(lv, bi)

	cmp, ok := compareValuesV(a, b)
	if !ok {
		return ScalarBool(false)
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
	return ScalarBool(result)
}

// ---- String functions ----

// evalStringUnary applies a unary string→string function.
// If the child is not a string, converts via valueToStringValue first.
func evalStringUnary(e *funcExpr, lv [][]Value, bi int, fn func(string) string) Value {
	v := e.kids[0].eval(lv, bi)
	if v.IsNull() {
		return ScalarString("")
	}
	s, ok := scalarInterface(v).(string)
	if !ok {
		s = valueToStringValue(v)
	}
	return ScalarString(fn(s))
}

// evalTrimFix applies TrimPrefix or TrimSuffix. The fix string is stored
// in funcExpr.separator.
func evalTrimFix(e *funcExpr, lv [][]Value, bi int, fn func(string, string) string) Value {
	v := e.kids[0].eval(lv, bi)
	if v.IsNull() {
		return ScalarString("")
	}
	s, ok := scalarInterface(v).(string)
	if !ok {
		s = valueToStringValue(v)
	}
	return ScalarString(fn(s, e.separator))
}

// ---- Math functions (unary) ----

// evalMathUnary applies Abs, Ceil, Floor, or Round. Preserves int vs float kind.
func evalMathUnary(e *funcExpr, lv [][]Value, bi int, op funcKind) Value {
	v := e.kids[0].eval(lv, bi)
	if v.IsNull() {
		return Value{}
	}

	if i, ok := toInt64Value(v); ok {
		switch op {
		case funcAbs:
			if i < 0 {
				i = -i
			}
			return ScalarInt64(i)
		case funcCeil, funcFloor, funcRound:
			// No-op for integers.
			return ScalarInt64(i)
		}
	}

	iface := scalarInterface(v)
	switch val := iface.(type) {
	case float32:
		f := float64(val)
		return ScalarFloat64(applyMathFloat(f, op))
	case float64:
		return ScalarFloat64(applyMathFloat(val, op))
	default:
		return Value{} // non-numeric
	}
}

// applyMathFloat applies a math function to a float64 value.
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
func evalMinMax(e *funcExpr, lv [][]Value, bi int, isMin bool) Value {
	a := e.kids[0].eval(lv, bi)
	b := e.kids[1].eval(lv, bi)
	if a.IsNull() || b.IsNull() {
		return Value{}
	}

	aInt, aIsInt := toInt64Value(a)
	bInt, bIsInt := toInt64Value(b)

	if aIsInt && bIsInt {
		if isMin {
			if aInt < bInt {
				return ScalarInt64(aInt)
			}
			return ScalarInt64(bInt)
		}
		if aInt > bInt {
			return ScalarInt64(aInt)
		}
		return ScalarInt64(bInt)
	}

	aFloat, aOk := toFloat64Value(a, aInt, aIsInt)
	bFloat, bOk := toFloat64Value(b, bInt, bIsInt)
	if !aOk || !bOk {
		return Value{}
	}
	if isMin {
		return ScalarFloat64(math.Min(aFloat, bFloat))
	}
	return ScalarFloat64(math.Max(aFloat, bFloat))
}

// ---- Cast functions ----

// evalCastInt casts the child value to Int64.
func evalCastInt(e *funcExpr, lv [][]Value, bi int) Value {
	v := e.kids[0].eval(lv, bi)
	if v.IsNull() {
		return Value{}
	}
	// Already int?
	if i, ok := toInt64Value(v); ok {
		return ScalarInt64(i)
	}
	iface := scalarInterface(v)
	switch val := iface.(type) {
	case float32:
		return ScalarInt64(int64(val))
	case float64:
		return ScalarInt64(int64(val))
	case string:
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			// Try float parse then truncate.
			f, err2 := strconv.ParseFloat(val, 64)
			if err2 != nil {
				return Value{} // unparseable
			}
			return ScalarInt64(int64(f))
		}
		return ScalarInt64(n)
	default:
		return Value{}
	}
}

// evalCastFloat casts the child value to Float64 (DoubleKind).
func evalCastFloat(e *funcExpr, lv [][]Value, bi int) Value {
	v := e.kids[0].eval(lv, bi)
	if v.IsNull() {
		return Value{}
	}
	if i, ok := toInt64Value(v); ok {
		return ScalarFloat64(float64(i))
	}
	iface := scalarInterface(v)
	switch val := iface.(type) {
	case float32:
		return ScalarFloat64(float64(val))
	case float64:
		return ScalarFloat64(val)
	case string:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return Value{} // unparseable
		}
		return ScalarFloat64(f)
	default:
		return Value{}
	}
}

// evalCastString casts the child value to String.
func evalCastString(e *funcExpr, lv [][]Value, bi int) Value {
	v := e.kids[0].eval(lv, bi)
	if v.IsNull() {
		return ScalarString("")
	}
	return ScalarString(valueToStringValue(v))
}

// ---- Timestamp functions ----

// evalStrptime parses a string child into a Unix-millisecond Int64 timestamp.
// When tryMode is true, parse failures return zero (epoch) instead of null.
func evalStrptime(e *funcExpr, lv [][]Value, bi int, tryMode bool) Value {
	v := e.kids[0].eval(lv, bi)
	if v.IsNull() {
		if tryMode {
			return ScalarInt64(0)
		}
		return Value{}
	}
	s, ok := scalarInterface(v).(string)
	if !ok {
		s = valueToStringValue(v)
	}

	t, err := ParseStrptime(e.separator, s)
	if err != nil {
		if tryMode {
			return ScalarInt64(0)
		}
		return Value{}
	}
	return ScalarInt64(t.UnixMilli())
}

// evalAge computes the duration between timestamps in milliseconds.
// 1 child: now − child. 2 children: child[0] − child[1].
func evalAge(e *funcExpr, lv [][]Value, bi int) Value {
	switch len(e.kids) {
	case 1:
		v := e.kids[0].eval(lv, bi)
		if v.IsNull() {
			return Value{}
		}
		ts, ok := toInt64Value(v)
		if !ok {
			return Value{}
		}
		nowMs := time.Now().UnixMilli()
		return ScalarInt64(nowMs - ts)
	case 2:
		a := e.kids[0].eval(lv, bi)
		b := e.kids[1].eval(lv, bi)
		if a.IsNull() || b.IsNull() {
			return Value{}
		}
		aTs, aOk := toInt64Value(a)
		bTs, bOk := toInt64Value(b)
		if !aOk || !bOk {
			return Value{}
		}
		return ScalarInt64(aTs - bTs)
	default:
		return Value{}
	}
}

// evalExtract extracts a date/time component from a Unix-millisecond timestamp.
func evalExtract(e *funcExpr, lv [][]Value, bi int) Value {
	v := e.kids[0].eval(lv, bi)
	if v.IsNull() {
		return Value{}
	}
	ms, ok := toInt64Value(v)
	if !ok {
		return Value{}
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
	return ScalarInt64(result)
}

// ---- Shared helpers ----

// isNonZero returns true when v is valid and not the protobuf zero value
// for its kind. This operates on raw [protoreflect.Value] and is used by
// the [Value]-aware wrapper [isNonZeroValue] and legacy code paths.
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

// toInt64 attempts to extract an integer value from a [protoreflect.Value].
// Returns (val, true) for int32, int64, uint32, uint64, enum, and bool.
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

// toFloat64 attempts to extract or promote a [protoreflect.Value] to float64.
// If the value was already parsed as int64, it promotes from that. Otherwise
// tries the float interface types.
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

// valueToString converts a [protoreflect.Value] to its string representation.
// Used by [valueToStringValue] and legacy code paths.
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
func evalHash(e *funcExpr, leafValues [][]Value, branchIdx int) Value {
	h := fnv.New64a()
	for _, kid := range e.kids {
		v := kid.eval(leafValues, branchIdx)
		if !v.IsNull() {
			_, _ = h.Write([]byte(valueToStringValue(v)))
		}
	}
	return ScalarInt64(int64(h.Sum64()))
}

// ---- EpochToDate ----

// evalEpochToDate converts a Unix-epoch-second value to a day offset
// (seconds / 86400) as Int32.
func evalEpochToDate(e *funcExpr, leafValues [][]Value, branchIdx int) Value {
	v := e.kids[0].eval(leafValues, branchIdx)
	if v.IsNull() {
		return Value{}
	}
	sec, ok := toInt64Value(v)
	if !ok {
		return Value{}
	}
	return ScalarInt32(int32(sec / 86400))
}

// ---- DatePart ----

// evalDatePart extracts a calendar component from a Unix-epoch-second value.
// The component name is in e.separator.
func evalDatePart(e *funcExpr, leafValues [][]Value, branchIdx int) Value {
	v := e.kids[0].eval(leafValues, branchIdx)
	if v.IsNull() {
		return Value{}
	}
	sec, ok := toInt64Value(v)
	if !ok {
		return Value{}
	}
	t := time.Unix(sec, 0).UTC()
	switch strings.ToLower(e.separator) {
	case "year":
		return ScalarInt64(int64(t.Year()))
	case "month":
		return ScalarInt64(int64(t.Month()))
	case "day":
		return ScalarInt64(int64(t.Day()))
	case "hour":
		return ScalarInt64(int64(t.Hour()))
	case "minute":
		return ScalarInt64(int64(t.Minute()))
	case "second":
		return ScalarInt64(int64(t.Second()))
	default:
		return Value{} // unknown part → null
	}
}

// ---- Bucket ----

// evalBucket floors the child value to the nearest multiple of e.intParam.
// result = value − value%size.
func evalBucket(e *funcExpr, leafValues [][]Value, branchIdx int) Value {
	v := e.kids[0].eval(leafValues, branchIdx)
	if v.IsNull() {
		return Value{}
	}
	n, ok := toInt64Value(v)
	if !ok {
		return Value{}
	}
	size := int64(e.intParam)
	if size <= 0 {
		return v // no bucketing
	}
	bucketed := n - n%size
	return ScalarInt64(bucketed)
}

// ---- Mask ----

// evalMask redacts the interior of a string value.
// Keeps keepFirst leading and keepLast trailing runes; fills the middle
// with the mask character (e.separator). keepFirst is stored in e.literal
// (as int64), keepLast in e.intParam.
func evalMask(e *funcExpr, leafValues [][]Value, branchIdx int) Value {
	v := e.kids[0].eval(leafValues, branchIdx)
	if v.IsNull() {
		return Value{}
	}
	s := valueToStringValue(v)
	runes := []rune(s)
	total := len(runes)

	// Extract keepFirst from the literal (stored as ScalarInt64).
	keepFirst := 0
	if kf, ok := toInt64Value(e.literal); ok {
		keepFirst = int(kf)
	}
	keepLast := e.intParam
	if keepFirst < 0 {
		keepFirst = 0
	}
	if keepLast < 0 {
		keepLast = 0
	}
	if keepFirst+keepLast >= total {
		return ScalarString(s) // nothing to mask
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
	return ScalarString(b.String())
}

// ---- Coerce ----

// evalCoerce maps a boolean child to one of two literal values.
// Non-zero → e.literal (ifTrue); zero → e.literal2 (ifFalse).
func evalCoerce(e *funcExpr, leafValues [][]Value, branchIdx int) Value {
	v := e.kids[0].eval(leafValues, branchIdx)
	if isNonZeroValue(v) {
		return e.literal
	}
	return e.literal2
}

// ---- EnumName ----

// evalEnumName maps an enum numeric value to its string name using the
// EnumDescriptor resolved at plan compile time and stored in e.enumDesc.
func evalEnumName(e *funcExpr, leafValues [][]Value, branchIdx int) Value {
	v := e.kids[0].eval(leafValues, branchIdx)
	if v.IsNull() {
		return Value{}
	}
	if e.enumDesc == nil {
		return Value{} // not resolved — should not happen
	}
	iface := scalarInterface(v)
	var num protoreflect.EnumNumber
	switch val := iface.(type) {
	case protoreflect.EnumNumber:
		num = val
	case int32:
		num = protoreflect.EnumNumber(val)
	case int64:
		num = protoreflect.EnumNumber(val)
	default:
		return Value{} // not an enum-compatible type
	}
	evd := e.enumDesc.Values().ByNumber(num)
	if evd == nil {
		return Value{} // unknown enum value
	}
	return ScalarString(string(evd.Name()))
}

// ---- Sum (aggregate) ----

// evalSum sums all fan-out branches of the single child leaf.
// Returns the same total for every branchIdx.
func evalSum(e *funcExpr, leafValues [][]Value, _ int) Value {
	child := e.kids[0]
	// Collect all branches of the child.
	leaves := resolvePathExprs(child)
	if len(leaves) == 0 {
		return Value{}
	}
	// Determine total branches from the first leaf.
	vals := leafValues[leaves[0].entryIdx]
	if len(vals) == 0 {
		return Value{}
	}
	var sumI int64
	var sumF float64
	isFloat := false
	for bi := range len(vals) {
		v := child.eval(leafValues, bi)
		if v.IsNull() {
			continue
		}
		n, okI := toInt64Value(v)
		if okI {
			if isFloat {
				sumF += float64(n)
			} else {
				sumI += n
			}
			continue
		}
		f, okF := toFloat64Value(v, 0, false)
		if okF {
			if !isFloat {
				isFloat = true
				sumF = float64(sumI)
			}
			sumF += f
		}
	}
	if isFloat {
		return ScalarFloat64(sumF)
	}
	return ScalarInt64(sumI)
}

// ---- Distinct (aggregate) ----

// evalDistinct counts distinct values across all fan-out branches.
// Returns the same count for every branchIdx.
func evalDistinct(e *funcExpr, leafValues [][]Value, _ int) Value {
	child := e.kids[0]
	leaves := resolvePathExprs(child)
	if len(leaves) == 0 {
		return ScalarInt64(0)
	}
	vals := leafValues[leaves[0].entryIdx]
	seen := make(map[string]struct{}, len(vals))
	for bi := range len(vals) {
		v := child.eval(leafValues, bi)
		if v.IsNull() {
			continue
		}
		seen[valueToStringValue(v)] = struct{}{}
	}
	return ScalarInt64(int64(len(seen)))
}

// ---- ListConcat (aggregate) ----

// evalListConcat joins the string representation of all fan-out branches
// with e.separator. Returns the same string for every branchIdx.
func evalListConcat(e *funcExpr, leafValues [][]Value, _ int) Value {
	child := e.kids[0]
	leaves := resolvePathExprs(child)
	if len(leaves) == 0 {
		return ScalarString("")
	}
	vals := leafValues[leaves[0].entryIdx]
	parts := make([]string, 0, len(vals))
	for bi := range len(vals) {
		v := child.eval(leafValues, bi)
		if v.IsNull() {
			continue
		}
		parts = append(parts, valueToStringValue(v))
	}
	return ScalarString(strings.Join(parts, e.separator))
}

// ---- Wave 4: Select (filter pass-through) ----

// evalSelect evaluates the predicate (child 0) and, if truthy, returns the
// value of the input (child 1). If the predicate is falsy (null, false, zero,
// empty), returns null — which downstream consumers interpret as "filtered out".
//
// This implements jq's `select(pred)` semantics: the value is passed through
// unchanged when the predicate holds, and dropped (null) when it does not.
func evalSelect(e *funcExpr, leafValues [][]Value, branchIdx int) Value {
	pred := e.kids[0].eval(leafValues, branchIdx)
	if !isNonZeroValue(pred) {
		return Null()
	}
	return e.kids[1].eval(leafValues, branchIdx)
}

// ---- Wave 4: Logical combinators ----

// evalAnd returns true (BoolKind) when both children are truthy.
func evalAnd(e *funcExpr, leafValues [][]Value, branchIdx int) Value {
	a := e.kids[0].eval(leafValues, branchIdx)
	if !isNonZeroValue(a) {
		return ScalarBool(false)
	}
	b := e.kids[1].eval(leafValues, branchIdx)
	return ScalarBool(isNonZeroValue(b))
}

// evalOr returns true (BoolKind) when at least one child is truthy.
func evalOr(e *funcExpr, leafValues [][]Value, branchIdx int) Value {
	a := e.kids[0].eval(leafValues, branchIdx)
	if isNonZeroValue(a) {
		return ScalarBool(true)
	}
	b := e.kids[1].eval(leafValues, branchIdx)
	return ScalarBool(isNonZeroValue(b))
}

// evalNot returns true (BoolKind) when the child is falsy (null, false,
// zero, empty string, empty list).
func evalNot(e *funcExpr, leafValues [][]Value, branchIdx int) Value {
	v := e.kids[0].eval(leafValues, branchIdx)
	return ScalarBool(!isNonZeroValue(v))
}
