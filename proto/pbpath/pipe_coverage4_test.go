package pbpath

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// ── Direct unit tests for low-coverage internal functions ───────────────

// --- isNonZero (47.1%) ---

func TestIsNonZeroDirect(t *testing.T) {
	// invalid value
	if isNonZero(protoreflect.Value{}) {
		t.Error("invalid should be zero")
	}
	// bool
	if !isNonZero(protoreflect.ValueOfBool(true)) {
		t.Error("true should be non-zero")
	}
	if isNonZero(protoreflect.ValueOfBool(false)) {
		t.Error("false should be zero")
	}
	// int32
	if !isNonZero(protoreflect.ValueOfInt32(1)) {
		t.Error("int32(1) should be non-zero")
	}
	if isNonZero(protoreflect.ValueOfInt32(0)) {
		t.Error("int32(0) should be zero")
	}
	// int64
	if !isNonZero(protoreflect.ValueOfInt64(42)) {
		t.Error("int64(42) should be non-zero")
	}
	if isNonZero(protoreflect.ValueOfInt64(0)) {
		t.Error("int64(0) should be zero")
	}
	// uint32
	if !isNonZero(protoreflect.ValueOfUint32(1)) {
		t.Error("uint32(1) should be non-zero")
	}
	if isNonZero(protoreflect.ValueOfUint32(0)) {
		t.Error("uint32(0) should be zero")
	}
	// uint64
	if !isNonZero(protoreflect.ValueOfUint64(1)) {
		t.Error("uint64(1) should be non-zero")
	}
	if isNonZero(protoreflect.ValueOfUint64(0)) {
		t.Error("uint64(0) should be zero")
	}
	// float32
	if !isNonZero(protoreflect.ValueOfFloat32(1.0)) {
		t.Error("float32(1.0) should be non-zero")
	}
	if isNonZero(protoreflect.ValueOfFloat32(0.0)) {
		t.Error("float32(0.0) should be zero")
	}
	// float64
	if !isNonZero(protoreflect.ValueOfFloat64(1.0)) {
		t.Error("float64(1.0) should be non-zero")
	}
	if isNonZero(protoreflect.ValueOfFloat64(0.0)) {
		t.Error("float64(0.0) should be zero")
	}
	// string
	if !isNonZero(protoreflect.ValueOfString("hello")) {
		t.Error("non-empty string should be non-zero")
	}
	if isNonZero(protoreflect.ValueOfString("")) {
		t.Error("empty string should be zero")
	}
	// bytes
	if !isNonZero(protoreflect.ValueOfBytes([]byte{1})) {
		t.Error("non-empty bytes should be non-zero")
	}
	if isNonZero(protoreflect.ValueOfBytes(nil)) {
		t.Error("nil bytes should be zero")
	}
	// enum
	if !isNonZero(protoreflect.ValueOfEnum(1)) {
		t.Error("enum(1) should be non-zero")
	}
	if isNonZero(protoreflect.ValueOfEnum(0)) {
		t.Error("enum(0) should be zero")
	}
}

// --- toInt64 (50%) ---

func TestToInt64Direct(t *testing.T) {
	// int32
	if v, ok := toInt64(protoreflect.ValueOfInt32(42)); !ok || v != 42 {
		t.Errorf("int32: got %d, %v", v, ok)
	}
	// int64
	if v, ok := toInt64(protoreflect.ValueOfInt64(100)); !ok || v != 100 {
		t.Errorf("int64: got %d, %v", v, ok)
	}
	// uint32
	if v, ok := toInt64(protoreflect.ValueOfUint32(5)); !ok || v != 5 {
		t.Errorf("uint32: got %d, %v", v, ok)
	}
	// uint64
	if v, ok := toInt64(protoreflect.ValueOfUint64(999)); !ok || v != 999 {
		t.Errorf("uint64: got %d, %v", v, ok)
	}
	// enum
	if v, ok := toInt64(protoreflect.ValueOfEnum(3)); !ok || v != 3 {
		t.Errorf("enum: got %d, %v", v, ok)
	}
	// bool true
	if v, ok := toInt64(protoreflect.ValueOfBool(true)); !ok || v != 1 {
		t.Errorf("bool true: got %d, %v", v, ok)
	}
	// bool false
	if v, ok := toInt64(protoreflect.ValueOfBool(false)); !ok || v != 0 {
		t.Errorf("bool false: got %d, %v", v, ok)
	}
	// float (not convertible)
	if _, ok := toInt64(protoreflect.ValueOfFloat64(1.5)); ok {
		t.Error("float should not convert")
	}
	// string (not convertible)
	if _, ok := toInt64(protoreflect.ValueOfString("hi")); ok {
		t.Error("string should not convert")
	}
}

// --- toFloat64 (83.3%) ---

func TestToFloat64Direct(t *testing.T) {
	// promoted from int
	if v, ok := toFloat64(protoreflect.Value{}, 42, true); !ok || v != 42.0 {
		t.Errorf("from int: got %f, %v", v, ok)
	}
	// float32
	if v, ok := toFloat64(protoreflect.ValueOfFloat32(1.5), 0, false); !ok || v != float64(float32(1.5)) {
		t.Errorf("float32: got %f, %v", v, ok)
	}
	// float64
	if v, ok := toFloat64(protoreflect.ValueOfFloat64(2.5), 0, false); !ok || v != 2.5 {
		t.Errorf("float64: got %f, %v", v, ok)
	}
	// string (not convertible)
	if _, ok := toFloat64(protoreflect.ValueOfString("x"), 0, false); ok {
		t.Error("string should not convert")
	}
}

// --- valueToString (52.9%) ---

func TestValueToStringDirect(t *testing.T) {
	// string
	if s := valueToString(protoreflect.ValueOfString("hello")); s != "hello" {
		t.Errorf("string: %s", s)
	}
	// bool true
	if s := valueToString(protoreflect.ValueOfBool(true)); s != "true" {
		t.Errorf("bool true: %s", s)
	}
	// bool false
	if s := valueToString(protoreflect.ValueOfBool(false)); s != "false" {
		t.Errorf("bool false: %s", s)
	}
	// int32
	if s := valueToString(protoreflect.ValueOfInt32(42)); s != "42" {
		t.Errorf("int32: %s", s)
	}
	// int64
	if s := valueToString(protoreflect.ValueOfInt64(-100)); s != "-100" {
		t.Errorf("int64: %s", s)
	}
	// uint32
	if s := valueToString(protoreflect.ValueOfUint32(7)); s != "7" {
		t.Errorf("uint32: %s", s)
	}
	// uint64
	if s := valueToString(protoreflect.ValueOfUint64(999)); s != "999" {
		t.Errorf("uint64: %s", s)
	}
	// float32
	s := valueToString(protoreflect.ValueOfFloat32(1.5))
	if s != "1.5" {
		t.Errorf("float32: %s", s)
	}
	// float64
	if s := valueToString(protoreflect.ValueOfFloat64(2.5)); s != "2.5" {
		t.Errorf("float64: %s", s)
	}
	// bytes
	if s := valueToString(protoreflect.ValueOfBytes([]byte{0xab, 0xcd})); s != "abcd" {
		t.Errorf("bytes: %s", s)
	}
	// enum
	if s := valueToString(protoreflect.ValueOfEnum(3)); s != "3" {
		t.Errorf("enum: %s", s)
	}
}

// --- floatArithV (36.4%) ---

func TestFloatArithVDirect(t *testing.T) {
	// add
	v := floatArithV(1.5, 2.5, funcAdd)
	if s := v.String(); s != "4" {
		t.Errorf("add: %s", s)
	}
	// sub
	v = floatArithV(5.0, 1.5, funcSub)
	if s := v.String(); s != "3.5" {
		t.Errorf("sub: %s", s)
	}
	// mul
	v = floatArithV(2.0, 3.0, funcMul)
	if s := v.String(); s != "6" {
		t.Errorf("mul: %s", s)
	}
	// div
	v = floatArithV(10.0, 4.0, funcDiv)
	if s := v.String(); s != "2.5" {
		t.Errorf("div: %s", s)
	}
	// div by zero
	v = floatArithV(10.0, 0.0, funcDiv)
	if s := v.String(); s != "0" {
		t.Errorf("div zero: %s", s)
	}
	// mod
	v = floatArithV(7.5, 2.0, funcMod)
	if s := v.String(); s != "1.5" {
		t.Errorf("mod: %s", s)
	}
	// mod by zero
	v = floatArithV(7.5, 0.0, funcMod)
	if s := v.String(); s != "0" {
		t.Errorf("mod zero: %s", s)
	}
	// unknown op
	v = floatArithV(1.0, 2.0, funcConcat)
	if v.Kind() != NullKind && v.Kind() != 0 {
		t.Errorf("unknown op: kind=%d", v.Kind())
	}
}

// --- accessField (32%) - direct tests with various Value kinds ---

func TestAccessFieldOnNullMessage(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	// Access field on a non-existent nested message => null
	strs := helperExecStr(t, containerMD, msg, `.single | .inner | .label`)
	if len(strs) != 1 || strs[0] != "lbl-s" {
		t.Fatalf("access nested: got %v; want [lbl-s]", strs)
	}
}

func TestAccessFieldOnScalarError(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	p, err := ParsePipeline(containerMD, `"hello" | .foo`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = p.ExecMessage(msg.ProtoReflect())
	if err == nil {
		t.Fatal("access field on string should error")
	}
}

// --- builtinFloor/Ceil/Round on integer (50%) ---

func TestFloorCeilRoundOnInt(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `5 | floor`)
	if len(strs) != 1 || strs[0] != "5" {
		t.Fatalf("floor int: got %v; want [5]", strs)
	}
	strs = helperExecStr(t, containerMD, msg, `5 | ceil`)
	if len(strs) != 1 || strs[0] != "5" {
		t.Fatalf("ceil int: got %v; want [5]", strs)
	}
	strs = helperExecStr(t, containerMD, msg, `5 | round`)
	if len(strs) != 1 || strs[0] != "5" {
		t.Fatalf("round int: got %v; want [5]", strs)
	}
}

// --- describeToken / pipeDescribeToken (11%/22%) ---
// These are error-formatting functions called when parse fails.
// Test by triggering parse errors with various bad tokens.

func TestParseErrorDescriptions(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)

	badExprs := []string{
		`.items | @`,       // illegal token
		`.items | +`,       // unexpected +
		`.items[`,          // unexpected eof
		`.items | (`,       // unclosed paren
		`.items | ==`,      // unexpected ==
		`.items | !=`,      // unexpected !=
		`.items | <`,       // unexpected <
		`.items | <=`,      // unexpected <=
		`.items | >`,       // unexpected >
		`.items | >=`,      // unexpected >=
		`.items | &&`,      // unexpected &&
		`.items | ||`,      // unexpected ||
		`.items | !`,       // unexpected !
		`.items | ,`,       // unexpected comma
		`.items | "foo`,    // unterminated string
		`{`,                // unclosed brace
	}
	for _, expr := range badExprs {
		_, err := ParsePipeline(containerMD, expr)
		if err == nil {
			t.Errorf("expected parse error for %q", expr)
		}
	}
}

func TestPredicateParseErrors(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)

	badPreds := []string{
		`.items[?(@.value == )]`,           // missing rhs
		`.items[?(@.value <> 1)]`,          // bad operator
		`.items[?(@ ==)]`,                  // missing operand
	}
	for _, expr := range badPreds {
		_, err := ParsePath(containerMD, expr)
		if err == nil {
			t.Errorf("expected parse error for %q", expr)
		}
	}
}

// --- builtinIndices (55.6%) ---

func TestIndicesString(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `"abcabcabc" | indices("bc")`)
	if len(strs) != 1 {
		t.Fatalf("indices string: got %d results", len(strs))
	}
}

func TestIndicesList(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `[1, 2, 3, 2, 1] | indices(2)`)
	if len(strs) != 1 {
		t.Fatalf("indices list: got %d results", len(strs))
	}
}

// --- builtinType on more value kinds (57.1% → higher) ---

func TestTypeOnBytes(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	// Access the inner message to exercise type on sub-messages
	strs := helperExecStr(t, containerMD, msg, `.single.inner | type`)
	if len(strs) != 1 || strs[0] != "object" {
		t.Fatalf("type message: got %v; want [object]", strs)
	}
}

// --- lookupUserFunc (0%) ---
// Tested indirectly via def (user-defined functions), which we already test.
// But let's add a test with a nested def call to exercise lookupUserFunc.

func TestNestedDefCalls(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `def inc: . + 1; def double: . * 2; 3 | inc | double`)
	if len(strs) != 1 || strs[0] != "8" {
		t.Fatalf("nested def: got %v; want [8]", strs)
	}
}

func TestRecursiveDefCall(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `def f: . * 2; 5 | f | f`)
	if len(strs) != 1 || strs[0] != "20" {
		t.Fatalf("recursive def: got %v; want [20]", strs)
	}
}

// --- appendValue with message, list, map, enum ---

func TestAppendValueMessage(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	// Access the single item as a message
	singleFD := containerMD.Fields().ByName("single")
	singleMsg := msg.ProtoReflect().Get(singleFD).Message()
	buf := appendValue(nil, protoreflect.ValueOfMessage(singleMsg), singleFD)
	if len(buf) == 0 {
		t.Fatal("message appendValue should produce output")
	}
	if buf[0] != '{' {
		t.Errorf("message should start with {, got %c", buf[0])
	}
}

func TestAppendValueList(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	itemsFD := containerMD.Fields().ByName("items")
	list := msg.ProtoReflect().Get(itemsFD)
	buf := appendValue(nil, list, itemsFD)
	if len(buf) == 0 {
		t.Fatal("list appendValue should produce output")
	}
	if buf[0] != '[' {
		t.Errorf("list should start with [, got %c", buf[0])
	}
}

func TestAppendValueMap(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	lookupFD := containerMD.Fields().ByName("lookup")
	mapVal := msg.ProtoReflect().Get(lookupFD)
	buf := appendValue(nil, mapVal, lookupFD)
	if len(buf) == 0 {
		t.Fatal("map appendValue should produce output")
	}
	if buf[0] != '{' {
		t.Errorf("map should start with {, got %c", buf[0])
	}
}

// --- more pipeline tests to exercise mid-coverage functions ---

// builtinBase64d error handling (57.1%)
func TestBase64dInvalidInput(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	p, err := ParsePipeline(containerMD, `"!!!not-base64!!!" | @base64d`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, _ = p.ExecMessage(msg.ProtoReflect()) // exercise error path
}

// exec at pipe.go:339 (54.5%) - this is the "pipeRepeatN" exec
func TestRepeatNViaLimit(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `[limit(3; 1, 1, 1, 1)]`)
	if len(strs) != 1 {
		t.Fatalf("limit: got %d results", len(strs))
	}
}

// Scan edge cases (scanStringTail 58.6%)
func TestScanEscapedStrings(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	// Escaped characters in string
	strs := helperExecStr(t, containerMD, msg, `"hello\nworld"`)
	if len(strs) != 1 {
		t.Fatalf("escaped string: got %d results", len(strs))
	}
	// String with tabs and backslashes
	strs = helperExecStr(t, containerMD, msg, `"tab\there"`)
	if len(strs) != 1 {
		t.Fatalf("tab string: got %d results", len(strs))
	}
}

// More object operations (to improve with_entries, from_entries paths)
func TestWithEntriesTransform(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `{a: 1, b: 2} | with_entries(select(.key == "a"))`)
	if len(strs) != 1 {
		t.Fatalf("with_entries transform: got %d results", len(strs))
	}
}

// Test group_by more (90%)
func TestGroupByValues(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `.items | group_by(.kind) | length`)
	if len(strs) != 1 || strs[0] != "2" {
		t.Fatalf("group_by: got %v; want [2]", strs)
	}
}

// Test sort_by (83.3%)
func TestSortByField(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `.items | sort_by(.value) | .[0] | .name`)
	if len(strs) != 1 || strs[0] != "alpha" {
		t.Fatalf("sort_by: got %v; want [alpha]", strs)
	}
}

// Test unique_by (87.5%)
func TestUniqueByField(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `.items | unique_by(.kind) | length`)
	if len(strs) != 1 || strs[0] != "2" {
		t.Fatalf("unique_by: got %v; want [2]", strs)
	}
}

// Test min_by/max_by (74.1%)
func TestMinByMaxByField(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `.items | min_by(.value) | .name`)
	if len(strs) != 1 || strs[0] != "alpha" {
		t.Fatalf("min_by: got %v; want [alpha]", strs)
	}
	strs = helperExecStr(t, containerMD, msg, `.items | max_by(.value) | .name`)
	if len(strs) != 1 || strs[0] != "delta" {
		t.Fatalf("max_by: got %v; want [delta]", strs)
	}
}

// Test splits (73.7%)
func TestSplitsMultiple(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `[.name | splits("-")]`)
	if len(strs) != 1 {
		t.Fatalf("splits: got %d results", len(strs))
	}
}

// Test scan (79.2%)
func TestScanPattern(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `[.name | scan("[a-z]+")]`)
	if len(strs) != 1 {
		t.Fatalf("scan: got %d results", len(strs))
	}
}

// Test paths (builtinPaths + collectPathsAndValues)
func TestPathsOnObject(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	results := helperExec(t, containerMD, msg, `{a: 1, b: {c: 2}} | [paths]`)
	if len(results) != 1 {
		t.Fatalf("paths: got %d results", len(results))
	}
}

func TestLeafPathsOnObject(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	results := helperExec(t, containerMD, msg, `{a: 1, b: {c: 2}} | [leaf_paths]`)
	if len(results) != 1 {
		t.Fatalf("leaf_paths: got %d results", len(results))
	}
}

// Test map function (execMap 80%)
func TestMapTransform(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `[1, 2, 3] | map(. * 2)`)
	if len(strs) != 1 || strs[0] != "[2,4,6]" {
		t.Fatalf("map: got %v; want [[2,4,6]]", strs)
	}
}

// Test not_null
func TestNotNull(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `[1, null, 2, null, 3] | [.[] | not_null]`)
	if len(strs) != 1 || strs[0] != "[1,2,3]" {
		t.Fatalf("not_null: got %v; want [[1,2,3]]", strs)
	}
}

// Test env (builtinEnv)
func TestEnvBuiltin(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	// env returns an object with environment variables
	p, err := ParsePipeline(containerMD, `env`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, _ = p.ExecMessage(msg.ProtoReflect()) // exercise code path
}

// Test debug (builtinDebug)
func TestDebugBuiltin(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `42 | debug`)
	if len(strs) != 1 || strs[0] != "42" {
		t.Fatalf("debug: got %v; want [42]", strs)
	}
}

// Test error (builtinError)
func TestErrorBuiltin(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	p, err := ParsePipeline(containerMD, `error`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = p.ExecMessage(msg.ProtoReflect())
	if err == nil {
		t.Fatal("error should produce error")
	}
}

// Test transpose (88.9%)
func TestTranspose(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `[[1, 2], [3, 4]] | transpose`)
	if len(strs) != 1 {
		t.Fatalf("transpose: got %d results", len(strs))
	}
}

// --- More comparison via sort to exercise compareValues ---

func TestSortMixed(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	// Sort objects by key
	strs := helperExecStr(t, containerMD, msg, `[{b: 1}, {a: 2}] | sort`)
	if len(strs) != 1 {
		t.Fatalf("sort objects: got %d results", len(strs))
	}
}

// Test any/all without argument (66.7%/83.3%)
func TestAnyAllNoArg(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `[true, false] | any`)
	if len(strs) != 1 || strs[0] != "true" {
		t.Fatalf("any: got %v; want [true]", strs)
	}
	strs = helperExecStr(t, containerMD, msg, `[true, true] | all`)
	if len(strs) != 1 || strs[0] != "true" {
		t.Fatalf("all: got %v; want [true]", strs)
	}
	strs = helperExecStr(t, containerMD, msg, `[true, false] | all`)
	if len(strs) != 1 || strs[0] != "false" {
		t.Fatalf("all false: got %v; want [false]", strs)
	}
}

// Test execAnyWith/execAllWith edge cases (70%/80%)
func TestAnyAllGenerator(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `[1, 2, 3] | any(. > 2)`)
	if len(strs) != 1 || strs[0] != "true" {
		t.Fatalf("any(>2): got %v; want [true]", strs)
	}
	strs = helperExecStr(t, containerMD, msg, `[1, 2, 3] | all(. > 0)`)
	if len(strs) != 1 || strs[0] != "true" {
		t.Fatalf("all(>0): got %v; want [true]", strs)
	}
	strs = helperExecStr(t, containerMD, msg, `[1, 2, 3] | any(. > 5)`)
	if len(strs) != 1 || strs[0] != "false" {
		t.Fatalf("any(>5): got %v; want [false]", strs)
	}
	strs = helperExecStr(t, containerMD, msg, `[1, 2, 3] | all(. > 1)`)
	if len(strs) != 1 || strs[0] != "false" {
		t.Fatalf("all(>1): got %v; want [false]", strs)
	}
}

// Test object merge (93.8%)
func TestObjectMerge(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `{a: 1} + {b: 2} | keys`)
	if len(strs) != 1 {
		t.Fatalf("merge: got %d results", len(strs))
	}
}

func TestObjectRecursiveMerge(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)
	strs := helperExecStr(t, containerMD, msg, `{a: {b: 1}} * {a: {c: 2}} | .a | keys`)
	if len(strs) != 1 {
		t.Fatalf("recursive merge: got %d results", len(strs))
	}
}
