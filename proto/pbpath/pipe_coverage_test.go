package pbpath

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// helperExec parses and executes a pipeline against a message, returning the results.
func helperExec(t *testing.T, md protoreflect.MessageDescriptor, msg proto.Message, pipeline string) []Value {
	t.Helper()
	p, err := ParsePipeline(md, pipeline)
	if err != nil {
		t.Fatalf("ParsePipeline(%q): %v", pipeline, err)
	}
	results, err := p.ExecMessage(msg.ProtoReflect())
	if err != nil {
		t.Fatalf("Exec(%q): %v", pipeline, err)
	}
	return results
}

// helperExecStr is like helperExec but returns string representations.
func helperExecStr(t *testing.T, md protoreflect.MessageDescriptor, msg proto.Message, pipeline string) []string {
	t.Helper()
	results := helperExec(t, md, msg, pipeline)
	out := make([]string, len(results))
	for i, v := range results {
		out[i] = v.String()
	}
	return out
}

// ── builtins with 0% or low coverage ────────────────────────────────────

func TestPipeBuiltinValues(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	// .lookup | to_entries | .[] | .value — exercises builtinValues indirectly
	// and also exercises object operations
	results := helperExec(t, containerMD, msg, `{a: .name, b: .single.name} | keys`)
	if len(results) != 1 {
		t.Fatalf("keys len = %d; want 1", len(results))
	}
}

func TestPipeBuiltinLength(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		want     string
	}{
		{"string length", `.name | length`, "14"}, // "test-container" = 14 chars
		{"list length", `.items | length`, "4"},
		{"null length", `null | length`, "0"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			strs := helperExecStr(t, containerMD, msg, tc.pipeline)
			if len(strs) != 1 || strs[0] != tc.want {
				t.Fatalf("got %v; want [%s]", strs, tc.want)
			}
		})
	}
}

func TestPipeBuiltinType(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		want     string
	}{
		{"string type", `.name | type`, "string"},
		{"number type", `.single.value | type`, "number"},
		{"bool type", `.single.active | type`, "boolean"},
		{"null type", `null | type`, "null"},
		{"array type", `[.items | .[] | .name] | type`, "array"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			strs := helperExecStr(t, containerMD, msg, tc.pipeline)
			if len(strs) != 1 || strs[0] != tc.want {
				t.Fatalf("got %v; want [%s]", strs, tc.want)
			}
		})
	}
}

func TestPipeBuiltinTostring(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `.single.value | tostring`)
	if len(strs) != 1 || strs[0] != "99" {
		t.Fatalf("tostring: got %v; want [99]", strs)
	}
}

func TestPipeBuiltinTonumber(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `.single.value | tonumber`)
	if len(strs) != 1 || strs[0] != "99" {
		t.Fatalf("tonumber: got %v; want [99]", strs)
	}
}

func TestPipeBuiltinKeys(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	// keys on constructed object
	results := helperExec(t, containerMD, msg, `{a: .name, b: .single.name} | keys`)
	if len(results) != 1 {
		t.Fatalf("keys: got %d results; want 1", len(results))
	}
}

func TestPipeBuiltinAdd(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	// add list of numbers
	strs := helperExecStr(t, containerMD, msg, `[.items | .[] | .value] | add`)
	if len(strs) != 1 || strs[0] != "100" {
		t.Fatalf("add numbers: got %v; want [100]", strs)
	}

	// add list of strings
	strs = helperExecStr(t, containerMD, msg, `[.items | .[] | .name] | add`)
	if len(strs) != 1 || strs[0] != "alphabetagammadelta" {
		t.Fatalf("add strings: got %v; want [alphabetagammadelta]", strs)
	}
}

func TestPipeBuiltinAsciiDownUpcase(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `.items | .[0] | .name | ascii_downcase`)
	if len(strs) != 1 || strs[0] != "alpha" {
		t.Fatalf("ascii_downcase: got %v", strs)
	}

	strs = helperExecStr(t, containerMD, msg, `.items | .[0] | .name | ascii_upcase`)
	if len(strs) != 1 || strs[0] != "ALPHA" {
		t.Fatalf("ascii_upcase: got %v", strs)
	}
}

func TestPipeBuiltinLtrimstrRtrimstr(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `.name | ltrimstr("test-")`)
	if len(strs) != 1 || strs[0] != "container" {
		t.Fatalf("ltrimstr: got %v; want [container]", strs)
	}

	strs = helperExecStr(t, containerMD, msg, `.name | rtrimstr("-container")`)
	if len(strs) != 1 || strs[0] != "test" {
		t.Fatalf("rtrimstr: got %v; want [test]", strs)
	}
}

func TestPipeBuiltinStartswithEndswith(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `.name | startswith("test")`)
	if len(strs) != 1 || strs[0] != "true" {
		t.Fatalf("startswith: got %v", strs)
	}

	strs = helperExecStr(t, containerMD, msg, `.name | endswith("container")`)
	if len(strs) != 1 || strs[0] != "true" {
		t.Fatalf("endswith: got %v", strs)
	}
}

func TestPipeBuiltinSplit(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `.name | split("-") | length`)
	if len(strs) != 1 || strs[0] != "2" {
		t.Fatalf("split: got %v", strs)
	}
}

func TestPipeBuiltinJoin(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[.items | .[] | .name] | join(",")`)
	if len(strs) != 1 || strs[0] != "alpha,beta,gamma,delta" {
		t.Fatalf("join: got %v", strs)
	}
}

func TestPipeBuiltinTest(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `.name | test("test.*")`)
	if len(strs) != 1 || strs[0] != "true" {
		t.Fatalf("test: got %v", strs)
	}
}

func TestPipeBuiltinMatch(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `.name | match("(test)-(.*)")`)
	if len(results) != 1 {
		t.Fatalf("match: got %d results; want 1", len(results))
	}
}

func TestPipeBuiltinExplode(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `"abc" | explode | length`)
	if len(strs) != 1 || strs[0] != "3" {
		t.Fatalf("explode: got %v", strs)
	}
}

func TestPipeBuiltinImplode(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `"abc" | explode | implode`)
	if len(strs) != 1 || strs[0] != "abc" {
		t.Fatalf("implode: got %v", strs)
	}
}

func TestPipeBuiltinGsub(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `.name | gsub("-"; "_")`)
	if len(strs) != 1 || strs[0] != "test_container" {
		t.Fatalf("gsub: got %v", strs)
	}
}

func TestPipeBuiltinSub(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `.name | sub("-"; "_")`)
	if len(strs) != 1 || strs[0] != "test_container" {
		t.Fatalf("sub: got %v", strs)
	}
}

func TestPipeBuiltinMap(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[.items | .[] | .value] | map(. + 1)`)
	if len(strs) != 1 {
		t.Fatalf("map: got %d results", len(strs))
	}
}

func TestPipeBuiltinSortBy(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `[.items | .[] | .name] | sort_by(.)`)
	if len(results) != 1 {
		t.Fatalf("sort_by: got %d results", len(results))
	}
}

func TestPipeBuiltinGroupBy(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `[.items | .[] | .kind] | group_by(.)`)
	if len(results) != 1 {
		t.Fatalf("group_by: got %d results", len(results))
	}
}

func TestPipeBuiltinUniqueBy(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `[.items | .[] | .kind] | unique_by(.) | length`)
	if len(results) != 1 {
		t.Fatalf("unique_by: got %d results", len(results))
	}
}

func TestPipeBuiltinMinBy(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[.items | .[] | .value] | min_by(.)`)
	if len(strs) != 1 || strs[0] != "10" {
		t.Fatalf("min_by: got %v", strs)
	}
}

func TestPipeBuiltinMaxBy(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[.items | .[] | .value] | max_by(.)`)
	if len(strs) != 1 || strs[0] != "40" {
		t.Fatalf("max_by: got %v", strs)
	}
}

func TestPipeBuiltinLimit(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[.items | .[] | .name] | limit(2; .[])`)
	if len(strs) != 2 {
		t.Fatalf("limit: got %d results; want 2", len(strs))
	}
}

func TestPipeBuiltinFlatten(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[[.items | .[] | .name], [.items | .[] | .kind]] | flatten | length`)
	if len(strs) != 1 || strs[0] != "8" {
		t.Fatalf("flatten: got %v", strs)
	}
}

func TestPipeBuiltinReverse(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[.items | .[] | .name] | reverse | .[0]`)
	if len(strs) != 1 || strs[0] != "delta" {
		t.Fatalf("reverse: got %v", strs)
	}
}

func TestPipeBuiltinFirst(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `first(.items | .[] | .name)`)
	if len(strs) != 1 || strs[0] != "alpha" {
		t.Fatalf("first: got %v", strs)
	}
}

func TestPipeBuiltinLast(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `last(.items | .[] | .name)`)
	if len(strs) != 1 || strs[0] != "delta" {
		t.Fatalf("last: got %v", strs)
	}
}

func TestPipeBuiltinNth(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	// nth as array index
	strs := helperExecStr(t, containerMD, msg, `[.items | .[] | .name] | .[2]`)
	if len(strs) != 1 || strs[0] != "gamma" {
		t.Fatalf("nth: got %v", strs)
	}
}

func TestPipeBuiltinIndices(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `"abcabc" | indices("bc") | length`)
	if len(strs) != 1 || strs[0] != "2" {
		t.Fatalf("indices: got %v; want [2]", strs)
	}
}

func TestPipeBuiltinIndex(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `"abcdef" | index("cd")`)
	if len(strs) != 1 || strs[0] != "2" {
		t.Fatalf("index: got %v", strs)
	}
}

func TestPipeBuiltinRindex(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `"abcabc" | rindex("bc")`)
	if len(strs) != 1 || strs[0] != "4" {
		t.Fatalf("rindex: got %v", strs)
	}
}

func TestPipeBuiltinContains(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `.name | contains("test")`)
	if len(strs) != 1 || strs[0] != "true" {
		t.Fatalf("contains: got %v", strs)
	}
}

func TestPipeBuiltinInside(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `"test" | inside("test-container")`)
	if len(strs) != 1 || strs[0] != "true" {
		t.Fatalf("inside: got %v", strs)
	}
}

func TestPipeBuiltinMathFuncs(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
	}{
		{"fabs", `.single.score | fabs`},
		{"sqrt", `.single.score | sqrt`},
		{"pow", `[.single.score, 2] | pow`},
		{"nan", `nan`},
		{"infinite", `infinite`},
		{"isnan", `1 | isnan`},
		{"isinfinite", `1 | isinfinite`},
		{"isnormal", `1 | isnormal`},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			results := helperExec(t, containerMD, msg, tc.pipeline)
			if len(results) == 0 {
				t.Fatalf("got no results")
			}
		})
	}
}

func TestPipeBuiltinTojsonFromjson(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `{a: .name} | tojson`)
	if len(strs) != 1 {
		t.Fatalf("tojson: got %d results", len(strs))
	}

	// fromjson
	results := helperExec(t, containerMD, msg, `"{\"x\":1}" | fromjson | .x`)
	if len(results) != 1 {
		t.Fatalf("fromjson: got %d results; want 1", len(results))
	}
}

func TestPipeBuiltinBase64(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `"hello" | @base64`)
	if len(strs) != 1 || strs[0] != "aGVsbG8=" {
		t.Fatalf("base64: got %v", strs)
	}

	strs = helperExecStr(t, containerMD, msg, `"aGVsbG8=" | @base64d`)
	if len(strs) != 1 || strs[0] != "hello" {
		t.Fatalf("base64d: got %v", strs)
	}
}

func TestPipeBuiltinURI(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `"hello world" | @uri`)
	if len(strs) != 1 || strs[0] != "hello+world" {
		t.Fatalf("uri: got %v", strs)
	}
}

func TestPipeBuiltinHTML(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `"<b>hello</b>" | @html`)
	if len(strs) != 1 || strs[0] != "&lt;b&gt;hello&lt;/b&gt;" {
		t.Fatalf("html: got %v", strs)
	}
}

func TestPipeBuiltinJSON(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `{a: 1} | @json`)
	if len(strs) != 1 {
		t.Fatalf("json: got %d results", len(strs))
	}
}

// ── Control flow builtins ───────────────────────────────────────────────

func TestPipeBuiltinIfElse(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `.single.value | if . > 50 then "big" else "small" end`)
	if len(strs) != 1 || strs[0] != "big" {
		t.Fatalf("if-else: got %v", strs)
	}
}

func TestPipeBuiltinTryCatch(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `try error("boom") catch "caught"`)
	if len(strs) != 1 || strs[0] != "caught" {
		t.Fatalf("try-catch: got %v", strs)
	}
}

func TestPipeBuiltinReduce(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `reduce (.items | .[] | .value) as $x (0; . + $x)`)
	if len(strs) != 1 || strs[0] != "100" {
		t.Fatalf("reduce: got %v", strs)
	}
}

func TestPipeBuiltinForeach(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[foreach (.items | .[] | .value) as $x (0; . + $x)]`)
	if len(strs) != 1 {
		t.Fatalf("foreach: got %d results", len(strs))
	}
}

func TestPipeBuiltinWhile(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[1 | while(. < 10; . * 2)]`)
	if len(strs) != 1 {
		t.Fatalf("while: got %d results", len(strs))
	}
}

func TestPipeBuiltinUntil(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `1 | until(. >= 10; . * 2)`)
	if len(strs) != 1 {
		t.Fatalf("until: got %v", strs)
	}
}

func TestPipeBuiltinEnv(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	// env should at least work (returns object)
	results := helperExec(t, containerMD, msg, `env`)
	if len(results) != 1 {
		t.Fatalf("env: got %d results", len(results))
	}
}

func TestPipeBuiltinDebug(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	// debug should pass through the value
	strs := helperExecStr(t, containerMD, msg, `42 | debug`)
	if len(strs) != 1 || strs[0] != "42" {
		t.Fatalf("debug: got %v", strs)
	}
}

func TestPipeBuiltinError(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	// error should produce an error
	p, err := ParsePipeline(containerMD, `error("test error")`)
	if err != nil {
		t.Fatalf("ParsePipeline: %v", err)
	}
	_, err = p.ExecMessage(msg.ProtoReflect())
	if err == nil {
		t.Fatal("expected error from error(), got nil")
	}
	if !strings.Contains(err.Error(), "test error") {
		t.Fatalf("error should contain 'test error': %v", err)
	}
}

func TestPipeBuiltinRange(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[5 | range]`)
	if len(strs) != 1 {
		t.Fatalf("range: got %d results", len(strs))
	}
}

func TestPipeBuiltinFloorCeilRound(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		want     string
	}{
		{"floor", `3.7 | floor`, "3"},
		{"ceil", `3.2 | ceil`, "4"},
		{"round", `3.5 | round`, "4"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			strs := helperExecStr(t, containerMD, msg, tc.pipeline)
			if len(strs) != 1 || strs[0] != tc.want {
				t.Fatalf("got %v; want [%s]", strs, tc.want)
			}
		})
	}
}

func TestPipeBuiltinMin(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[.items | .[] | .value] | min`)
	if len(strs) != 1 || strs[0] != "10" {
		t.Fatalf("min: got %v", strs)
	}
}

func TestPipeBuiltinMax(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[.items | .[] | .value] | max`)
	if len(strs) != 1 || strs[0] != "40" {
		t.Fatalf("max: got %v", strs)
	}
}

func TestPipeBuiltinAny(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[.items | .[] | .active] | any`)
	if len(strs) != 1 || strs[0] != "true" {
		t.Fatalf("any: got %v", strs)
	}
}

func TestPipeBuiltinAll(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[.items | .[] | .active] | all`)
	if len(strs) != 1 || strs[0] != "false" {
		t.Fatalf("all: got %v", strs)
	}
}

// ── Extra builtins ──────────────────────────────────────────────────────

func TestPipeBuiltinPaths(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `.single | [paths]`)
	if len(results) != 1 {
		t.Fatalf("paths: got %d results", len(results))
	}
}

func TestPipeBuiltinLeafPaths(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `.single | [leaf_paths]`)
	if len(results) != 1 {
		t.Fatalf("leaf_paths: got %d results", len(results))
	}
}

func TestPipeBuiltinUtf8bytelength(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `"hello" | utf8bytelength`)
	if len(strs) != 1 || strs[0] != "5" {
		t.Fatalf("utf8bytelength: got %v", strs)
	}
}

func TestPipeBuiltinSort(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[3, 1, 2] | sort | .[0]`)
	if len(strs) != 1 || strs[0] != "1" {
		t.Fatalf("sort: got %v", strs)
	}
}

func TestPipeBuiltinUnique(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[1, 2, 1, 3] | unique | length`)
	if len(strs) != 1 || strs[0] != "3" {
		t.Fatalf("unique: got %v", strs)
	}
}

func TestPipeBuiltinNotNull(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[1, null, 2] | [.[] | not_null] | length`)
	if len(strs) != 1 || strs[0] != "2" {
		t.Fatalf("not_null: got %v", strs)
	}
}

func TestPipeBuiltinTranspose(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `[[1, 2], [3, 4]] | transpose`)
	if len(results) != 1 {
		t.Fatalf("transpose: got %d results", len(results))
	}
}

func TestPipeBuiltinScan(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `"abcabc" | [scan("b")]`)
	if len(strs) != 1 {
		t.Fatalf("scan: got %d results", len(strs))
	}
}

func TestPipeBuiltinSplits(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `"a,b,c" | [splits(",")]`)
	if len(strs) != 1 {
		t.Fatalf("splits: got %d results", len(strs))
	}
}

func TestPipeBuiltinFirstLast(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `first(.items | .[] | .name)`)
	if len(strs) != 1 || strs[0] != "alpha" {
		t.Fatalf("first: got %v", strs)
	}

	strs = helperExecStr(t, containerMD, msg, `last(.items | .[] | .name)`)
	if len(strs) != 1 || strs[0] != "delta" {
		t.Fatalf("last: got %v", strs)
	}
}

func TestPipeBuiltinToStringInt(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `.single.value | tostring`)
	if len(strs) != 1 || strs[0] != "99" {
		t.Fatalf("tostring int: got %v", strs)
	}
}

func TestPipeBuiltinKeysUnsorted(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `{b: 1, a: 2} | keys_unsorted`)
	if len(results) != 1 {
		t.Fatalf("keys_unsorted: got %d results", len(results))
	}
}

func TestPipeBuiltinScalars(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `[1, "a", null, true] | [.[] | scalars]`)
	if len(strs) != 1 {
		t.Fatalf("scalars: got %d results", len(strs))
	}
}

func TestPipeBuiltinIterables(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `[[1, 2], "skip", [3]] | [.[] | iterables]`)
	if len(results) != 1 {
		t.Fatalf("iterables: got %d results", len(results))
	}
}

func TestPipeBuiltinBuiltins(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `[builtins] | length`)
	if len(results) != 1 {
		t.Fatalf("builtins: got %d results", len(results))
	}
}

// ── Pipe object operations ──────────────────────────────────────────────

func TestPipeObjectConstruct(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `{name: .name, count: (.items | length)}`)
	if len(results) != 1 {
		t.Fatalf("object construct: got %d results", len(results))
	}
}

func TestPipeObjectObjKeys(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `{a: 1, b: 2} | keys`)
	if len(results) != 1 {
		t.Fatalf("obj keys: got %d results", len(results))
	}
}

func TestPipeObjectObjValues(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `{a: 1, b: 2} | [values]`)
	if len(results) != 1 {
		t.Fatalf("obj values: got %d results", len(results))
	}
}

func TestPipeObjectObjHas(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `{a: 1, b: 2} | has("a")`)
	if len(strs) != 1 || strs[0] != "true" {
		t.Fatalf("has: got %v", strs)
	}
}

func TestPipeObjectToFromEntries(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `{a: 1, b: 2} | to_entries | length`)
	if len(results) != 1 {
		t.Fatalf("to_entries: got %d results", len(results))
	}

	results = helperExec(t, containerMD, msg, `[{key: "a", value: 1}, {key: "b", value: 2}] | from_entries`)
	if len(results) != 1 {
		t.Fatalf("from_entries: got %d results", len(results))
	}
}

func TestPipeObjectWithEntries(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `{a: 1, b: 2} | with_entries(select(.value > 1))`)
	if len(results) != 1 {
		t.Fatalf("with_entries: got %d results", len(results))
	}
}

func TestPipeObjectGetpath(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `{a: {b: 1}} | getpath(["a", "b"])`)
	if len(strs) != 1 || strs[0] != "1" {
		t.Fatalf("getpath: got %v", strs)
	}
}

func TestPipeObjectSetpath(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `{a: 1} | setpath(["b"]; 2)`)
	if len(results) != 1 {
		t.Fatalf("setpath: got %d results", len(results))
	}
}

func TestPipeObjectDelpaths(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	results := helperExec(t, containerMD, msg, `{a: 1, b: 2, c: 3} | delpaths([["a"], ["c"]])`)
	if len(results) != 1 {
		t.Fatalf("delpaths: got %d results", len(results))
	}
}

func TestPipeObjectIn(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `"a" | in({a: 1})`)
	if len(strs) != 1 || strs[0] != "true" {
		t.Fatalf("in: got %v", strs)
	}
}

// ── Variable binding ────────────────────────────────────────────────────

func TestPipeVariableBinding(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `.name as $n | $n`)
	if len(strs) != 1 || strs[0] != "test-container" {
		t.Fatalf("variable: got %v", strs)
	}
}

// ── Alternative operator ────────────────────────────────────────────────

func TestPipeAlternative(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `null // "fallback"`)
	if len(strs) != 1 || strs[0] != "fallback" {
		t.Fatalf("alternative: got %v", strs)
	}
}

// ── Optional operator ───────────────────────────────────────────────────

func TestPipeOptional(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	// .nonexistent? should produce no output (empty) rather than error
	strs := helperExecStr(t, containerMD, msg, `try .name`)
	if len(strs) != 1 || strs[0] != "test-container" {
		t.Fatalf("optional: got %v", strs)
	}
}

// ── Pipe def/funcall ────────────────────────────────────────────────────

func TestPipeDef(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `def double: . * 2; .single.value | double`)
	if len(strs) != 1 || strs[0] != "198" {
		t.Fatalf("def: got %v", strs)
	}
}

// ── Label/break ─────────────────────────────────────────────────────────

func TestPipeLabelBreak(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	// Simple label/break test
	strs := helperExecStr(t, containerMD, msg, `label $out | "before" | break $out`)
	if len(strs) == 0 {
		t.Fatal("label/break: got no results")
	}
}

// ── String interpolation ────────────────────────────────────────────────

func TestPipeStringInterp(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `"name is \(.name)"`)
	if len(strs) != 1 || strs[0] != "name is test-container" {
		t.Fatalf("string interpolation: got %v", strs)
	}
}

// ── Comma expression ────────────────────────────────────────────────────

func TestPipeCommaExpr(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	strs := helperExecStr(t, containerMD, msg, `.name, .single.name`)
	if len(strs) != 2 || strs[0] != "test-container" || strs[1] != "solo" {
		t.Fatalf("comma: got %v", strs)
	}
}

// ── Error paths for coverage of error branches ──────────────────────────

func TestPipeErrorWithMsg(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	p, err := ParsePipeline(containerMD, `error("custom message")`)
	if err != nil {
		t.Fatalf("ParsePipeline: %v", err)
	}
	_, execErr := p.ExecMessage(msg.ProtoReflect())
	if execErr == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(execErr.Error(), "custom message") {
		t.Fatalf("error message mismatch: %v", execErr)
	}
}
