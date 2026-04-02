package pbpath

import (
	"strings"
	"testing"
)

// ── Comma operator ──────────────────────────────────────────────────────

func TestPipelineComma(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		{
			name:     "two fields",
			pipeline: `.items | .[0] | .name, .value`,
			wantLen:  2,
			wantStrs: []string{"alpha", "10"},
		},
		{
			name:     "three fields",
			pipeline: `.items | .[0] | .name, .value, .active`,
			wantLen:  3,
			wantStrs: []string{"alpha", "10", "true"},
		},
		{
			name:     "comma in collect",
			pipeline: `[.items | .[0] | .name, .value]`,
			wantLen:  1, // one list with 2 elements
		},
		{
			name:     "comma feeds pipe",
			pipeline: `.items | .[0] | (.name, .kind) | ascii_upcase`,
			wantLen:  2,
			wantStrs: []string{"ALPHA", "A"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParsePipeline(containerMD, tc.pipeline)
			if err != nil {
				t.Fatalf("ParsePipeline(%q): %v", tc.pipeline, err)
			}
			results, err := p.ExecMessage(msg.ProtoReflect())
			if err != nil {
				t.Fatalf("Exec(%q): %v", tc.pipeline, err)
			}
			if len(results) != tc.wantLen {
				t.Errorf("got %d results, want %d", len(results), tc.wantLen)
				for i, v := range results {
					t.Logf("  [%d] = %s", i, v.String())
				}
				return
			}
			for i, want := range tc.wantStrs {
				if i >= len(results) {
					break
				}
				if got := results[i].String(); got != want {
					t.Errorf("result[%d] = %q, want %q", i, got, want)
				}
			}
		})
	}
}

// ── String functions ────────────────────────────────────────────────────

func TestPipelineStringFuncs(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		// ascii_downcase / ascii_upcase
		{
			name:     "ascii_downcase",
			pipeline: `"HELLO" | ascii_downcase`,
			wantLen:  1,
			wantStrs: []string{"hello"},
		},
		{
			name:     "ascii_upcase",
			pipeline: `"hello" | ascii_upcase`,
			wantLen:  1,
			wantStrs: []string{"HELLO"},
		},
		{
			name:     "upcase field",
			pipeline: `.items | .[0] | .name | ascii_upcase`,
			wantLen:  1,
			wantStrs: []string{"ALPHA"},
		},
		// ltrimstr / rtrimstr
		{
			name:     "ltrimstr",
			pipeline: `"hello world" | ltrimstr("hello ")`,
			wantLen:  1,
			wantStrs: []string{"world"},
		},
		{
			name:     "ltrimstr no match",
			pipeline: `"hello world" | ltrimstr("xyz")`,
			wantLen:  1,
			wantStrs: []string{"hello world"},
		},
		{
			name:     "rtrimstr",
			pipeline: `"hello world" | rtrimstr(" world")`,
			wantLen:  1,
			wantStrs: []string{"hello"},
		},
		// startswith / endswith
		{
			name:     "startswith true",
			pipeline: `"hello" | startswith("he")`,
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		{
			name:     "startswith false",
			pipeline: `"hello" | startswith("xyz")`,
			wantLen:  1,
			wantStrs: []string{"false"},
		},
		{
			name:     "endswith true",
			pipeline: `"hello" | endswith("lo")`,
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		{
			name:     "endswith false",
			pipeline: `"hello" | endswith("xyz")`,
			wantLen:  1,
			wantStrs: []string{"false"},
		},
		// split / join
		{
			name:     "split",
			pipeline: `"a,b,c" | split(",") | length`,
			wantLen:  1,
			wantStrs: []string{"3"},
		},
		{
			name:     "split then join",
			pipeline: `"a-b-c" | split("-") | join("+")`,
			wantLen:  1,
			wantStrs: []string{"a+b+c"},
		},
		// test
		{
			name:     "test match",
			pipeline: `"foo bar" | test("bar")`,
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		{
			name:     "test no match",
			pipeline: `"foo bar" | test("^bar")`,
			wantLen:  1,
			wantStrs: []string{"false"},
		},
		{
			name:     "test regex",
			pipeline: `"123abc" | test("[0-9]+")`,
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		// match
		{
			name:     "match found",
			pipeline: `"foo bar" | match("bar") | .[2]`,
			wantLen:  1,
			wantStrs: []string{"bar"},
		},
		{
			name:     "match offset",
			pipeline: `"foo bar" | match("bar") | .[0]`,
			wantLen:  1,
			wantStrs: []string{"4"},
		},
		// gsub / sub
		{
			name:     "gsub all",
			pipeline: `"foo bar foo" | gsub("foo"; "baz")`,
			wantLen:  1,
			wantStrs: []string{"baz bar baz"},
		},
		{
			name:     "sub first only",
			pipeline: `"foo bar foo" | sub("foo"; "baz")`,
			wantLen:  1,
			wantStrs: []string{"baz bar foo"},
		},
		// explode / implode
		{
			name:     "explode",
			pipeline: `"abc" | explode | length`,
			wantLen:  1,
			wantStrs: []string{"3"},
		},
		{
			name:     "explode implode roundtrip",
			pipeline: `"hello" | explode | implode`,
			wantLen:  1,
			wantStrs: []string{"hello"},
		},
		// select with startswith on field
		{
			name:     "select startswith",
			pipeline: `.items | .[] | select(.name | startswith("al")) | .name`,
			wantLen:  1,
			wantStrs: []string{"alpha"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParsePipeline(containerMD, tc.pipeline)
			if err != nil {
				t.Fatalf("ParsePipeline(%q): %v", tc.pipeline, err)
			}
			results, err := p.ExecMessage(msg.ProtoReflect())
			if err != nil {
				t.Fatalf("Exec(%q): %v", tc.pipeline, err)
			}
			if len(results) != tc.wantLen {
				t.Errorf("got %d results, want %d", len(results), tc.wantLen)
				for i, v := range results {
					t.Logf("  [%d] = %s", i, v.String())
				}
				return
			}
			for i, want := range tc.wantStrs {
				if i >= len(results) {
					break
				}
				if got := results[i].String(); got != want {
					t.Errorf("result[%d] = %q, want %q", i, got, want)
				}
			}
		})
	}
}

// ── Collection functions ────────────────────────────────────────────────

func TestPipelineCollectionFuncs(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		// map(f)
		{
			name:     "map field",
			pipeline: `.items | map(.name)`,
			wantLen:  1, // one list
		},
		{
			name:     "map field iterate",
			pipeline: `.items | map(.name) | .[]`,
			wantLen:  4,
			wantStrs: []string{"alpha", "beta", "gamma", "delta"},
		},
		{
			name:     "map value sum",
			pipeline: `.items | map(.value) | add`,
			wantLen:  1,
			wantStrs: []string{"100"},
		},
		// sort_by(f)
		{
			name:     "sort_by value ascending",
			pipeline: `.items | sort_by(.value) | .[] | .name`,
			wantLen:  4,
			wantStrs: []string{"alpha", "beta", "gamma", "delta"},
		},
		{
			name:     "sort_by name",
			pipeline: `.items | sort_by(.name) | .[] | .name`,
			wantLen:  4,
			wantStrs: []string{"alpha", "beta", "delta", "gamma"},
		},
		// group_by(f)
		{
			name:     "group_by kind count",
			pipeline: `.items | group_by(.kind) | length`,
			wantLen:  1,
			wantStrs: []string{"2"}, // A and B
		},
		// unique_by(f)
		{
			name:     "unique_by kind",
			pipeline: `.items | unique_by(.kind) | length`,
			wantLen:  1,
			wantStrs: []string{"2"},
		},
		{
			name:     "unique_by kind names",
			pipeline: `.items | unique_by(.kind) | .[] | .name`,
			wantLen:  2,
			wantStrs: []string{"alpha", "beta"}, // first of each kind
		},
		// min_by / max_by
		{
			name:     "min_by value",
			pipeline: `.items | min_by(.value) | .name`,
			wantLen:  1,
			wantStrs: []string{"alpha"},
		},
		{
			name:     "max_by value",
			pipeline: `.items | max_by(.value) | .name`,
			wantLen:  1,
			wantStrs: []string{"delta"},
		},
		{
			name:     "min_by score",
			pipeline: `.items | min_by(.score) | .name`,
			wantLen:  1,
			wantStrs: []string{"delta"}, // score 0.5
		},
		// flatten
		{
			name:     "flatten nested",
			pipeline: `[[1, 2], [3, 4]] | flatten | length`,
			wantLen:  1,
			wantStrs: []string{"4"},
		},
		// reverse
		{
			name:     "reverse list",
			pipeline: `.items | map(.name) | reverse | .[]`,
			wantLen:  4,
			wantStrs: []string{"delta", "gamma", "beta", "alpha"},
		},
		{
			name:     "reverse string",
			pipeline: `"abc" | reverse`,
			wantLen:  1,
			wantStrs: []string{"cba"},
		},
		// first / last
		{
			name:     "first element",
			pipeline: `.items | map(.name) | first`,
			wantLen:  1,
			wantStrs: []string{"alpha"},
		},
		{
			name:     "last element",
			pipeline: `.items | map(.name) | last`,
			wantLen:  1,
			wantStrs: []string{"delta"},
		},
		// nth(n)
		{
			name:     "nth 2",
			pipeline: `.items | map(.name) | nth(2)`,
			wantLen:  1,
			wantStrs: []string{"gamma"},
		},
		// limit(n; f)
		{
			name:     "limit 2",
			pipeline: `limit(2; .items | .[] | .name)`,
			wantLen:  2,
			wantStrs: []string{"alpha", "beta"},
		},
		// contains / inside
		{
			name:     "contains string true",
			pipeline: `"foobar" | contains("bar")`,
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		{
			name:     "contains string false",
			pipeline: `"foobar" | contains("xyz")`,
			wantLen:  1,
			wantStrs: []string{"false"},
		},
		{
			name:     "inside string",
			pipeline: `"bar" | inside("foobar")`,
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		// index / rindex
		{
			name:     "index string",
			pipeline: `"abcabc" | index("bc")`,
			wantLen:  1,
			wantStrs: []string{"1"},
		},
		{
			name:     "rindex string",
			pipeline: `"abcabc" | rindex("bc")`,
			wantLen:  1,
			wantStrs: []string{"4"},
		},
		// indices
		{
			name:     "indices string",
			pipeline: `"abcabc" | indices("bc") | length`,
			wantLen:  1,
			wantStrs: []string{"2"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParsePipeline(containerMD, tc.pipeline)
			if err != nil {
				t.Fatalf("ParsePipeline(%q): %v", tc.pipeline, err)
			}
			results, err := p.ExecMessage(msg.ProtoReflect())
			if err != nil {
				t.Fatalf("Exec(%q): %v", tc.pipeline, err)
			}
			if len(results) != tc.wantLen {
				t.Errorf("got %d results, want %d", len(results), tc.wantLen)
				for i, v := range results {
					t.Logf("  [%d] = %s", i, v.String())
				}
				return
			}
			for i, want := range tc.wantStrs {
				if i >= len(results) {
					break
				}
				if got := results[i].String(); got != want {
					t.Errorf("result[%d] = %q, want %q", i, got, want)
				}
			}
		})
	}
}

// ── Numeric functions ───────────────────────────────────────────────────

func TestPipelineNumericFuncs(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		{
			name:     "fabs positive",
			pipeline: `10 | fabs`,
			wantLen:  1,
			wantStrs: []string{"10"},
		},
		{
			name:     "sqrt 16",
			pipeline: `16 | sqrt`,
			wantLen:  1,
			wantStrs: []string{"4"},
		},
		{
			name:     "pow",
			pipeline: `[2, 10] | pow`,
			wantLen:  1,
			wantStrs: []string{"1024"},
		},
		{
			name:     "isnan false",
			pipeline: `1 | isnan`,
			wantLen:  1,
			wantStrs: []string{"false"},
		},
		{
			name:     "isnan true",
			pipeline: `nan | isnan`,
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		{
			name:     "isinfinite false",
			pipeline: `1 | isinfinite`,
			wantLen:  1,
			wantStrs: []string{"false"},
		},
		{
			name:     "isinfinite true",
			pipeline: `infinite | isinfinite`,
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		{
			name:     "isnormal true",
			pipeline: `1 | isnormal`,
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		{
			name:     "isnormal nan",
			pipeline: `nan | isnormal`,
			wantLen:  1,
			wantStrs: []string{"false"},
		},
		// Use on fields
		{
			name:     "sqrt on field score",
			pipeline: `.items | .[0] | .score | fabs`,
			wantLen:  1,
			wantStrs: []string{"1.5"},
		},
	}

	_ = msg // suppress unused if needed
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParsePipeline(containerMD, tc.pipeline)
			if err != nil {
				t.Fatalf("ParsePipeline(%q): %v", tc.pipeline, err)
			}
			results, err := p.ExecMessage(msg.ProtoReflect())
			if err != nil {
				t.Fatalf("Exec(%q): %v", tc.pipeline, err)
			}
			if len(results) != tc.wantLen {
				t.Errorf("got %d results, want %d", len(results), tc.wantLen)
				for i, v := range results {
					t.Logf("  [%d] = %s", i, v.String())
				}
				return
			}
			for i, want := range tc.wantStrs {
				if i >= len(results) {
					break
				}
				if got := results[i].String(); got != want {
					t.Errorf("result[%d] = %q, want %q", i, got, want)
				}
			}
		})
	}
}

// ── Serialization functions ─────────────────────────────────────────────

func TestPipelineSerializationFuncs(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		{
			name:     "tojson number",
			pipeline: `42 | tojson`,
			wantLen:  1,
			wantStrs: []string{"42"},
		},
		{
			name:     "tojson string",
			pipeline: `"hello" | tojson`,
			wantLen:  1,
			wantStrs: []string{`"hello"`},
		},
		{
			name:     "fromjson number",
			pipeline: `"42" | fromjson`,
			wantLen:  1,
			wantStrs: []string{"42"},
		},
		{
			name:     "fromjson string",
			pipeline: `"\"hello\"" | fromjson`,
			wantLen:  1,
			wantStrs: []string{"hello"},
		},
		{
			name:     "base64 encode",
			pipeline: `"hello" | @base64`,
			wantLen:  1,
			wantStrs: []string{"aGVsbG8="},
		},
		{
			name:     "base64 roundtrip",
			pipeline: `"hello" | @base64 | @base64d`,
			wantLen:  1,
			wantStrs: []string{"hello"},
		},
		{
			name:     "uri encode",
			pipeline: `"hello world" | @uri`,
			wantLen:  1,
			wantStrs: []string{"hello+world"},
		},
		{
			name:     "html encode",
			pipeline: `"<b>bold</b>" | @html`,
			wantLen:  1,
			wantStrs: []string{"&lt;b&gt;bold&lt;/b&gt;"},
		},
		{
			name:     "json format same as tojson",
			pipeline: `42 | @json`,
			wantLen:  1,
			wantStrs: []string{"42"},
		},
	}

	_ = msg
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParsePipeline(containerMD, tc.pipeline)
			if err != nil {
				t.Fatalf("ParsePipeline(%q): %v", tc.pipeline, err)
			}
			results, err := p.ExecMessage(msg.ProtoReflect())
			if err != nil {
				t.Fatalf("Exec(%q): %v", tc.pipeline, err)
			}
			if len(results) != tc.wantLen {
				t.Errorf("got %d results, want %d", len(results), tc.wantLen)
				for i, v := range results {
					t.Logf("  [%d] = %s", i, v.String())
				}
				return
			}
			for i, want := range tc.wantStrs {
				if i >= len(results) {
					break
				}
				if got := results[i].String(); got != want {
					t.Errorf("result[%d] = %q, want %q", i, got, want)
				}
			}
		})
	}
}

// ── Phase 3b parse error tests ──────────────────────────────────────────

func TestPipelinePhase3bParseErrors(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)

	tcs := []struct {
		name    string
		input   string
		wantErr string
	}{
		{
			name:    "map without args",
			input:   "map",
			wantErr: "requires arguments",
		},
		{
			name:    "sort_by without args",
			input:   "sort_by",
			wantErr: "requires arguments",
		},
		{
			name:    "gsub without args",
			input:   "gsub",
			wantErr: "requires arguments",
		},
		{
			name:    "map empty parens",
			input:   "map()",
			wantErr: "requires an argument",
		},
		{
			name:    "unknown format",
			input:   "@unknown",
			wantErr: "unknown format string",
		},
		{
			name:    "at without ident",
			input:   "@42",
			wantErr: "expected format name",
		},
		{
			name:    "limit missing second arg",
			input:   `limit(2)`,
			wantErr: "unknown function",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParsePipeline(containerMD, tc.input)
			if err == nil {
				t.Fatalf("ParsePipeline(%q): expected error containing %q, got nil", tc.input, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("ParsePipeline(%q): error %q does not contain %q", tc.input, err.Error(), tc.wantErr)
			}
		})
	}
}

// ── Complex pipeline integration tests ──────────────────────────────────

func TestPipelinePhase3bIntegration(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		{
			name:     "filter then sort_by",
			pipeline: `.items | [.[] | select(.active)] | sort_by(.value) | .[] | .name`,
			wantLen:  3,
			wantStrs: []string{"alpha", "gamma", "delta"},
		},
		{
			name:     "group_by then map length",
			pipeline: `.items | group_by(.kind) | map(length) | .[]`,
			wantLen:  2,
		},
		{
			name:     "map with nested pipeline",
			pipeline: `.items | map(.name | ascii_upcase) | .[]`,
			wantLen:  4,
			wantStrs: []string{"ALPHA", "BETA", "GAMMA", "DELTA"},
		},
		{
			name:     "comma with collect",
			pipeline: `[.items | .[0] | .name, .kind]`,
			wantLen:  1, // one list
		},
		{
			name:     "comma collect iterate",
			pipeline: `[.items | .[0] | .name, .kind] | .[]`,
			wantLen:  2,
			wantStrs: []string{"alpha", "A"},
		},
		{
			name:     "sort_by with tostring",
			pipeline: `.items | sort_by(.name) | first | .name`,
			wantLen:  1,
			wantStrs: []string{"alpha"},
		},
		{
			name:     "unique_by then max_by",
			pipeline: `.items | unique_by(.kind) | max_by(.value) | .name`,
			wantLen:  1,
			wantStrs: []string{"beta"},
		},
		{
			name:     "map join",
			pipeline: `.items | map(.name) | join(", ")`,
			wantLen:  1,
			wantStrs: []string{"alpha, beta, gamma, delta"},
		},
		{
			name:     "name to base64 and back",
			pipeline: `.name | @base64 | @base64d`,
			wantLen:  1,
			wantStrs: []string{"test-container"},
		},
		{
			name:     "gsub on field",
			pipeline: `.items | .[0] | .name | gsub("a"; "x")`,
			wantLen:  1,
			wantStrs: []string{"xlphx"},
		},
		{
			name:     "split then length",
			pipeline: `"a.b.c.d" | split(".") | length`,
			wantLen:  1,
			wantStrs: []string{"4"},
		},
		{
			name:     "test with select",
			pipeline: `.items | .[] | select(.name | test("^(alpha|gamma)$")) | .name`,
			wantLen:  2,
			wantStrs: []string{"alpha", "gamma"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParsePipeline(containerMD, tc.pipeline)
			if err != nil {
				t.Fatalf("ParsePipeline(%q): %v", tc.pipeline, err)
			}
			results, err := p.ExecMessage(msg.ProtoReflect())
			if err != nil {
				t.Fatalf("Exec(%q): %v", tc.pipeline, err)
			}
			if len(results) != tc.wantLen {
				t.Errorf("got %d results, want %d", len(results), tc.wantLen)
				for i, v := range results {
					t.Logf("  [%d] = %s", i, v.String())
				}
				return
			}
			for i, want := range tc.wantStrs {
				if i >= len(results) {
					break
				}
				if got := results[i].String(); got != want {
					t.Errorf("result[%d] = %q, want %q", i, got, want)
				}
			}
		})
	}
}

// ── Benchmark ───────────────────────────────────────────────────────────

func BenchmarkPipelineMapSortBy(b *testing.B) {
	containerMD, _ := buildPipeTestDescriptor(b)
	msg := buildPipeTestMsg(b, containerMD)

	p, err := ParsePipeline(containerMD, `.items | sort_by(.value) | map(.name) | join(", ")`)
	if err != nil {
		b.Fatalf("ParsePipeline: %v", err)
	}
	input := []Value{MessageVal(msg.ProtoReflect())}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := p.Exec(input)
		if err != nil {
			b.Fatal(err)
		}
	}
}
