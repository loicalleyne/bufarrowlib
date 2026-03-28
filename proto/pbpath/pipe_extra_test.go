package pbpath

import (
	"strings"
	"testing"
)

// ── Phase 5 extra builtins tests ────────────────────────────────────────

func TestPipelineExtraBuiltins(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		// ── sort ──
		{
			name:     "sort array",
			pipeline: `[3, 1, 2] | sort`,
			wantLen:  1,
			wantStrs: []string{"[1,2,3]"},
		},
		{
			name:     "sort strings",
			pipeline: `["banana", "apple", "cherry"] | sort`,
			wantLen:  1,
			wantStrs: []string{`["apple","banana","cherry"]`},
		},
		// ── unique ──
		{
			name:     "unique array",
			pipeline: `[1, 2, 1, 3, 2] | unique`,
			wantLen:  1,
			wantStrs: []string{"[1,2,3]"},
		},
		{
			name:     "unique strings",
			pipeline: `["a", "b", "a", "c"] | unique`,
			wantLen:  1,
			wantStrs: []string{`["a","b","c"]`},
		},
		// ── keys_unsorted ──
		{
			name:     "keys_unsorted object",
			pipeline: `{b: 2, a: 1} | keys_unsorted`,
			wantLen:  1,
			wantStrs: []string{`"b"`}, // first key should be "b"
		},
		{
			name:     "keys_unsorted array",
			pipeline: `["x", "y", "z"] | keys_unsorted`,
			wantLen:  1,
			wantStrs: []string{"[0,1,2]"},
		},
		// ── utf8bytelength ──
		{
			name:     "utf8bytelength ascii",
			pipeline: `"hello" | utf8bytelength`,
			wantLen:  1,
			wantStrs: []string{"5"},
		},
		{
			name:     "utf8bytelength unicode",
			pipeline: `"café" | utf8bytelength`,
			wantLen:  1,
			wantStrs: []string{"5"}, // é is 2 bytes in UTF-8
		},
		// ── type selection filters ──
		{
			name:     "strings filter",
			pipeline: `.name | strings`,
			wantLen:  1,
			wantStrs: []string{"test-container"},
		},
		{
			name:     "numbers filter rejects string",
			pipeline: `.name | numbers`,
			wantLen:  0,
		},
		{
			name:     "arrays filter",
			pipeline: `.items | arrays`,
			wantLen:  1,
		},
		{
			name:     "nulls filter on null",
			pipeline: `null | nulls`,
			wantLen:  1,
			wantStrs: []string{"null"},
		},
		{
			name:     "nulls filter on string",
			pipeline: `"hello" | nulls`,
			wantLen:  0,
		},
		{
			name:     "booleans filter",
			pipeline: `true | booleans`,
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		{
			name:     "scalars filter on number",
			pipeline: `42 | scalars`,
			wantLen:  1,
			wantStrs: []string{"42"},
		},
		{
			name:     "iterables filter on array",
			pipeline: `[1,2] | iterables`,
			wantLen:  1,
		},
		{
			name:     "iterables filter on number",
			pipeline: `42 | iterables`,
			wantLen:  0,
		},
		// ── @text format ──
		{
			name:     "at text string",
			pipeline: `"hello" | @text`,
			wantLen:  1,
			wantStrs: []string{"hello"},
		},
		{
			name:     "at text number",
			pipeline: `42 | @text`,
			wantLen:  1,
			wantStrs: []string{"42"},
		},
		// ── not_null ──
		{
			name:     "not_null on value",
			pipeline: `"hello" | not_null`,
			wantLen:  1,
			wantStrs: []string{"hello"},
		},
		{
			name:     "not_null on null",
			pipeline: `null | not_null`,
			wantLen:  0,
		},
		// ── scan(re) ──
		{
			name:     "scan simple",
			pipeline: `"test 123 foo 456" | scan("[0-9]+")`,
			wantLen:  2,
			wantStrs: []string{"123", "456"},
		},
		{
			name:     "scan with groups",
			pipeline: `"2024-01-15" | scan("([0-9]+)-([0-9]+)-([0-9]+)")`,
			wantLen:  1,
			wantStrs: []string{"2024"},
		},
		// ── splits(re) ──
		{
			name:     "splits regex",
			pipeline: `"a,b,,c" | [splits(",")]`,
			wantLen:  1,
			wantStrs: []string{`["a","b","","c"]`},
		},
		// ── first(f) / last(f) ──
		{
			name:     "first with filter",
			pipeline: `.items | first(.[])`,
			wantLen:  1,
		},
		{
			name:     "last with filter",
			pipeline: `.items | last(.[])`,
			wantLen:  1,
		},
		// ── transpose ──
		{
			name:     "transpose",
			pipeline: `[[1,2],[3,4]] | transpose`,
			wantLen:  1,
			wantStrs: []string{"[[1,3],[2,4]]"},
		},
		{
			name:     "transpose uneven",
			pipeline: `[[1,2,3],[4,5]] | transpose`,
			wantLen:  1,
			wantStrs: []string{"[[1,4],[2,5],[3,null]]"},
		},
		// ── builtins ──
		{
			name:     "builtins returns array",
			pipeline: `[builtins] | length > 20`,
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		// ── paths on object ──
		{
			name:     "paths on object",
			pipeline: `{a: 1, b: {c: 2}} | [paths]`,
			wantLen:  1,
			wantStrs: []string{`["a"]`},
		},
		{
			name:     "leaf_paths on object",
			pipeline: `{a: 1, b: {c: 2}} | [leaf_paths]`,
			wantLen:  1,
			wantStrs: []string{`["a"]`},
		},
		// ── path(f) ──
		{
			name:     "path expression",
			pipeline: `{a: 1, b: 2} | path(.a)`,
			wantLen:  1,
			wantStrs: []string{`["a"]`},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParsePipeline(containerMD, tc.pipeline)
			if err != nil {
				t.Fatalf("ParsePipeline(%q): %v", tc.pipeline, err)
			}
			got, err := p.Exec([]Value{MessageVal(msg.ProtoReflect())})
			if err != nil {
				t.Fatalf("Exec(%q): %v", tc.pipeline, err)
			}
			if len(got) != tc.wantLen {
				strs := make([]string, len(got))
				for i, v := range got {
					strs[i] = v.String()
				}
				t.Fatalf("len(results) = %d, want %d; got %v", len(got), tc.wantLen, strs)
			}
			for i, want := range tc.wantStrs {
				if i >= len(got) {
					break
				}
				gs := got[i].String()
				if !strings.Contains(gs, want) {
					t.Errorf("result[%d] = %q, want substring %q", i, gs, want)
				}
			}
		})
	}
}

// ── def tests ───────────────────────────────────────────────────────────

func TestPipelineDef(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		{
			name:     "def zero-arg",
			pipeline: `def double: . * 2; 5 | double`,
			wantLen:  1,
			wantStrs: []string{"10"},
		},
		{
			name:     "def with param",
			pipeline: `def addN(n): . + n; 5 | addN(3)`,
			wantLen:  1,
			wantStrs: []string{"8"},
		},
		{
			name:     "def used in pipeline",
			pipeline: `def inc: . + 1; .items | .[] | .value | inc`,
			wantLen:  4,
			wantStrs: []string{"11", "21", "31", "41"},
		},
		{
			name:     "def multiple functions",
			pipeline: `def double: . * 2; def inc: . + 1; 3 | double | inc`,
			wantLen:  1,
			wantStrs: []string{"7"},
		},
		{
			name:     "def with two params",
			pipeline: `def between(a; b): . >= a and . <= b; 5 | between(3; 7)`,
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		{
			name:     "def with string interp",
			pipeline: `def greet: "hello \(.)"; "world" | greet`,
			wantLen:  1,
			wantStrs: []string{"hello world"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParsePipeline(containerMD, tc.pipeline)
			if err != nil {
				t.Fatalf("ParsePipeline(%q): %v", tc.pipeline, err)
			}
			got, err := p.Exec([]Value{MessageVal(msg.ProtoReflect())})
			if err != nil {
				t.Fatalf("Exec(%q): %v", tc.pipeline, err)
			}
			if len(got) != tc.wantLen {
				strs := make([]string, len(got))
				for i, v := range got {
					strs[i] = v.String()
				}
				t.Fatalf("len(results) = %d, want %d; got %v", len(got), tc.wantLen, strs)
			}
			for i, want := range tc.wantStrs {
				if i >= len(got) {
					break
				}
				gs := got[i].String()
				if !strings.Contains(gs, want) {
					t.Errorf("result[%d] = %q, want substring %q", i, gs, want)
				}
			}
		})
	}
}

// ── Compare values tests ────────────────────────────────────────────────

func TestPipeCompareValues(t *testing.T) {
	tcs := []struct {
		name string
		a, b Value
		want int // <0, 0, >0
	}{
		{"null eq null", Null(), Null(), 0},
		{"null < false", Null(), ScalarBool(false), -1},
		{"false < true", ScalarBool(false), ScalarBool(true), -1},
		{"true < number", ScalarBool(true), ScalarInt64(1), -1},
		{"number < string", ScalarInt64(1), ScalarString("a"), -1},
		{"string < array", ScalarString("a"), ListVal(nil), -1},
		{"int compare", ScalarInt64(1), ScalarInt64(2), -1},
		{"int equal", ScalarInt64(5), ScalarInt64(5), 0},
		{"string compare", ScalarString("apple"), ScalarString("banana"), -1},
		{"string equal", ScalarString("x"), ScalarString("x"), 0},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := compareValues(tc.a, tc.b)
			if tc.want < 0 && got >= 0 {
				t.Errorf("compareValues = %d, want <0", got)
			} else if tc.want == 0 && got != 0 {
				t.Errorf("compareValues = %d, want 0", got)
			} else if tc.want > 0 && got <= 0 {
				t.Errorf("compareValues = %d, want >0", got)
			}
		})
	}
}

func BenchmarkPipelineSort(b *testing.B) {
	containerMD, _ := buildPipeTestDescriptor(b)
	msg := buildPipeTestMsg(b, containerMD)

	p, err := ParsePipeline(containerMD, `[.items | .[] | .value] | sort`)
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

func BenchmarkPipelineDef(b *testing.B) {
	containerMD, _ := buildPipeTestDescriptor(b)
	msg := buildPipeTestMsg(b, containerMD)

	p, err := ParsePipeline(containerMD, `def double: . * 2; .items | .[] | .value | double`)
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
