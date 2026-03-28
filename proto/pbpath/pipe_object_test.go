package pbpath

import (
	"strings"
	"testing"
)

// ── Object construction tests ───────────────────────────────────────────

func TestPipelineObjectConstruct(t *testing.T) {
	containerMD, _ := buildFilterTestDescriptor(t)
	msg := buildFilterTestMsg(t, containerMD, containerMD.Fields().ByName("items").Message())

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		// ── Basic construction ──────────────────────────────────────
		{
			name:     "empty object",
			pipeline: `{}`,
			wantLen:  1,
			wantStrs: []string{"{}"},
		},
		{
			name:     "single static key",
			pipeline: `{a: .name}`,
			wantLen:  1,
			wantStrs: []string{`{"a":"test-container"}`},
		},
		{
			name:     "multiple static keys",
			pipeline: `{n: .name, v: .single.value}`,
			wantLen:  1,
			wantStrs: []string{`{"n":"test-container","v":99}`},
		},
		{
			name:     "string literal key",
			pipeline: `{"my-name": .name}`,
			wantLen:  1,
			wantStrs: []string{`{"my-name":"test-container"}`},
		},
		{
			name:     "literal values",
			pipeline: `{a: 1, b: "hello", c: true, d: null}`,
			wantLen:  1,
			wantStrs: []string{`{"a":1,"b":"hello","c":true,"d":null}`},
		},
		{
			name:     "trailing comma",
			pipeline: `{a: .name,}`,
			wantLen:  1,
			wantStrs: []string{`{"a":"test-container"}`},
		},

		// ── Shorthand ──────────────────────────────────────────────
		{
			name:     "shorthand single",
			pipeline: `{name}`,
			wantLen:  1,
			wantStrs: []string{`{"name":"test-container"}`},
		},
		{
			name:     "shorthand multiple on items",
			pipeline: `.items | .[0] | {name, value}`,
			wantLen:  1,
			wantStrs: []string{`{"name":"alpha","value":10}`},
		},
		{
			name:     "mixed shorthand and explicit",
			pipeline: `{name, val: .single.value}`,
			wantLen:  1,
			wantStrs: []string{`{"name":"test-container","val":99}`},
		},

		// ── Dynamic keys ───────────────────────────────────────────
		{
			name:     "dynamic key",
			pipeline: `{(.name): .single.value}`,
			wantLen:  1,
			wantStrs: []string{`{"test-container":99}`},
		},

		// ── Nested objects ─────────────────────────────────────────
		{
			name:     "nested object",
			pipeline: `{outer: {inner: .name}}`,
			wantLen:  1,
			wantStrs: []string{`{"outer":{"inner":"test-container"}}`},
		},

		// ── Object in pipeline ─────────────────────────────────────
		{
			name:     "object after iterate",
			pipeline: `.items | .[] | {name}`,
			wantLen:  4,
			wantStrs: []string{
				`{"name":"alpha"}`,
				`{"name":"beta"}`,
				`{"name":"gamma"}`,
				`{"name":"delta"}`,
			},
		},
		{
			name:     "object with select",
			pipeline: `.items | .[] | select(.value > 20) | {name, value}`,
			wantLen:  2,
			wantStrs: []string{
				`{"name":"gamma","value":30}`,
				`{"name":"delta","value":40}`,
			},
		},

		// ── Object builtins ────────────────────────────────────────
		{
			name:     "keys on object",
			pipeline: `{a: 1, b: 2, c: 3} | keys`,
			wantLen:  1,
			wantStrs: []string{`["a","b","c"]`},
		},
		{
			name:     "values on object",
			pipeline: `{a: 1, b: 2, c: 3} | values`,
			wantLen:  1,
			wantStrs: []string{"[1,2,3]"},
		},
		{
			name:     "length on object",
			pipeline: `{a: 1, b: 2, c: 3} | length`,
			wantLen:  1,
			wantStrs: []string{"3"},
		},
		{
			name:     "type on object",
			pipeline: `{a: 1} | type`,
			wantLen:  1,
			wantStrs: []string{"object"},
		},
		{
			name:     "iterate object values",
			pipeline: `{a: 1, b: 2, c: 3} | .[]`,
			wantLen:  3,
			wantStrs: []string{"1", "2", "3"},
		},
		{
			name:     "has on object true",
			pipeline: `{a: 1, b: 2} | has("a")`,
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		{
			name:     "has on object false",
			pipeline: `{a: 1, b: 2} | has("c")`,
			wantLen:  1,
			wantStrs: []string{"false"},
		},
		{
			name:     "in builtin",
			pipeline: `"a" | in({"a": 1, "b": 2})`,
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		{
			name:     "in builtin false",
			pipeline: `"z" | in({"a": 1, "b": 2})`,
			wantLen:  1,
			wantStrs: []string{"false"},
		},

		// ── to_entries / from_entries / with_entries ────────────────
		{
			name:     "to_entries",
			pipeline: `{a: 1, b: 2} | to_entries | .[0] | .key`,
			wantLen:  1,
			wantStrs: []string{"a"},
		},
		{
			name:     "to_entries value",
			pipeline: `{a: 1, b: 2} | to_entries | .[0] | .value`,
			wantLen:  1,
			wantStrs: []string{"1"},
		},
		{
			name:     "from_entries",
			pipeline: `[{key: "x", value: 42}] | from_entries | .x`,
			wantLen:  1,
			wantStrs: []string{"42"},
		},
		{
			name:     "with_entries identity",
			pipeline: `{a: 1, b: 2} | with_entries(.) | keys`,
			wantLen:  1,
			wantStrs: []string{`["a","b"]`},
		},

		// ── Object arithmetic ──────────────────────────────────────
		{
			name:     "object merge +",
			pipeline: `{a: 1} + {b: 2}`,
			wantLen:  1,
			wantStrs: []string{`{"a":1,"b":2}`},
		},
		{
			name:     "object merge + override",
			pipeline: `{a: 1, b: 2} + {b: 3, c: 4}`,
			wantLen:  1,
			wantStrs: []string{`{"a":1,"b":3,"c":4}`},
		},
		{
			name:     "object recursive merge *",
			pipeline: `{a: {x: 1}} * {a: {y: 2}}`,
			wantLen:  1,
			wantStrs: []string{`{"a":{"x":1,"y":2}}`},
		},
		{
			name:     "add objects",
			pipeline: `[{a: 1}, {b: 2}, {c: 3}] | add`,
			wantLen:  1,
			wantStrs: []string{`{"a":1,"b":2,"c":3}`},
		},

		// ── getpath / setpath / delpaths ───────────────────────────
		{
			name:     "getpath on object",
			pipeline: `{a: {b: 42}} | getpath(["a", "b"])`,
			wantLen:  1,
			wantStrs: []string{"42"},
		},
		{
			name:     "setpath on object",
			pipeline: `{a: 1} | setpath(["b"]; 2) | .b`,
			wantLen:  1,
			wantStrs: []string{"2"},
		},
		{
			name:     "delpaths on object",
			pipeline: `{a: 1, b: 2, c: 3} | delpaths([["b"]]) | keys`,
			wantLen:  1,
			wantStrs: []string{`["a","c"]`},
		},

		// ── Field access on objects (.key) ─────────────────────────
		{
			name:     "dot access on object",
			pipeline: `{a: 1, b: 2} | .a`,
			wantLen:  1,
			wantStrs: []string{"1"},
		},

		// ── Variables and objects ───────────────────────────────────
		{
			name:     "object from variable",
			pipeline: `.name as $n | {myname: $n}`,
			wantLen:  1,
			wantStrs: []string{`{"myname":"test-container"}`},
		},
		{
			name:     "reduce into object",
			pipeline: `reduce (.items | .[] | .name) as $n ({} ; . + {($n): true})`,
			wantLen:  1,
		},

		// ── Object with if-then-else ───────────────────────────────
		{
			name:     "conditional value in object",
			pipeline: `{val: (if .single.value > 50 then "big" else "small" end)}`,
			wantLen:  1,
			wantStrs: []string{`{"val":"big"}`},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			pipe, err := ParsePipeline(containerMD, tc.pipeline)
			if err != nil {
				t.Fatalf("ParsePipeline(%q) error: %v", tc.pipeline, err)
			}
			got, err := pipe.ExecMessage(msg.ProtoReflect())
			if err != nil {
				t.Fatalf("ExecMessage(%q) error: %v", tc.pipeline, err)
			}
			if len(got) != tc.wantLen {
				t.Errorf("len(results) = %d, want %d", len(got), tc.wantLen)
				for i, v := range got {
					t.Logf("  [%d] %s", i, v.String())
				}
				return
			}
			if tc.wantStrs != nil {
				for i, want := range tc.wantStrs {
					if i >= len(got) {
						break
					}
					if got[i].String() != want {
						t.Errorf("result[%d] = %q, want %q", i, got[i].String(), want)
					}
				}
			}
		})
	}
}

// ── Object parse error tests ────────────────────────────────────────────

func TestPipelineObjectParseErrors(t *testing.T) {
	containerMD, _ := buildFilterTestDescriptor(t)

	tcs := []struct {
		name     string
		pipeline string
		wantErr  string // substring expected in error
	}{
		{
			name:     "missing closing brace",
			pipeline: `{a: 1`,
			wantErr:  "expected ',' or '}'",
		},
		{
			name:     "missing colon after string key",
			pipeline: `{"a" 1}`,
			wantErr:  "expected ':'",
		},
		{
			name:     "dynamic key missing close paren",
			pipeline: `{(.name: .value}`,
			wantErr:  "expected ')'",
		},
		{
			name:     "dynamic key missing colon",
			pipeline: `{(.name) .value}`,
			wantErr:  "expected ':'",
		},
		{
			name:     "unexpected token as key",
			pipeline: `{123: .name}`,
			wantErr:  "expected object key",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParsePipeline(containerMD, tc.pipeline)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %q, want substring %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// ── Benchmarks ──────────────────────────────────────────────────────────

func BenchmarkPipelineObjectConstruct(b *testing.B) {
	containerMD, _ := buildPipeTestDescriptor(b)
	msg := buildPipeTestMsg(b, containerMD)

	pipe, err := ParsePipeline(containerMD, `.items | .[] | {name, value}`)
	if err != nil {
		b.Fatalf("parse: %v", err)
	}
	input := []Value{MessageVal(msg.ProtoReflect())}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = pipe.Exec(input)
	}
}

func BenchmarkPipelineObjectMerge(b *testing.B) {
	containerMD, _ := buildPipeTestDescriptor(b)
	msg := buildPipeTestMsg(b, containerMD)

	pipe, err := ParsePipeline(containerMD, `{a: 1, b: 2} + {c: .single.value}`)
	if err != nil {
		b.Fatalf("parse: %v", err)
	}
	input := []Value{MessageVal(msg.ProtoReflect())}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = pipe.Exec(input)
	}
}
