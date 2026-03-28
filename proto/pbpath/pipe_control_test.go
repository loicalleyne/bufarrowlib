package pbpath

import (
	"strings"
	"testing"
)

// ── Arithmetic operators ────────────────────────────────────────────────

func TestPipelineArithmetic(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		// Integer arithmetic
		{
			name:     "int addition literal",
			pipeline: `5 + 3`,
			wantLen:  1,
			wantStrs: []string{"8"},
		},
		{
			name:     "int subtraction literal",
			pipeline: `10 - 3`,
			wantLen:  1,
			wantStrs: []string{"7"},
		},
		{
			name:     "int multiplication",
			pipeline: `4 * 5`,
			wantLen:  1,
			wantStrs: []string{"20"},
		},
		{
			name:     "int division",
			pipeline: `10 / 3`,
			wantLen:  1,
			wantStrs: []string{"3"},
		},
		{
			name:     "int modulo",
			pipeline: `10 % 3`,
			wantLen:  1,
			wantStrs: []string{"1"},
		},
		// Float promotion
		{
			name:     "float addition",
			pipeline: `1.5 + 2.5`,
			wantLen:  1,
			wantStrs: []string{"4"},
		},
		{
			name:     "int plus float promotes",
			pipeline: `1 + 0.5`,
			wantLen:  1,
			wantStrs: []string{"1.5"},
		},
		// String concatenation
		{
			name:     "string concat",
			pipeline: `"hello" + " " + "world"`,
			wantLen:  1,
			wantStrs: []string{"hello world"},
		},
		// Field arithmetic
		{
			name:     "field addition",
			pipeline: `.items | .[0] | .value + 5`,
			wantLen:  1,
			wantStrs: []string{"15"}, // alpha.value=10 + 5
		},
		{
			name:     "field subtraction",
			pipeline: `.items | .[1] | .value - 5`,
			wantLen:  1,
			wantStrs: []string{"15"}, // beta.value=20 - 5
		},
		{
			name:     "field multiplication",
			pipeline: `.items | .[0] | .value * 3`,
			wantLen:  1,
			wantStrs: []string{"30"}, // alpha.value=10 * 3
		},
		// Array concat with +
		{
			name:     "array concatenation",
			pipeline: `[.items | .[0] | .name] + [.items | .[1] | .name]`,
			wantLen:  1,
			wantStrs: []string{`["alpha","beta"]`},
		},
		// Operator precedence: * before +
		{
			name:     "precedence mul before add",
			pipeline: `2 + 3 * 4`,
			wantLen:  1,
			wantStrs: []string{"14"}, // 2 + (3*4) = 14, not (2+3)*4=20
		},
		// null + x = x
		{
			name:     "null plus value",
			pipeline: `null + 5`,
			wantLen:  1,
			wantStrs: []string{"5"},
		},
		// Negative literal as value (not subtraction)
		{
			name:     "negative literal",
			pipeline: `-5`,
			wantLen:  1,
			wantStrs: []string{"-5"},
		},
		{
			name:     "add negative literal",
			pipeline: `5 + -3`,
			wantLen:  1,
			wantStrs: []string{"2"},
		},
		// Unary negation of expression
		{
			name:     "unary negate field",
			pipeline: `.items | .[0] | - .value`,
			wantLen:  1,
			wantStrs: []string{"-10"},
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
				t.Fatalf("got %d results, want %d: %v", len(results), tc.wantLen, results)
			}
			if tc.wantStrs != nil {
				for i, want := range tc.wantStrs {
					got := results[i].String()
					if got != want {
						t.Errorf("result[%d] = %q, want %q", i, got, want)
					}
				}
			}
		})
	}
}

func TestPipelineArithmeticErrors(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantErr  string
	}{
		{
			name:     "division by zero int",
			pipeline: `10 / 0`,
			wantErr:  "division by zero",
		},
		{
			name:     "modulo by zero int",
			pipeline: `10 % 0`,
			wantErr:  "modulo by zero",
		},
		{
			name:     "subtract strings",
			pipeline: `"a" - "b"`,
			wantErr:  "cannot apply",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParsePipeline(containerMD, tc.pipeline)
			if err != nil {
				t.Fatalf("ParsePipeline(%q): %v", tc.pipeline, err)
			}
			_, err = p.ExecMessage(msg.ProtoReflect())
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// ── Variables ───────────────────────────────────────────────────────────

func TestPipelineVariables(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		{
			name:     "simple variable bind",
			pipeline: `.name as $n | $n`,
			wantLen:  1,
			wantStrs: []string{"test-container"},
		},
		{
			name:     "variable in subsequent pipe",
			pipeline: `.name as $n | .items | .[0] | $n`,
			wantLen:  1,
			wantStrs: []string{"test-container"},
		},
		{
			name:     "variable with field access",
			pipeline: `.items | .[0] | .name as $n | .value as $v | [$n, $v]`,
			wantLen:  1,
			wantStrs: []string{`["alpha",10]`},
		},
		{
			name:     "variable in select",
			pipeline: `.items | .[] | .kind as $k | select(.name == "alpha") | $k`,
			wantLen:  1,
			wantStrs: []string{"A"},
		},
		{
			name:     "variable scoping - inner does not leak",
			pipeline: `"outer" as $x | ("inner" as $x | $x) as $inner | [$x, $inner]`,
			wantLen:  1,
			wantStrs: []string{`["outer","inner"]`},
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
				t.Fatalf("got %d results, want %d: %v", len(results), tc.wantLen, results)
			}
			if tc.wantStrs != nil {
				for i, want := range tc.wantStrs {
					got := results[i].String()
					if got != want {
						t.Errorf("result[%d] = %q, want %q", i, got, want)
					}
				}
			}
		})
	}
}

func TestPipelineVariableErrors(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantErr  string
	}{
		{
			name:     "undefined variable",
			pipeline: `$x`,
			wantErr:  "undefined variable",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParsePipeline(containerMD, tc.pipeline)
			if err != nil {
				t.Skipf("ParsePipeline(%q): %v (parse error, not runtime)", tc.pipeline, err)
			}
			_, err = p.ExecMessage(msg.ProtoReflect())
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// ── If-then-else ────────────────────────────────────────────────────────

func TestPipelineIfThenElse(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		{
			name:     "if true then",
			pipeline: `if true then "yes" else "no" end`,
			wantLen:  1,
			wantStrs: []string{"yes"},
		},
		{
			name:     "if false then else",
			pipeline: `if false then "yes" else "no" end`,
			wantLen:  1,
			wantStrs: []string{"no"},
		},
		{
			name:     "if without else (true)",
			pipeline: `.items | .[0] | if .active then .name end`,
			wantLen:  1,
			wantStrs: []string{"alpha"},
		},
		{
			name:     "if without else (false) returns identity",
			pipeline: `.items | .[1] | if .active then .name end`,
			wantLen:  1,
			// beta.active=false, no else → identity returns the item message
		},
		{
			name:     "if with field condition",
			pipeline: `.items | .[] | if .active then .name else "inactive" end`,
			wantLen:  4,
			wantStrs: []string{"alpha", "inactive", "gamma", "delta"},
		},
		{
			name:     "elif chain",
			pipeline: `.items | .[0] | if .value > 50 then "high" elif .value > 5 then "medium" else "low" end`,
			wantLen:  1,
			wantStrs: []string{"medium"}, // alpha.value=10
		},
		{
			name:     "elif falls through to else",
			pipeline: `0 | if . > 10 then "high" elif . > 5 then "medium" else "low" end`,
			wantLen:  1,
			wantStrs: []string{"low"},
		},
		{
			name:     "nested if",
			pipeline: `.items | .[0] | if .active then if .kind == "A" then "active-A" else "active-other" end else "inactive" end`,
			wantLen:  1,
			wantStrs: []string{"active-A"},
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
				t.Fatalf("got %d results, want %d: %v", len(results), tc.wantLen, results)
			}
			if tc.wantStrs != nil {
				for i, want := range tc.wantStrs {
					got := results[i].String()
					if got != want {
						t.Errorf("result[%d] = %q, want %q", i, got, want)
					}
				}
			}
		})
	}
}

// ── Try-catch ───────────────────────────────────────────────────────────

func TestPipelineTryCatch(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		{
			name:     "try success",
			pipeline: `try .name`,
			wantLen:  1,
			wantStrs: []string{"test-container"},
		},
		{
			name:     "try catch error",
			pipeline: `try error catch "caught"`,
			wantLen:  1,
			wantStrs: []string{"caught"},
		},
		{
			name:     "try without catch suppresses",
			pipeline: `try error`,
			wantLen:  0,
		},
		{
			name:     "try catch receives error message",
			pipeline: `try error("oops") catch .`,
			wantLen:  1,
			wantStrs: []string{"oops"},
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
				t.Fatalf("got %d results, want %d: %v", len(results), tc.wantLen, results)
			}
			if tc.wantStrs != nil {
				for i, want := range tc.wantStrs {
					got := results[i].String()
					if got != want {
						t.Errorf("result[%d] = %q, want %q", i, got, want)
					}
				}
			}
		})
	}
}

// ── Alternative operator // ─────────────────────────────────────────────

func TestPipelineAlternative(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		{
			name:     "non-null passes through",
			pipeline: `.name // "default"`,
			wantLen:  1,
			wantStrs: []string{"test-container"},
		},
		{
			name:     "null falls to alternative",
			pipeline: `null // "default"`,
			wantLen:  1,
			wantStrs: []string{"default"},
		},
		{
			name:     "false falls to alternative",
			pipeline: `false // "fallback"`,
			wantLen:  1,
			wantStrs: []string{"fallback"},
		},
		{
			name:     "chain alternatives",
			pipeline: `null // false // "last"`,
			wantLen:  1,
			wantStrs: []string{"last"},
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
				t.Fatalf("got %d results, want %d: %v", len(results), tc.wantLen, results)
			}
			if tc.wantStrs != nil {
				for i, want := range tc.wantStrs {
					got := results[i].String()
					if got != want {
						t.Errorf("result[%d] = %q, want %q", i, got, want)
					}
				}
			}
		})
	}
}

// ── Optional operator ? ─────────────────────────────────────────────────

func TestPipelineOptional(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		{
			name:     "optional on valid field",
			pipeline: `.name?`,
			wantLen:  1,
			wantStrs: []string{"test-container"},
		},
		{
			name:     "optional on iterate",
			pipeline: `.items | .[]?`,
			wantLen:  4,
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
				t.Fatalf("got %d results, want %d: %v", len(results), tc.wantLen, results)
			}
			if tc.wantStrs != nil {
				for i, want := range tc.wantStrs {
					got := results[i].String()
					if got != want {
						t.Errorf("result[%d] = %q, want %q", i, got, want)
					}
				}
			}
		})
	}
}

// ── Reduce ──────────────────────────────────────────────────────────────

func TestPipelineReduce(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		{
			name:     "reduce sum values",
			pipeline: `reduce (.items | .[]) as $x (0; . + $x.value)`,
			wantLen:  1,
			wantStrs: []string{"100"}, // 10+20+30+40
		},
		{
			name:     "reduce count",
			pipeline: `reduce (.items | .[]) as $x (0; . + 1)`,
			wantLen:  1,
			wantStrs: []string{"4"},
		},
		{
			name:     "reduce concat names",
			pipeline: `reduce (.items | .[]) as $x (""; . + $x.name + " ")`,
			wantLen:  1,
			wantStrs: []string{"alpha beta gamma delta "},
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
				t.Fatalf("got %d results, want %d: %v", len(results), tc.wantLen, results)
			}
			if tc.wantStrs != nil {
				for i, want := range tc.wantStrs {
					got := results[i].String()
					if got != want {
						t.Errorf("result[%d] = %q, want %q", i, got, want)
					}
				}
			}
		})
	}
}

// ── Foreach ─────────────────────────────────────────────────────────────

func TestPipelineForeach(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		{
			name:     "foreach running sum",
			pipeline: `foreach (.items | .[]) as $x (0; . + $x.value)`,
			wantLen:  4,
			wantStrs: []string{"10", "30", "60", "100"}, // running sum
		},
		{
			name:     "foreach with extract",
			pipeline: `foreach (.items | .[]) as $x (0; . + 1; . * 10)`,
			wantLen:  4,
			wantStrs: []string{"10", "20", "30", "40"}, // counter * 10
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
				t.Fatalf("got %d results, want %d: %v", len(results), tc.wantLen, results)
			}
			if tc.wantStrs != nil {
				for i, want := range tc.wantStrs {
					got := results[i].String()
					if got != want {
						t.Errorf("result[%d] = %q, want %q", i, got, want)
					}
				}
			}
		})
	}
}

// ── Label-break ─────────────────────────────────────────────────────────

func TestPipelineLabelBreak(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		{
			name:     "label with immediate break",
			pipeline: `label $out | "before" | break $out`,
			wantLen:  1,
			wantStrs: []string{"before"},
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
				t.Fatalf("got %d results, want %d: %v", len(results), tc.wantLen, results)
			}
			if tc.wantStrs != nil {
				for i, want := range tc.wantStrs {
					got := results[i].String()
					if got != want {
						t.Errorf("result[%d] = %q, want %q", i, got, want)
					}
				}
			}
		})
	}
}

// ── Builtin functions from pipe_control.go ──────────────────────────────

func TestPipelineControlBuiltins(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		// min / max
		{
			name:     "min of array",
			pipeline: `[.items | .[] | .value] | min`,
			wantLen:  1,
			wantStrs: []string{"10"},
		},
		{
			name:     "max of array",
			pipeline: `[.items | .[] | .value] | max`,
			wantLen:  1,
			wantStrs: []string{"40"},
		},
		// floor / ceil / round
		{
			name:     "floor",
			pipeline: `3.7 | floor`,
			wantLen:  1,
			wantStrs: []string{"3"},
		},
		{
			name:     "ceil",
			pipeline: `3.2 | ceil`,
			wantLen:  1,
			wantStrs: []string{"4"},
		},
		{
			name:     "round",
			pipeline: `3.5 | round`,
			wantLen:  1,
			wantStrs: []string{"4"},
		},
		{
			name:     "round down",
			pipeline: `3.4 | round`,
			wantLen:  1,
			wantStrs: []string{"3"},
		},
		// any / all
		{
			name:     "any true",
			pipeline: `[true, false, true] | any`,
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		{
			name:     "all false",
			pipeline: `[true, false, true] | all`,
			wantLen:  1,
			wantStrs: []string{"false"},
		},
		{
			name:     "all true",
			pipeline: `[true, true] | all`,
			wantLen:  1,
			wantStrs: []string{"true"},
		},
		// range
		{
			name:     "range 4",
			pipeline: `4 | range`,
			wantLen:  4,
			wantStrs: []string{"0", "1", "2", "3"},
		},
		// debug passes through
		{
			name:     "debug passthrough",
			pipeline: `.name | debug`,
			wantLen:  1,
			wantStrs: []string{"test-container"},
		},
		// env returns null
		{
			name:     "env is null",
			pipeline: `env`,
			wantLen:  1,
			wantStrs: []string{"null"},
		},
		// while
		{
			name:     "while increment",
			pipeline: `0 | while(. < 4; . + 1)`,
			wantLen:  4,
			wantStrs: []string{"0", "1", "2", "3"},
		},
		// until
		{
			name:     "until reaches target",
			pipeline: `0 | until(. >= 5; . + 1)`,
			wantLen:  1,
			wantStrs: []string{"5"},
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
				t.Fatalf("got %d results, want %d: %v", len(results), tc.wantLen, results)
			}
			if tc.wantStrs != nil {
				for i, want := range tc.wantStrs {
					got := results[i].String()
					if got != want {
						t.Errorf("result[%d] = %q, want %q", i, got, want)
					}
				}
			}
		})
	}
}

// ── Parse errors for Phase 3c ───────────────────────────────────────────

func TestPipelineControlParseErrors(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)

	tcs := []struct {
		name     string
		pipeline string
		wantErr  string
	}{
		{
			name:     "if missing then",
			pipeline: `if true "yes" end`,
			wantErr:  "expected 'then'",
		},
		{
			name:     "if missing end",
			pipeline: `if true then "yes"`,
			wantErr:  "expected",
		},
		{
			name:     "reduce missing as",
			pipeline: `reduce .items $x (0; . + 1)`,
			wantErr:  "expected 'as'",
		},
		{
			name:     "reduce missing dollar",
			pipeline: `reduce .items as x (0; . + 1)`,
			wantErr:  "expected '$'",
		},
		{
			name:     "reduce missing open paren",
			pipeline: `reduce .items as $x 0; . + 1)`,
			wantErr:  "expected '('",
		},
		{
			name:     "label missing dollar",
			pipeline: `label out | .`,
			wantErr:  "expected '$'",
		},
		{
			name:     "break missing dollar",
			pipeline: `break out`,
			wantErr:  "expected '$'",
		},
		{
			name:     "as missing dollar",
			pipeline: `.name as n | .`,
			wantErr:  "expected '$'",
		},
		{
			name:     "as missing pipe",
			pipeline: `.name as $n .`,
			wantErr:  "expected '|'",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParsePipeline(containerMD, tc.pipeline)
			if err == nil {
				t.Fatalf("ParsePipeline(%q): expected error containing %q, got nil", tc.pipeline, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("ParsePipeline(%q): error %q does not contain %q", tc.pipeline, err.Error(), tc.wantErr)
			}
		})
	}
}

// ── Integration tests ───────────────────────────────────────────────────

func TestPipelineControlIntegration(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		{
			name:     "variable with if-then-else",
			pipeline: `.name as $n | .items | .[] | if .active then $n + ":" + .name else "skip" end`,
			wantLen:  4,
			wantStrs: []string{"test-container:alpha", "skip", "test-container:gamma", "test-container:delta"},
		},
		{
			name:     "reduce with variable and arithmetic",
			pipeline: `reduce (.items | .[]) as $x (0; . + $x.value) | . * 2`,
			wantLen:  1,
			wantStrs: []string{"200"}, // (10+20+30+40) * 2
		},
		{
			name:     "alternative with try",
			pipeline: `try error // "recovered"`,
			wantLen:  1,
			wantStrs: []string{"recovered"},
		},
		{
			name:     "if with arithmetic comparison",
			pipeline: `.items | .[] | if .value * 2 > 50 then .name else empty end`,
			wantLen:  2,
			wantStrs: []string{"gamma", "delta"}, // 30*2=60>50, 40*2=80>50
		},
		{
			name:     "foreach with variable",
			pipeline: `"prefix" as $p | foreach (.items | .[]) as $x (0; . + 1)`,
			wantLen:  4,
			wantStrs: []string{"1", "2", "3", "4"},
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
				t.Fatalf("got %d results, want %d: %v", len(results), tc.wantLen, results)
			}
			if tc.wantStrs != nil {
				for i, want := range tc.wantStrs {
					got := results[i].String()
					if got != want {
						t.Errorf("result[%d] = %q, want %q", i, got, want)
					}
				}
			}
		})
	}
}

// ── Benchmark ───────────────────────────────────────────────────────────

func BenchmarkPipelineReduce(b *testing.B) {
	containerMD, _ := buildPipeTestDescriptor(b)
	msg := buildPipeTestMsg(b, containerMD)

	p, err := ParsePipeline(containerMD, `reduce (.items | .[]) as $x (0; . + $x.value)`)
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

func BenchmarkPipelineIfThenElse(b *testing.B) {
	containerMD, _ := buildPipeTestDescriptor(b)
	msg := buildPipeTestMsg(b, containerMD)

	p, err := ParsePipeline(containerMD, `.items | .[] | if .active then .name else "inactive" end`)
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

// ── String interpolation ────────────────────────────────────────────────

func TestPipelineStringInterp(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)
	msg := buildPipeTestMsg(t, containerMD)

	tcs := []struct {
		name     string
		pipeline string
		wantLen  int
		wantStrs []string
	}{
		// Basic field interpolation
		{
			name:     "simple field interp",
			pipeline: `"hello \(.name) world"`,
			wantLen:  1,
			wantStrs: []string{"hello test-container world"},
		},
		// Empty prefix and suffix
		{
			name:     "interp at start",
			pipeline: `"\(.name) rocks"`,
			wantLen:  1,
			wantStrs: []string{"test-container rocks"},
		},
		{
			name:     "interp at end",
			pipeline: `"name is \(.name)"`,
			wantLen:  1,
			wantStrs: []string{"name is test-container"},
		},
		{
			name:     "interp only",
			pipeline: `"\(.name)"`,
			wantLen:  1,
			wantStrs: []string{"test-container"},
		},
		// Multiple interpolations
		{
			name:     "two interpolations",
			pipeline: `"\(.name) has \(.items | length) items"`,
			wantLen:  1,
			wantStrs: []string{"test-container has 4 items"},
		},
		// Interpolated expression with pipeline
		{
			name:     "pipeline inside interp",
			pipeline: `"first: \(.items | .[0] | .name)"`,
			wantLen:  1,
			wantStrs: []string{"first: alpha"},
		},
		// Nested function call (tests paren depth tracking)
		{
			name:     "nested parens in interp",
			pipeline: `.items | .[] | "item: \(if .active then .name else "off" end)"`,
			wantLen:  4,
			wantStrs: []string{"item: alpha", "item: off", "item: gamma", "item: delta"},
		},
		// Integer interpolation
		{
			name:     "integer interp",
			pipeline: `"count: \(.items | length)"`,
			wantLen:  1,
			wantStrs: []string{"count: 4"},
		},
		// Arithmetic inside interpolation
		{
			name:     "arithmetic interp",
			pipeline: `"result: \(2 + 3)"`,
			wantLen:  1,
			wantStrs: []string{"result: 5"},
		},
		// Boolean interpolation
		{
			name:     "boolean interp",
			pipeline: `"active: \(.single | .active)"`,
			wantLen:  1,
			wantStrs: []string{"active: true"},
		},
		// Null interpolation
		{
			name:     "null interp",
			pipeline: `"value: \(null)"`,
			wantLen:  1,
			wantStrs: []string{"value: null"},
		},
		// String within interp
		{
			name:     "string concat via interp",
			pipeline: `"\("hello") \("world")"`,
			wantLen:  1,
			wantStrs: []string{"hello world"},
		},
		// Three interpolations
		{
			name:     "three interpolations",
			pipeline: `"\(.name)-\(.items | length)-\(.single | .name)"`,
			wantLen:  1,
			wantStrs: []string{"test-container-4-solo"},
		},
		// Variable in interpolation
		{
			name:     "variable in interp",
			pipeline: `"hello" as $g | "\($g) world"`,
			wantLen:  1,
			wantStrs: []string{"hello world"},
		},
		// Plain string (no interpolation) still works
		{
			name:     "plain string no interp",
			pipeline: `"just a plain string"`,
			wantLen:  1,
			wantStrs: []string{"just a plain string"},
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

// ── String interpolation error cases ────────────────────────────────────

func TestPipelineStringInterpErrors(t *testing.T) {
	containerMD, _ := buildPipeTestDescriptor(t)

	errorCases := []struct {
		name     string
		pipeline string
		wantErr  string
	}{
		{
			name:     "unclosed interpolation",
			pipeline: `"hello \(.name"`,
			wantErr:  "expected",
		},
	}

	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParsePipeline(containerMD, tc.pipeline)
			if err == nil {
				t.Fatalf("expected error for %q, got nil", tc.pipeline)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q should contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func BenchmarkPipelineStringInterp(b *testing.B) {
	containerMD, _ := buildPipeTestDescriptor(b)
	msg := buildPipeTestMsg(b, containerMD)

	p, err := ParsePipeline(containerMD, `"hello \(.name) you have \(.items | length) items"`)
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
