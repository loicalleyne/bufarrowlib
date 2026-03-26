package pbpath

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"google.golang.org/protobuf/reflect/protodesc"
)

// buildExprTestDescriptors creates an ExprTest message with numeric, string,
// enum, and repeated-string fields suitable for testing all Expr functions.
//
//	enum Status {
//	    UNKNOWN  = 0;
//	    ACTIVE   = 1;
//	    INACTIVE = 2;
//	}
//	message ExprTest {
//	    int64           x       = 1;
//	    int64           y       = 2;
//	    double          dx      = 3;
//	    double          dy      = 4;
//	    string          name    = 5;
//	    string          suffix  = 6;
//	    bool            flag    = 7;
//	    int64           zero    = 8;
//	    repeated int64  nums    = 9;
//	    ExprTest        child   = 10;
//	    Status          status  = 11;
//	    repeated string labels  = 12;
//	}
func buildExprTestDescriptors(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()

	int64Type := descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	doubleType := descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum()
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	boolType := descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum()
	messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	enumType := descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum()
	labelOptional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	labelRepeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("expr_test.proto"),
		Package: proto.String("pbpath.exprtest"),
		Syntax:  proto.String("proto3"),
		EnumType: []*descriptorpb.EnumDescriptorProto{
			{
				Name: proto.String("Status"),
				Value: []*descriptorpb.EnumValueDescriptorProto{
					{Name: proto.String("UNKNOWN"), Number: proto.Int32(0)},
					{Name: proto.String("ACTIVE"), Number: proto.Int32(1)},
					{Name: proto.String("INACTIVE"), Number: proto.Int32(2)},
				},
			},
		},
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("ExprTest"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("x"), Number: proto.Int32(1), Type: int64Type, Label: labelOptional},
					{Name: proto.String("y"), Number: proto.Int32(2), Type: int64Type, Label: labelOptional},
					{Name: proto.String("dx"), Number: proto.Int32(3), Type: doubleType, Label: labelOptional},
					{Name: proto.String("dy"), Number: proto.Int32(4), Type: doubleType, Label: labelOptional},
					{Name: proto.String("name"), Number: proto.Int32(5), Type: stringType, Label: labelOptional},
					{Name: proto.String("suffix"), Number: proto.Int32(6), Type: stringType, Label: labelOptional},
					{Name: proto.String("flag"), Number: proto.Int32(7), Type: boolType, Label: labelOptional},
					{Name: proto.String("zero"), Number: proto.Int32(8), Type: int64Type, Label: labelOptional},
					{Name: proto.String("nums"), Number: proto.Int32(9), Type: int64Type, Label: labelRepeated},
					{
						Name: proto.String("child"), Number: proto.Int32(10),
						Type: messageType, TypeName: proto.String(".pbpath.exprtest.ExprTest"),
						Label: labelOptional,
					},
					{
						Name: proto.String("status"), Number: proto.Int32(11),
						Type: enumType, TypeName: proto.String(".pbpath.exprtest.Status"),
						Label: labelOptional,
					},
					{Name: proto.String("labels"), Number: proto.Int32(12), Type: stringType, Label: labelRepeated},
				},
			},
		},
	}

	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatalf("protodesc.NewFile: %v", err)
	}
	return fd.Messages().ByName("ExprTest")
}

// exprTestMsg builds an ExprTest message with the given field values.
func exprTestMsg(md protoreflect.MessageDescriptor, fields map[string]any) *dynamicpb.Message {
	m := dynamicpb.NewMessage(md)
	for name, val := range fields {
		fd := md.Fields().ByName(protoreflect.Name(name))
		switch v := val.(type) {
		case int:
			m.Set(fd, protoreflect.ValueOfInt64(int64(v)))
		case int64:
			m.Set(fd, protoreflect.ValueOfInt64(v))
		case float64:
			m.Set(fd, protoreflect.ValueOfFloat64(v))
		case string:
			m.Set(fd, protoreflect.ValueOfString(v))
		case bool:
			m.Set(fd, protoreflect.ValueOfBool(v))
		case []int64:
			list := m.Mutable(fd).List()
			for _, n := range v {
				list.Append(protoreflect.ValueOfInt64(n))
			}
		case protoreflect.EnumNumber:
			m.Set(fd, protoreflect.ValueOfEnum(v))
		case []string:
			list := m.Mutable(fd).List()
			for _, s := range v {
				list.Append(protoreflect.ValueOfString(s))
			}
		}
	}
	return m
}

// ---- PathRef ----

func TestPathRef(t *testing.T) {
	e := PathRef("x")
	paths := e.inputPaths()
	if len(paths) != 1 || paths[0] != "x" {
		t.Fatalf("expected [x], got %v", paths)
	}
	if e.outputKind() != 0 {
		t.Fatalf("expected pass-through kind (0), got %v", e.outputKind())
	}
}

// ---- FuncCoalesce ----

func TestFuncCoalesce(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("first non-zero", func(t *testing.T) {
		// zero, x=10, y=20 → should return 10
		msg := exprTestMsg(md, map[string]any{"x": 10, "y": 20})
		plan, err := NewPlan(md, nil,
			PlanPath("coalesced", WithExpr(FuncCoalesce(PathRef("zero"), PathRef("x"), PathRef("y"))), Alias("coalesced")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		if len(result) != 1 || len(result[0]) != 1 {
			t.Fatalf("expected 1 result with 1 branch, got %v", result)
		}
		if got := result[0][0].Int(); got != 10 {
			t.Fatalf("expected 10, got %d", got)
		}
	})

	t.Run("all zero returns invalid", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{})
		plan, err := NewPlan(md, nil,
			PlanPath("coalesced", WithExpr(FuncCoalesce(PathRef("zero"), PathRef("x"))), Alias("coalesced")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		if result[0][0].IsValid() {
			t.Fatalf("expected invalid (null), got %v", result[0][0])
		}
	})
}

// ---- FuncDefault ----

func TestFuncDefault(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("uses value when present", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"x": 42})
		plan, err := NewPlan(md, nil,
			PlanPath("d", WithExpr(FuncDefault(PathRef("x"), protoreflect.ValueOfInt64(99))), Alias("d")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		if got := result[0][0].Int(); got != 42 {
			t.Fatalf("expected 42, got %d", got)
		}
	})

	t.Run("uses literal when zero", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{})
		plan, err := NewPlan(md, nil,
			PlanPath("d", WithExpr(FuncDefault(PathRef("x"), protoreflect.ValueOfInt64(99))), Alias("d")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		if got := result[0][0].Int(); got != 99 {
			t.Fatalf("expected 99, got %d", got)
		}
	})
}

// ---- FuncCond ----

func TestFuncCond(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("true branch", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"flag": true, "x": 10, "y": 20})
		plan, err := NewPlan(md, nil,
			PlanPath("c", WithExpr(FuncCond(PathRef("flag"), PathRef("x"), PathRef("y"))), Alias("c")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		if got := result[0][0].Int(); got != 10 {
			t.Fatalf("expected 10, got %d", got)
		}
	})

	t.Run("false branch", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"flag": false, "x": 10, "y": 20})
		plan, err := NewPlan(md, nil,
			PlanPath("c", WithExpr(FuncCond(PathRef("flag"), PathRef("x"), PathRef("y"))), Alias("c")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		if got := result[0][0].Int(); got != 20 {
			t.Fatalf("expected 20, got %d", got)
		}
	})
}

// ---- FuncHas ----

func TestFuncHas(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("field set", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"x": 42})
		plan, err := NewPlan(md, nil,
			PlanPath("h", WithExpr(FuncHas(PathRef("x"))), Alias("h")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		if got := result[0][0].Bool(); !got {
			t.Fatalf("expected true, got false")
		}
		if plan.Entries()[0].OutputKind != protoreflect.BoolKind {
			t.Fatalf("expected BoolKind, got %v", plan.Entries()[0].OutputKind)
		}
	})

	t.Run("field zero", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{})
		plan, err := NewPlan(md, nil,
			PlanPath("h", WithExpr(FuncHas(PathRef("x"))), Alias("h")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		if got := result[0][0].Bool(); got {
			t.Fatalf("expected false, got true")
		}
	})
}

// ---- FuncLen ----

func TestFuncLen(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("string length", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"name": "hello"})
		plan, err := NewPlan(md, nil,
			PlanPath("l", WithExpr(FuncLen(PathRef("name"))), Alias("l")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		if got := result[0][0].Int(); got != 5 {
			t.Fatalf("expected 5, got %d", got)
		}
		if plan.Entries()[0].OutputKind != protoreflect.Int64Kind {
			t.Fatalf("expected Int64Kind, got %v", plan.Entries()[0].OutputKind)
		}
	})

	t.Run("empty string", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{})
		plan, err := NewPlan(md, nil,
			PlanPath("l", WithExpr(FuncLen(PathRef("name"))), Alias("l")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		if got := result[0][0].Int(); got != 0 {
			t.Fatalf("expected 0, got %d", got)
		}
	})
}

// ---- Arithmetic ----

func TestFuncArith(t *testing.T) {
	md := buildExprTestDescriptors(t)

	tests := []struct {
		name   string
		expr   Expr
		fields map[string]any
		want   int64
	}{
		{"add_int", FuncAdd(PathRef("x"), PathRef("y")), map[string]any{"x": 10, "y": 3}, 13},
		{"sub_int", FuncSub(PathRef("x"), PathRef("y")), map[string]any{"x": 10, "y": 3}, 7},
		{"mul_int", FuncMul(PathRef("x"), PathRef("y")), map[string]any{"x": 10, "y": 3}, 30},
		{"div_int", FuncDiv(PathRef("x"), PathRef("y")), map[string]any{"x": 10, "y": 3}, 3}, // truncating
		{"mod_int", FuncMod(PathRef("x"), PathRef("y")), map[string]any{"x": 10, "y": 3}, 1},
		{"div_by_zero", FuncDiv(PathRef("x"), PathRef("zero")), map[string]any{"x": 10}, 0},
		{"mod_by_zero", FuncMod(PathRef("x"), PathRef("zero")), map[string]any{"x": 10}, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := exprTestMsg(md, tc.fields)
			plan, err := NewPlan(md, nil,
				PlanPath("r", WithExpr(tc.expr), Alias("r")),
			)
			if err != nil {
				t.Fatal(err)
			}
			result, err := plan.EvalLeaves(msg)
			if err != nil {
				t.Fatal(err)
			}
			if got := result[0][0].Int(); got != tc.want {
				t.Fatalf("expected %d, got %d", tc.want, got)
			}
		})
	}

	t.Run("null propagation", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"x": 10})
		// y is zero → int zero, not null. But if we add null child...
		// Actually in proto3, zero-valued int is a valid value (0).
		// To get a truly invalid value we'd need an optional field or
		// a missing message path. Let's test with a path that produces
		// invalid (non-existent child path).
		plan, err := NewPlan(md, nil,
			PlanPath("r", WithExpr(FuncAdd(PathRef("x"), PathRef("child.x"))), Alias("r")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		// child.x on a message with no child set: proto3 returns default (0)
		// for scalar fields on default message instances, which is valid.
		// So this should produce 10 + 0 = 10.
		if got := result[0][0].Int(); got != 10 {
			t.Fatalf("expected 10, got %d", got)
		}
	})
}

func TestFuncArithFloat(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("float add", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"dx": 1.5, "dy": 2.5})
		plan, err := NewPlan(md, nil,
			PlanPath("r", WithExpr(FuncAdd(PathRef("dx"), PathRef("dy"))), Alias("r")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		if got := result[0][0].Float(); got != 4.0 {
			t.Fatalf("expected 4.0, got %f", got)
		}
	})

	t.Run("mixed int+float promotion", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"x": 3, "dx": 1.5})
		plan, err := NewPlan(md, nil,
			PlanPath("r", WithExpr(FuncAdd(PathRef("x"), PathRef("dx"))), Alias("r")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		if got := result[0][0].Float(); got != 4.5 {
			t.Fatalf("expected 4.5, got %f", got)
		}
	})

	t.Run("float div by zero", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"dx": 3.0})
		plan, err := NewPlan(md, nil,
			PlanPath("r", WithExpr(FuncDiv(PathRef("dx"), PathRef("dy"))), Alias("r")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		if got := result[0][0].Float(); got != 0 {
			t.Fatalf("expected 0, got %f", got)
		}
	})
}

// ---- FuncConcat ----

func TestFuncConcat(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("two strings with separator", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"name": "hello", "suffix": "world"})
		plan, err := NewPlan(md, nil,
			PlanPath("c", WithExpr(FuncConcat("-", PathRef("name"), PathRef("suffix"))), Alias("c")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		if got := result[0][0].String(); got != "hello-world" {
			t.Fatalf("expected %q, got %q", "hello-world", got)
		}
		if plan.Entries()[0].OutputKind != protoreflect.StringKind {
			t.Fatalf("expected StringKind, got %v", plan.Entries()[0].OutputKind)
		}
	})

	t.Run("empty separator", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"name": "foo", "suffix": "bar"})
		plan, err := NewPlan(md, nil,
			PlanPath("c", WithExpr(FuncConcat("", PathRef("name"), PathRef("suffix"))), Alias("c")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		if got := result[0][0].String(); got != "foobar" {
			t.Fatalf("expected %q, got %q", "foobar", got)
		}
	})

	t.Run("mixed types", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"name": "val", "x": 42})
		plan, err := NewPlan(md, nil,
			PlanPath("c", WithExpr(FuncConcat("=", PathRef("name"), PathRef("x"))), Alias("c")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		if got := result[0][0].String(); got != "val=42" {
			t.Fatalf("expected %q, got %q", "val=42", got)
		}
	})
}

// ---- Composition: nested Expr trees ----

func TestExprComposition(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("coalesce of add and default", func(t *testing.T) {
		// Coalesce(Add(zero, zero), Default(zero, 99)) → 99
		msg := exprTestMsg(md, map[string]any{})
		expr := FuncCoalesce(
			FuncAdd(PathRef("zero"), PathRef("zero")),                   // 0+0 = 0 (zero → not valid for coalesce)
			FuncDefault(PathRef("zero"), protoreflect.ValueOfInt64(99)), // → 99
		)
		plan, err := NewPlan(md, nil,
			PlanPath("r", WithExpr(expr), Alias("r")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		if got := result[0][0].Int(); got != 99 {
			t.Fatalf("expected 99, got %d", got)
		}
	})

	t.Run("cond with arithmetic branches", func(t *testing.T) {
		// Cond(flag, Add(x,y), Mul(x,y))
		msg := exprTestMsg(md, map[string]any{"flag": true, "x": 3, "y": 4})
		expr := FuncCond(PathRef("flag"), FuncAdd(PathRef("x"), PathRef("y")), FuncMul(PathRef("x"), PathRef("y")))
		plan, err := NewPlan(md, nil,
			PlanPath("r", WithExpr(expr), Alias("r")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		if got := result[0][0].Int(); got != 7 {
			t.Fatalf("expected 7 (3+4), got %d", got)
		}

		// Now with flag=false → 3*4=12
		msg2 := exprTestMsg(md, map[string]any{"flag": false, "x": 3, "y": 4})
		result2, err := plan.EvalLeaves(msg2)
		if err != nil {
			t.Fatal(err)
		}
		if got := result2[0][0].Int(); got != 12 {
			t.Fatalf("expected 12 (3*4), got %d", got)
		}
	})

	t.Run("concat of arithmetic result", func(t *testing.T) {
		// Concat(":", name, Add(x,y))
		msg := exprTestMsg(md, map[string]any{"name": "sum", "x": 3, "y": 4})
		expr := FuncConcat(":", PathRef("name"), FuncAdd(PathRef("x"), PathRef("y")))
		plan, err := NewPlan(md, nil,
			PlanPath("r", WithExpr(expr), Alias("r")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		// FuncAdd returns int64, Concat stringifies → "sum:7"
		if got := result[0][0].String(); got != "sum:7" {
			t.Fatalf("expected %q, got %q", "sum:7", got)
		}
	})
}

// ---- Multiple Expr entries + shared leaf dedup ----

func TestExprSharedLeafDedup(t *testing.T) {
	md := buildExprTestDescriptors(t)

	msg := exprTestMsg(md, map[string]any{"x": 5, "y": 3})
	plan, err := NewPlan(md, nil,
		PlanPath("sum", WithExpr(FuncAdd(PathRef("x"), PathRef("y"))), Alias("sum")),
		PlanPath("diff", WithExpr(FuncSub(PathRef("x"), PathRef("y"))), Alias("diff")),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Only 2 user-visible entries, but x and y are shared.
	entries := plan.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	result, err := plan.EvalLeaves(msg)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 result slots, got %d", len(result))
	}
	if got := result[0][0].Int(); got != 8 {
		t.Fatalf("sum: expected 8, got %d", got)
	}
	if got := result[1][0].Int(); got != 2 {
		t.Fatalf("diff: expected 2, got %d", got)
	}
}

// ---- Expr with raw path entries side by side ----

func TestExprMixedWithRawPaths(t *testing.T) {
	md := buildExprTestDescriptors(t)

	msg := exprTestMsg(md, map[string]any{"x": 10, "y": 3, "name": "hello"})
	plan, err := NewPlan(md, nil,
		PlanPath("name"), // raw path
		PlanPath("sum", WithExpr(FuncAdd(PathRef("x"), PathRef("y"))), Alias("sum")), // expr
		PlanPath("x"), // raw path (shared with expr leaf)
	)
	if err != nil {
		t.Fatal(err)
	}

	entries := plan.Entries()
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	result, err := plan.EvalLeaves(msg)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 result slots, got %d", len(result))
	}

	// Entry 0: raw "name" → "hello"
	if got := result[0][0].String(); got != "hello" {
		t.Fatalf("name: expected %q, got %q", "hello", got)
	}
	// Entry 1: expr Add(x,y) → 13
	if got := result[1][0].Int(); got != 13 {
		t.Fatalf("sum: expected 13, got %d", got)
	}
	// Entry 2: raw "x" → 10
	if got := result[2][0].Int(); got != 10 {
		t.Fatalf("x: expected 10, got %d", got)
	}
}

// ---- EvalLeavesConcurrent ----

func TestExprEvalLeavesConcurrent(t *testing.T) {
	md := buildExprTestDescriptors(t)

	plan, err := NewPlan(md, nil,
		PlanPath("r", WithExpr(FuncAdd(PathRef("x"), PathRef("y"))), Alias("r")),
	)
	if err != nil {
		t.Fatal(err)
	}

	msg := exprTestMsg(md, map[string]any{"x": 7, "y": 8})
	result, err := plan.EvalLeavesConcurrent(msg)
	if err != nil {
		t.Fatal(err)
	}
	if got := result[0][0].Int(); got != 15 {
		t.Fatalf("expected 15, got %d", got)
	}
}

// ---- inputPaths collection ----

func TestInputPaths(t *testing.T) {
	expr := FuncConcat("-",
		PathRef("name"),
		FuncAdd(PathRef("x"), PathRef("y")),
		FuncDefault(PathRef("suffix"), protoreflect.ValueOfString("none")),
	)
	paths := expr.inputPaths()
	want := []string{"name", "x", "y", "suffix"}
	if len(paths) != len(want) {
		t.Fatalf("expected %v, got %v", want, paths)
	}
	for i, p := range paths {
		if p != want[i] {
			t.Fatalf("index %d: expected %q, got %q", i, want[i], p)
		}
	}
}

// ---- isNonZero ----

func TestIsNonZero(t *testing.T) {
	tests := []struct {
		name string
		val  protoreflect.Value
		want bool
	}{
		{"invalid", protoreflect.Value{}, false},
		{"zero_int", protoreflect.ValueOfInt64(0), false},
		{"nonzero_int", protoreflect.ValueOfInt64(42), true},
		{"zero_float", protoreflect.ValueOfFloat64(0), false},
		{"nonzero_float", protoreflect.ValueOfFloat64(1.5), true},
		{"empty_string", protoreflect.ValueOfString(""), false},
		{"nonempty_string", protoreflect.ValueOfString("x"), true},
		{"false_bool", protoreflect.ValueOfBool(false), false},
		{"true_bool", protoreflect.ValueOfBool(true), true},
		{"zero_uint", protoreflect.ValueOfUint64(0), false},
		{"nonzero_uint", protoreflect.ValueOfUint64(1), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isNonZero(tc.val); got != tc.want {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
		})
	}
}

// ---- valueToString ----

func TestValueToString(t *testing.T) {
	tests := []struct {
		name string
		val  protoreflect.Value
		want string
	}{
		{"string", protoreflect.ValueOfString("hello"), "hello"},
		{"int64", protoreflect.ValueOfInt64(42), "42"},
		{"float64", protoreflect.ValueOfFloat64(3.14), "3.14"},
		{"bool_true", protoreflect.ValueOfBool(true), "true"},
		{"bool_false", protoreflect.ValueOfBool(false), "false"},
		{"uint64", protoreflect.ValueOfUint64(100), "100"},
		{"int32", protoreflect.ValueOfInt32(7), "7"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := valueToString(tc.val); got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}
}

// ---- resolvePathExprs ----

func TestResolvePathExprs(t *testing.T) {
	expr := FuncCond(
		PathRef("flag"),
		FuncAdd(PathRef("x"), PathRef("y")),
		PathRef("zero"),
	)
	leaves := resolvePathExprs(expr)
	if len(leaves) != 4 {
		t.Fatalf("expected 4 leaves, got %d", len(leaves))
	}
	want := []string{"flag", "x", "y", "zero"}
	for i, leaf := range leaves {
		if leaf.path != want[i] {
			t.Fatalf("index %d: expected %q, got %q", i, want[i], leaf.path)
		}
	}
}

// ---- WithExpr parse error ----

func TestWithExprBadPath(t *testing.T) {
	md := buildExprTestDescriptors(t)

	_, err := NewPlan(md, nil,
		PlanPath("r", WithExpr(FuncAdd(PathRef("x"), PathRef("no_such_field"))), Alias("r")),
	)
	if err == nil {
		t.Fatal("expected parse error for bad leaf path")
	}
	if !strings.Contains(err.Error(), "no_such_field") {
		t.Fatalf("error should mention bad path: %v", err)
	}
}

// ════════════════════════════════════════════════════════════════════════
// Wave 2 — Predicate functions
// ════════════════════════════════════════════════════════════════════════

func TestFuncPredicates(t *testing.T) {
	md := buildExprTestDescriptors(t)

	tests := []struct {
		name   string
		expr   Expr
		fields map[string]any
		want   bool
	}{
		// int comparisons
		{"eq_int_true", FuncEq(PathRef("x"), PathRef("y")), map[string]any{"x": 5, "y": 5}, true},
		{"eq_int_false", FuncEq(PathRef("x"), PathRef("y")), map[string]any{"x": 5, "y": 3}, false},
		{"ne_int_true", FuncNe(PathRef("x"), PathRef("y")), map[string]any{"x": 5, "y": 3}, true},
		{"ne_int_false", FuncNe(PathRef("x"), PathRef("y")), map[string]any{"x": 5, "y": 5}, false},
		{"lt_int_true", FuncLt(PathRef("x"), PathRef("y")), map[string]any{"x": 3, "y": 5}, true},
		{"lt_int_false", FuncLt(PathRef("x"), PathRef("y")), map[string]any{"x": 5, "y": 3}, false},
		{"le_int_eq", FuncLe(PathRef("x"), PathRef("y")), map[string]any{"x": 5, "y": 5}, true},
		{"le_int_less", FuncLe(PathRef("x"), PathRef("y")), map[string]any{"x": 3, "y": 5}, true},
		{"le_int_greater", FuncLe(PathRef("x"), PathRef("y")), map[string]any{"x": 7, "y": 5}, false},
		{"gt_int_true", FuncGt(PathRef("x"), PathRef("y")), map[string]any{"x": 5, "y": 3}, true},
		{"gt_int_false", FuncGt(PathRef("x"), PathRef("y")), map[string]any{"x": 3, "y": 5}, false},
		{"ge_int_eq", FuncGe(PathRef("x"), PathRef("y")), map[string]any{"x": 5, "y": 5}, true},
		{"ge_int_greater", FuncGe(PathRef("x"), PathRef("y")), map[string]any{"x": 7, "y": 5}, true},
		{"ge_int_less", FuncGe(PathRef("x"), PathRef("y")), map[string]any{"x": 3, "y": 5}, false},
		// string comparisons
		{"eq_str_true", FuncEq(PathRef("name"), PathRef("suffix")), map[string]any{"name": "abc", "suffix": "abc"}, true},
		{"eq_str_false", FuncEq(PathRef("name"), PathRef("suffix")), map[string]any{"name": "abc", "suffix": "xyz"}, false},
		{"lt_str", FuncLt(PathRef("name"), PathRef("suffix")), map[string]any{"name": "abc", "suffix": "xyz"}, true},
		{"gt_str", FuncGt(PathRef("name"), PathRef("suffix")), map[string]any{"name": "xyz", "suffix": "abc"}, true},
		// float comparisons
		{"lt_float", FuncLt(PathRef("dx"), PathRef("dy")), map[string]any{"dx": 1.5, "dy": 2.5}, true},
		// mixed int/float promotion
		{"eq_mixed_true", FuncEq(PathRef("x"), PathRef("dx")), map[string]any{"x": 3, "dx": 3.0}, true},
		{"lt_mixed", FuncLt(PathRef("x"), PathRef("dx")), map[string]any{"x": 2, "dx": 2.5}, true},
		// zero comparisons
		{"eq_zero", FuncEq(PathRef("zero"), PathRef("zero")), map[string]any{}, true},
		{"ne_zero", FuncNe(PathRef("zero"), PathRef("x")), map[string]any{"x": 1}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := exprTestMsg(md, tc.fields)
			plan, err := NewPlan(md, nil,
				PlanPath("r", WithExpr(tc.expr), Alias("r")),
			)
			if err != nil {
				t.Fatal(err)
			}
			result, err := plan.EvalLeaves(msg)
			if err != nil {
				t.Fatal(err)
			}
			if got := result[0][0].Bool(); got != tc.want {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
		})
	}

	t.Run("output_kind", func(t *testing.T) {
		plan, err := NewPlan(md, nil,
			PlanPath("r", WithExpr(FuncEq(PathRef("x"), PathRef("y"))), Alias("r")),
		)
		if err != nil {
			t.Fatal(err)
		}
		if plan.Entries()[0].OutputKind != protoreflect.BoolKind {
			t.Fatalf("expected BoolKind, got %v", plan.Entries()[0].OutputKind)
		}
	})
}

// ════════════════════════════════════════════════════════════════════════
// Wave 2 — String functions
// ════════════════════════════════════════════════════════════════════════

func TestFuncStringFunctions(t *testing.T) {
	md := buildExprTestDescriptors(t)

	tests := []struct {
		name   string
		expr   Expr
		fields map[string]any
		want   string
	}{
		{"upper", FuncUpper(PathRef("name")), map[string]any{"name": "hello"}, "HELLO"},
		{"lower", FuncLower(PathRef("name")), map[string]any{"name": "HELLO"}, "hello"},
		{"trim", FuncTrim(PathRef("name")), map[string]any{"name": "  hi  "}, "hi"},
		{"trim_prefix", FuncTrimPrefix(PathRef("name"), "pre_"), map[string]any{"name": "pre_value"}, "value"},
		{"trim_prefix_no_match", FuncTrimPrefix(PathRef("name"), "xxx"), map[string]any{"name": "hello"}, "hello"},
		{"trim_suffix", FuncTrimSuffix(PathRef("name"), "_suf"), map[string]any{"name": "value_suf"}, "value"},
		{"trim_suffix_no_match", FuncTrimSuffix(PathRef("name"), "xxx"), map[string]any{"name": "hello"}, "hello"},
		{"upper_empty", FuncUpper(PathRef("name")), map[string]any{}, ""},
		{"lower_empty", FuncLower(PathRef("name")), map[string]any{}, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := exprTestMsg(md, tc.fields)
			plan, err := NewPlan(md, nil,
				PlanPath("r", WithExpr(tc.expr), Alias("r")),
			)
			if err != nil {
				t.Fatal(err)
			}
			result, err := plan.EvalLeaves(msg)
			if err != nil {
				t.Fatal(err)
			}
			if got := result[0][0].String(); got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}

	t.Run("output_kind", func(t *testing.T) {
		plan, err := NewPlan(md, nil,
			PlanPath("r", WithExpr(FuncUpper(PathRef("name"))), Alias("r")),
		)
		if err != nil {
			t.Fatal(err)
		}
		if plan.Entries()[0].OutputKind != protoreflect.StringKind {
			t.Fatalf("expected StringKind, got %v", plan.Entries()[0].OutputKind)
		}
	})
}

// ════════════════════════════════════════════════════════════════════════
// Wave 2 — Math functions
// ════════════════════════════════════════════════════════════════════════

func TestFuncMathUnary(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("abs_int_positive", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"x": 5})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncAbs(PathRef("x"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Int(); got != 5 {
			t.Fatalf("expected 5, got %d", got)
		}
	})

	t.Run("abs_int_negative", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"x": -7})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncAbs(PathRef("x"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Int(); got != 7 {
			t.Fatalf("expected 7, got %d", got)
		}
	})

	t.Run("abs_float", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"dx": -3.14})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncAbs(PathRef("dx"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Float(); got != 3.14 {
			t.Fatalf("expected 3.14, got %f", got)
		}
	})

	t.Run("ceil_float", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"dx": 2.3})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncCeil(PathRef("dx"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Float(); got != 3.0 {
			t.Fatalf("expected 3.0, got %f", got)
		}
	})

	t.Run("floor_float", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"dx": 2.7})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncFloor(PathRef("dx"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Float(); got != 2.0 {
			t.Fatalf("expected 2.0, got %f", got)
		}
	})

	t.Run("round_float_even", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"dx": 2.5})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncRound(PathRef("dx"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		// Banker's rounding: 2.5 → 2.0
		if got := result[0][0].Float(); got != 2.0 {
			t.Fatalf("expected 2.0, got %f", got)
		}
	})

	t.Run("round_float_odd", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"dx": 3.5})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncRound(PathRef("dx"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		// Banker's rounding: 3.5 → 4.0
		if got := result[0][0].Float(); got != 4.0 {
			t.Fatalf("expected 4.0, got %f", got)
		}
	})

	t.Run("ceil_int_noop", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"x": 5})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncCeil(PathRef("x"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Int(); got != 5 {
			t.Fatalf("expected 5, got %d", got)
		}
	})

	t.Run("abs_invalid", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"name": "text"})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncAbs(PathRef("name"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if result[0][0].IsValid() {
			t.Fatalf("expected invalid for non-numeric, got %v", result[0][0])
		}
	})
}

func TestFuncMinMax(t *testing.T) {
	md := buildExprTestDescriptors(t)

	tests := []struct {
		name   string
		expr   Expr
		fields map[string]any
		want   int64
	}{
		{"min_int", FuncMin(PathRef("x"), PathRef("y")), map[string]any{"x": 10, "y": 3}, 3},
		{"max_int", FuncMax(PathRef("x"), PathRef("y")), map[string]any{"x": 10, "y": 3}, 10},
		{"min_equal", FuncMin(PathRef("x"), PathRef("y")), map[string]any{"x": 5, "y": 5}, 5},
		{"max_equal", FuncMax(PathRef("x"), PathRef("y")), map[string]any{"x": 5, "y": 5}, 5},
		{"min_negative", FuncMin(PathRef("x"), PathRef("y")), map[string]any{"x": -3, "y": 3}, -3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := exprTestMsg(md, tc.fields)
			plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(tc.expr), Alias("r")))
			result, _ := plan.EvalLeaves(msg)
			if got := result[0][0].Int(); got != tc.want {
				t.Fatalf("expected %d, got %d", tc.want, got)
			}
		})
	}

	t.Run("min_float", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"dx": 1.5, "dy": 2.5})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncMin(PathRef("dx"), PathRef("dy"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Float(); got != 1.5 {
			t.Fatalf("expected 1.5, got %f", got)
		}
	})

	t.Run("max_mixed_promotion", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"x": 2, "dx": 2.5})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncMax(PathRef("x"), PathRef("dx"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Float(); got != 2.5 {
			t.Fatalf("expected 2.5, got %f", got)
		}
	})
}

// ════════════════════════════════════════════════════════════════════════
// Wave 2 — Cast functions
// ════════════════════════════════════════════════════════════════════════

func TestFuncCast(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("cast_int_from_float", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"dx": 3.7})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncCastInt(PathRef("dx"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Int(); got != 3 {
			t.Fatalf("expected 3 (truncated), got %d", got)
		}
	})

	t.Run("cast_int_from_string", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"name": "42"})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncCastInt(PathRef("name"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Int(); got != 42 {
			t.Fatalf("expected 42, got %d", got)
		}
	})

	t.Run("cast_int_from_string_float", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"name": "3.9"})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncCastInt(PathRef("name"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Int(); got != 3 {
			t.Fatalf("expected 3 (truncated from 3.9), got %d", got)
		}
	})

	t.Run("cast_int_from_bool", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"flag": true})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncCastInt(PathRef("flag"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Int(); got != 1 {
			t.Fatalf("expected 1, got %d", got)
		}
	})

	t.Run("cast_int_unparseable", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"name": "not_a_number"})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncCastInt(PathRef("name"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if result[0][0].IsValid() {
			t.Fatalf("expected invalid, got %v", result[0][0])
		}
	})

	t.Run("cast_float_from_int", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"x": 7})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncCastFloat(PathRef("x"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Float(); got != 7.0 {
			t.Fatalf("expected 7.0, got %f", got)
		}
	})

	t.Run("cast_float_from_string", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"name": "3.14"})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncCastFloat(PathRef("name"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Float(); got != 3.14 {
			t.Fatalf("expected 3.14, got %f", got)
		}
	})

	t.Run("cast_string_from_int", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"x": 42})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncCastString(PathRef("x"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].String(); got != "42" {
			t.Fatalf("expected %q, got %q", "42", got)
		}
	})

	t.Run("cast_string_from_float", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"dx": 3.14})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncCastString(PathRef("dx"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].String(); got != "3.14" {
			t.Fatalf("expected %q, got %q", "3.14", got)
		}
	})

	t.Run("cast_string_from_bool", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"flag": true})
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncCastString(PathRef("flag"))), Alias("r")))
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].String(); got != "true" {
			t.Fatalf("expected %q, got %q", "true", got)
		}
	})

	t.Run("output_kinds", func(t *testing.T) {
		plan, err := NewPlan(md, nil,
			PlanPath("a", WithExpr(FuncCastInt(PathRef("dx"))), Alias("a")),
			PlanPath("b", WithExpr(FuncCastFloat(PathRef("x"))), Alias("b")),
			PlanPath("c", WithExpr(FuncCastString(PathRef("x"))), Alias("c")),
		)
		if err != nil {
			t.Fatal(err)
		}
		entries := plan.Entries()
		if entries[0].OutputKind != protoreflect.Int64Kind {
			t.Fatalf("CastInt: expected Int64Kind, got %v", entries[0].OutputKind)
		}
		if entries[1].OutputKind != protoreflect.DoubleKind {
			t.Fatalf("CastFloat: expected DoubleKind, got %v", entries[1].OutputKind)
		}
		if entries[2].OutputKind != protoreflect.StringKind {
			t.Fatalf("CastString: expected StringKind, got %v", entries[2].OutputKind)
		}
	})
}

// ════════════════════════════════════════════════════════════════════════
// Wave 2 — Timestamp functions
// ════════════════════════════════════════════════════════════════════════

func TestFuncStrptime(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("duckdb_format", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"name": "2024-03-15 10:30:00"})
		plan, err := NewPlan(md, nil,
			PlanPath("ts", WithExpr(FuncStrptime("%Y-%m-%d %H:%M:%S", PathRef("name"))), Alias("ts")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		want := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC).UnixMilli()
		if got := result[0][0].Int(); got != want {
			t.Fatalf("expected %d, got %d", want, got)
		}
	})

	t.Run("go_format", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"name": "2024-03-15T10:30:00Z"})
		plan, err := NewPlan(md, nil,
			PlanPath("ts", WithExpr(FuncStrptime("2006-01-02T15:04:05Z", PathRef("name"))), Alias("ts")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		want := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC).UnixMilli()
		if got := result[0][0].Int(); got != want {
			t.Fatalf("expected %d, got %d", want, got)
		}
	})

	t.Run("parse_failure_returns_invalid", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"name": "not-a-date"})
		plan, _ := NewPlan(md, nil,
			PlanPath("ts", WithExpr(FuncStrptime("%Y-%m-%d", PathRef("name"))), Alias("ts")),
		)
		result, _ := plan.EvalLeaves(msg)
		if result[0][0].IsValid() {
			t.Fatalf("expected invalid on parse failure, got %v", result[0][0])
		}
	})

	t.Run("output_kind", func(t *testing.T) {
		plan, _ := NewPlan(md, nil,
			PlanPath("ts", WithExpr(FuncStrptime("%Y", PathRef("name"))), Alias("ts")),
		)
		if plan.Entries()[0].OutputKind != protoreflect.Int64Kind {
			t.Fatalf("expected Int64Kind, got %v", plan.Entries()[0].OutputKind)
		}
	})
}

func TestFuncTryStrptime(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("success", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"name": "2024-01-01"})
		plan, _ := NewPlan(md, nil,
			PlanPath("ts", WithExpr(FuncTryStrptime("%Y-%m-%d", PathRef("name"))), Alias("ts")),
		)
		result, _ := plan.EvalLeaves(msg)
		want := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
		if got := result[0][0].Int(); got != want {
			t.Fatalf("expected %d, got %d", want, got)
		}
	})

	t.Run("failure_returns_zero", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"name": "garbage"})
		plan, _ := NewPlan(md, nil,
			PlanPath("ts", WithExpr(FuncTryStrptime("%Y-%m-%d", PathRef("name"))), Alias("ts")),
		)
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Int(); got != 0 {
			t.Fatalf("expected 0 on failure, got %d", got)
		}
	})
}

func TestFuncAge(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("two_args_diff", func(t *testing.T) {
		// x=10000, y=3000 → age = 7000
		msg := exprTestMsg(md, map[string]any{"x": 10000, "y": 3000})
		plan, _ := NewPlan(md, nil,
			PlanPath("a", WithExpr(FuncAge(PathRef("x"), PathRef("y"))), Alias("a")),
		)
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Int(); got != 7000 {
			t.Fatalf("expected 7000, got %d", got)
		}
	})

	t.Run("one_arg_age", func(t *testing.T) {
		// Use a recent timestamp — the age should be small and positive.
		nowMs := time.Now().UnixMilli()
		msg := exprTestMsg(md, map[string]any{"x": int64(nowMs - 5000)})
		plan, _ := NewPlan(md, nil,
			PlanPath("a", WithExpr(FuncAge(PathRef("x"))), Alias("a")),
		)
		result, _ := plan.EvalLeaves(msg)
		got := result[0][0].Int()
		// Should be around 5000ms, allow ±2000ms for test execution.
		if got < 3000 || got > 7000 {
			t.Fatalf("expected ~5000ms age, got %d", got)
		}
	})
}

func TestFuncExtract(t *testing.T) {
	md := buildExprTestDescriptors(t)

	// 2024-03-15 10:30:45 UTC
	ts := time.Date(2024, 3, 15, 10, 30, 45, 0, time.UTC).UnixMilli()

	tests := []struct {
		name string
		expr Expr
		want int64
	}{
		{"year", FuncExtractYear(PathRef("x")), 2024},
		{"month", FuncExtractMonth(PathRef("x")), 3},
		{"day", FuncExtractDay(PathRef("x")), 15},
		{"hour", FuncExtractHour(PathRef("x")), 10},
		{"minute", FuncExtractMinute(PathRef("x")), 30},
		{"second", FuncExtractSecond(PathRef("x")), 45},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := exprTestMsg(md, map[string]any{"x": ts})
			plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(tc.expr), Alias("r")))
			result, _ := plan.EvalLeaves(msg)
			if got := result[0][0].Int(); got != tc.want {
				t.Fatalf("expected %d, got %d", tc.want, got)
			}
		})
	}

	t.Run("output_kind", func(t *testing.T) {
		plan, _ := NewPlan(md, nil, PlanPath("r", WithExpr(FuncExtractYear(PathRef("x"))), Alias("r")))
		if plan.Entries()[0].OutputKind != protoreflect.Int64Kind {
			t.Fatalf("expected Int64Kind, got %v", plan.Entries()[0].OutputKind)
		}
	})
}

// ════════════════════════════════════════════════════════════════════════
// Wave 2 — compareValues helper
// ════════════════════════════════════════════════════════════════════════

func TestCompareValues(t *testing.T) {
	tests := []struct {
		name string
		a, b protoreflect.Value
		cmp  int
		ok   bool
	}{
		{"int_lt", protoreflect.ValueOfInt64(1), protoreflect.ValueOfInt64(2), -1, true},
		{"int_eq", protoreflect.ValueOfInt64(5), protoreflect.ValueOfInt64(5), 0, true},
		{"int_gt", protoreflect.ValueOfInt64(9), protoreflect.ValueOfInt64(1), 1, true},
		{"str_lt", protoreflect.ValueOfString("abc"), protoreflect.ValueOfString("xyz"), -1, true},
		{"str_eq", protoreflect.ValueOfString("aa"), protoreflect.ValueOfString("aa"), 0, true},
		{"float_lt", protoreflect.ValueOfFloat64(1.0), protoreflect.ValueOfFloat64(2.0), -1, true},
		{"invalid_a", protoreflect.Value{}, protoreflect.ValueOfInt64(1), 0, false},
		{"invalid_b", protoreflect.ValueOfInt64(1), protoreflect.Value{}, 0, false},
		{"mixed_int_float", protoreflect.ValueOfInt64(2), protoreflect.ValueOfFloat64(2.5), -1, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmp, ok := compareValues(tc.a, tc.b)
			if ok != tc.ok {
				t.Fatalf("expected ok=%v, got ok=%v", tc.ok, ok)
			}
			if ok && cmp != tc.cmp {
				t.Fatalf("expected cmp=%d, got cmp=%d", tc.cmp, cmp)
			}
		})
	}
}

// ════════════════════════════════════════════════════════════════════════
// Wave 3 — ETL functions
// ════════════════════════════════════════════════════════════════════════

// ---- FuncHash ----

func TestFuncHash(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("deterministic", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"name": "hello"})
		plan, err := NewPlan(md, nil,
			PlanPath("h", WithExpr(FuncHash(PathRef("name"))), Alias("h")),
		)
		if err != nil {
			t.Fatal(err)
		}
		r1, _ := plan.EvalLeaves(msg)
		r2, _ := plan.EvalLeaves(msg)
		if r1[0][0].Int() != r2[0][0].Int() {
			t.Fatalf("hash not deterministic: %d vs %d", r1[0][0].Int(), r2[0][0].Int())
		}
	})

	t.Run("different_inputs_differ", func(t *testing.T) {
		plan, err := NewPlan(md, nil,
			PlanPath("h", WithExpr(FuncHash(PathRef("name"))), Alias("h")),
		)
		if err != nil {
			t.Fatal(err)
		}
		msg1 := exprTestMsg(md, map[string]any{"name": "hello"})
		msg2 := exprTestMsg(md, map[string]any{"name": "world"})
		r1, _ := plan.EvalLeaves(msg1)
		hash1 := r1[0][0].Int() // capture before scratch is reused
		r2, _ := plan.EvalLeaves(msg2)
		hash2 := r2[0][0].Int()
		if hash1 == hash2 {
			t.Fatalf("expected different hashes for different inputs, both got %d", hash1)
		}
	})

	t.Run("multiple_children", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"name": "a", "suffix": "b"})
		plan, err := NewPlan(md, nil,
			PlanPath("h", WithExpr(FuncHash(PathRef("name"), PathRef("suffix"))), Alias("h")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, _ := plan.EvalLeaves(msg)
		if !result[0][0].IsValid() {
			t.Fatal("expected valid hash")
		}
	})

	t.Run("output_kind", func(t *testing.T) {
		plan, _ := NewPlan(md, nil,
			PlanPath("h", WithExpr(FuncHash(PathRef("name"))), Alias("h")),
		)
		if plan.Entries()[0].OutputKind != protoreflect.Int64Kind {
			t.Fatalf("expected Int64Kind, got %v", plan.Entries()[0].OutputKind)
		}
	})
}

// ---- FuncEpochToDate ----

func TestFuncEpochToDate(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("basic", func(t *testing.T) {
		// 2024-01-15 = day 19737 from epoch (1970-01-01)
		// Unix seconds for 2024-01-15 00:00:00 UTC = 1705276800
		msg := exprTestMsg(md, map[string]any{"x": int64(1705276800)})
		plan, err := NewPlan(md, nil,
			PlanPath("d", WithExpr(FuncEpochToDate(PathRef("x"))), Alias("d")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, _ := plan.EvalLeaves(msg)
		want := int32(1705276800 / 86400) // 19737
		// Use Int() which returns int64, then compare
		if got := int32(result[0][0].Int()); got != want {
			t.Fatalf("expected %d, got %d", want, got)
		}
	})

	t.Run("epoch_zero", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{})
		plan, _ := NewPlan(md, nil,
			PlanPath("d", WithExpr(FuncEpochToDate(PathRef("x"))), Alias("d")),
		)
		result, _ := plan.EvalLeaves(msg)
		if got := int32(result[0][0].Int()); got != 0 {
			t.Fatalf("expected 0, got %d", got)
		}
	})

	t.Run("output_kind", func(t *testing.T) {
		plan, _ := NewPlan(md, nil,
			PlanPath("d", WithExpr(FuncEpochToDate(PathRef("x"))), Alias("d")),
		)
		if plan.Entries()[0].OutputKind != protoreflect.Int32Kind {
			t.Fatalf("expected Int32Kind, got %v", plan.Entries()[0].OutputKind)
		}
	})
}

// ---- FuncDatePart ----

func TestFuncDatePart(t *testing.T) {
	md := buildExprTestDescriptors(t)

	// 2024-03-15 10:30:45 UTC → Unix seconds = 1710495045
	ts := time.Date(2024, 3, 15, 10, 30, 45, 0, time.UTC).Unix()

	tests := []struct {
		name string
		part string
		want int64
	}{
		{"year", "year", 2024},
		{"month", "month", 3},
		{"day", "day", 15},
		{"hour", "hour", 10},
		{"minute", "minute", 30},
		{"second", "second", 45},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := exprTestMsg(md, map[string]any{"x": ts})
			plan, _ := NewPlan(md, nil,
				PlanPath("r", WithExpr(FuncDatePart(tc.part, PathRef("x"))), Alias("r")),
			)
			result, _ := plan.EvalLeaves(msg)
			if got := result[0][0].Int(); got != tc.want {
				t.Fatalf("expected %d, got %d", tc.want, got)
			}
		})
	}

	t.Run("unknown_part", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"x": ts})
		plan, _ := NewPlan(md, nil,
			PlanPath("r", WithExpr(FuncDatePart("weekday", PathRef("x"))), Alias("r")),
		)
		result, _ := plan.EvalLeaves(msg)
		if result[0][0].IsValid() {
			t.Fatalf("expected invalid for unknown part, got %v", result[0][0])
		}
	})

	t.Run("output_kind", func(t *testing.T) {
		plan, _ := NewPlan(md, nil,
			PlanPath("r", WithExpr(FuncDatePart("year", PathRef("x"))), Alias("r")),
		)
		if plan.Entries()[0].OutputKind != protoreflect.Int64Kind {
			t.Fatalf("expected Int64Kind, got %v", plan.Entries()[0].OutputKind)
		}
	})
}

// ---- FuncBucket ----

func TestFuncBucket(t *testing.T) {
	md := buildExprTestDescriptors(t)

	tests := []struct {
		name string
		val  int
		size int
		want int64
	}{
		{"exact_multiple", 100, 10, 100},
		{"floor_down", 107, 10, 100},
		{"small_value", 3, 10, 0},
		{"large_bucket", 999, 1000, 0},
		{"size_1", 42, 1, 42},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := exprTestMsg(md, map[string]any{"x": tc.val})
			plan, _ := NewPlan(md, nil,
				PlanPath("r", WithExpr(FuncBucket(PathRef("x"), tc.size)), Alias("r")),
			)
			result, _ := plan.EvalLeaves(msg)
			if got := result[0][0].Int(); got != tc.want {
				t.Fatalf("expected %d, got %d", tc.want, got)
			}
		})
	}

	t.Run("zero_size_passthrough", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"x": 42})
		plan, _ := NewPlan(md, nil,
			PlanPath("r", WithExpr(FuncBucket(PathRef("x"), 0)), Alias("r")),
		)
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Int(); got != 42 {
			t.Fatalf("expected 42 (passthrough), got %d", got)
		}
	})
}

// ---- FuncMask ----

func TestFuncMask(t *testing.T) {
	md := buildExprTestDescriptors(t)

	tests := []struct {
		name      string
		input     string
		keepFirst int
		keepLast  int
		maskChar  string
		want      string
	}{
		{"basic", "hello world", 2, 2, "*", "he*******ld"},
		{"email_like", "user@example.com", 2, 4, "*", "us**********.com"},
		{"custom_char", "secret", 1, 1, "#", "s####t"},
		{"keep_all", "hi", 1, 1, "*", "hi"},
		{"keep_more_than_len", "ab", 3, 3, "*", "ab"},
		{"empty_mask_char", "hello", 1, 1, "", "h***o"},
		{"all_masked", "hello", 0, 0, "*", "*****"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := exprTestMsg(md, map[string]any{"name": tc.input})
			plan, _ := NewPlan(md, nil,
				PlanPath("r", WithExpr(FuncMask(PathRef("name"), tc.keepFirst, tc.keepLast, tc.maskChar)), Alias("r")),
			)
			result, _ := plan.EvalLeaves(msg)
			if got := result[0][0].String(); got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}

	t.Run("output_kind", func(t *testing.T) {
		plan, _ := NewPlan(md, nil,
			PlanPath("r", WithExpr(FuncMask(PathRef("name"), 1, 1, "*")), Alias("r")),
		)
		if plan.Entries()[0].OutputKind != protoreflect.StringKind {
			t.Fatalf("expected StringKind, got %v", plan.Entries()[0].OutputKind)
		}
	})
}

// ---- FuncCoerce ----

func TestFuncCoerce(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("true_branch", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"flag": true})
		plan, err := NewPlan(md, nil,
			PlanPath("r", WithExpr(FuncCoerce(PathRef("flag"),
				protoreflect.ValueOfString("yes"),
				protoreflect.ValueOfString("no"),
			)), Alias("r")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].String(); got != "yes" {
			t.Fatalf("expected %q, got %q", "yes", got)
		}
	})

	t.Run("false_branch", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"flag": false})
		plan, _ := NewPlan(md, nil,
			PlanPath("r", WithExpr(FuncCoerce(PathRef("flag"),
				protoreflect.ValueOfString("yes"),
				protoreflect.ValueOfString("no"),
			)), Alias("r")),
		)
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].String(); got != "no" {
			t.Fatalf("expected %q, got %q", "no", got)
		}
	})

	t.Run("nonzero_int_is_true", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"x": 42})
		plan, _ := NewPlan(md, nil,
			PlanPath("r", WithExpr(FuncCoerce(PathRef("x"),
				protoreflect.ValueOfString("present"),
				protoreflect.ValueOfString("absent"),
			)), Alias("r")),
		)
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].String(); got != "present" {
			t.Fatalf("expected %q, got %q", "present", got)
		}
	})

	t.Run("zero_int_is_false", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{})
		plan, _ := NewPlan(md, nil,
			PlanPath("r", WithExpr(FuncCoerce(PathRef("x"),
				protoreflect.ValueOfString("present"),
				protoreflect.ValueOfString("absent"),
			)), Alias("r")),
		)
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].String(); got != "absent" {
			t.Fatalf("expected %q, got %q", "absent", got)
		}
	})

	t.Run("output_kind", func(t *testing.T) {
		plan, _ := NewPlan(md, nil,
			PlanPath("r", WithExpr(FuncCoerce(PathRef("flag"),
				protoreflect.ValueOfString("y"), protoreflect.ValueOfString("n"),
			)), Alias("r")),
		)
		if plan.Entries()[0].OutputKind != protoreflect.StringKind {
			t.Fatalf("expected StringKind, got %v", plan.Entries()[0].OutputKind)
		}
	})
}

// ---- FuncEnumName ----

func TestFuncEnumName(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("active", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"status": protoreflect.EnumNumber(1)})
		plan, err := NewPlan(md, nil,
			PlanPath("s", WithExpr(FuncEnumName(PathRef("status"))), Alias("s")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].String(); got != "ACTIVE" {
			t.Fatalf("expected %q, got %q", "ACTIVE", got)
		}
	})

	t.Run("inactive", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"status": protoreflect.EnumNumber(2)})
		plan, _ := NewPlan(md, nil,
			PlanPath("s", WithExpr(FuncEnumName(PathRef("status"))), Alias("s")),
		)
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].String(); got != "INACTIVE" {
			t.Fatalf("expected %q, got %q", "INACTIVE", got)
		}
	})

	t.Run("unknown_zero", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{})
		plan, _ := NewPlan(md, nil,
			PlanPath("s", WithExpr(FuncEnumName(PathRef("status"))), Alias("s")),
		)
		result, _ := plan.EvalLeaves(msg)
		// Enum number 0 → "UNKNOWN"
		if got := result[0][0].String(); got != "UNKNOWN" {
			t.Fatalf("expected %q, got %q", "UNKNOWN", got)
		}
	})

	t.Run("unknown_number", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"status": protoreflect.EnumNumber(99)})
		plan, _ := NewPlan(md, nil,
			PlanPath("s", WithExpr(FuncEnumName(PathRef("status"))), Alias("s")),
		)
		result, _ := plan.EvalLeaves(msg)
		// Unknown enum value → invalid
		if result[0][0].IsValid() {
			t.Fatalf("expected invalid for unknown enum number, got %v", result[0][0])
		}
	})

	t.Run("non_enum_field_error", func(t *testing.T) {
		_, err := NewPlan(md, nil,
			PlanPath("s", WithExpr(FuncEnumName(PathRef("x"))), Alias("s")),
		)
		if err == nil {
			t.Fatal("expected error for FuncEnumName on non-enum field")
		}
		if !strings.Contains(err.Error(), "not an enum") {
			t.Fatalf("error should mention 'not an enum': %v", err)
		}
	})

	t.Run("output_kind", func(t *testing.T) {
		plan, _ := NewPlan(md, nil,
			PlanPath("s", WithExpr(FuncEnumName(PathRef("status"))), Alias("s")),
		)
		if plan.Entries()[0].OutputKind != protoreflect.StringKind {
			t.Fatalf("expected StringKind, got %v", plan.Entries()[0].OutputKind)
		}
	})
}

// ---- FuncSum (aggregate) ----

func TestFuncSum(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("repeated_int_sum", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"nums": []int64{10, 20, 30}})
		plan, err := NewPlan(md, nil,
			PlanPath("s", WithExpr(FuncSum(PathRef("nums[*]"))), Alias("s")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := plan.EvalLeaves(msg)
		if err != nil {
			t.Fatal(err)
		}
		// All branches should report the same sum.
		if got := result[0][0].Int(); got != 60 {
			t.Fatalf("expected 60, got %d", got)
		}
	})

	t.Run("single_value", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"x": 42})
		plan, _ := NewPlan(md, nil,
			PlanPath("s", WithExpr(FuncSum(PathRef("x"))), Alias("s")),
		)
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Int(); got != 42 {
			t.Fatalf("expected 42, got %d", got)
		}
	})

	t.Run("empty_repeated", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{})
		plan, _ := NewPlan(md, nil,
			PlanPath("s", WithExpr(FuncSum(PathRef("nums[*]"))), Alias("s")),
		)
		result, _ := plan.EvalLeaves(msg)
		// No branches → should return invalid or 0
		if result[0][0].IsValid() && result[0][0].Int() != 0 {
			t.Fatalf("expected 0 or invalid for empty, got %v", result[0][0])
		}
	})
}

// ---- FuncDistinct (aggregate) ----

func TestFuncDistinct(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("all_unique", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"nums": []int64{1, 2, 3}})
		plan, err := NewPlan(md, nil,
			PlanPath("d", WithExpr(FuncDistinct(PathRef("nums[*]"))), Alias("d")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Int(); got != 3 {
			t.Fatalf("expected 3, got %d", got)
		}
	})

	t.Run("with_duplicates", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"nums": []int64{1, 2, 1, 3, 2}})
		plan, _ := NewPlan(md, nil,
			PlanPath("d", WithExpr(FuncDistinct(PathRef("nums[*]"))), Alias("d")),
		)
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Int(); got != 3 {
			t.Fatalf("expected 3, got %d", got)
		}
	})

	t.Run("single_value", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"x": 42})
		plan, _ := NewPlan(md, nil,
			PlanPath("d", WithExpr(FuncDistinct(PathRef("x"))), Alias("d")),
		)
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].Int(); got != 1 {
			t.Fatalf("expected 1, got %d", got)
		}
	})

	t.Run("output_kind", func(t *testing.T) {
		plan, _ := NewPlan(md, nil,
			PlanPath("d", WithExpr(FuncDistinct(PathRef("nums[*]"))), Alias("d")),
		)
		if plan.Entries()[0].OutputKind != protoreflect.Int64Kind {
			t.Fatalf("expected Int64Kind, got %v", plan.Entries()[0].OutputKind)
		}
	})
}

// ---- FuncListConcat (aggregate) ----

func TestFuncListConcat(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("repeated_strings", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"labels": []string{"a", "b", "c"}})
		plan, err := NewPlan(md, nil,
			PlanPath("lc", WithExpr(FuncListConcat(PathRef("labels[*]"), ",")), Alias("lc")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].String(); got != "a,b,c" {
			t.Fatalf("expected %q, got %q", "a,b,c", got)
		}
	})

	t.Run("repeated_ints_as_strings", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"nums": []int64{10, 20, 30}})
		plan, _ := NewPlan(md, nil,
			PlanPath("lc", WithExpr(FuncListConcat(PathRef("nums[*]"), "|")), Alias("lc")),
		)
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].String(); got != "10|20|30" {
			t.Fatalf("expected %q, got %q", "10|20|30", got)
		}
	})

	t.Run("empty_separator", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"labels": []string{"x", "y"}})
		plan, _ := NewPlan(md, nil,
			PlanPath("lc", WithExpr(FuncListConcat(PathRef("labels[*]"), "")), Alias("lc")),
		)
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].String(); got != "xy" {
			t.Fatalf("expected %q, got %q", "xy", got)
		}
	})

	t.Run("single_value", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"name": "only"})
		plan, _ := NewPlan(md, nil,
			PlanPath("lc", WithExpr(FuncListConcat(PathRef("name"), ",")), Alias("lc")),
		)
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].String(); got != "only" {
			t.Fatalf("expected %q, got %q", "only", got)
		}
	})

	t.Run("output_kind", func(t *testing.T) {
		plan, _ := NewPlan(md, nil,
			PlanPath("lc", WithExpr(FuncListConcat(PathRef("labels[*]"), ",")), Alias("lc")),
		)
		if plan.Entries()[0].OutputKind != protoreflect.StringKind {
			t.Fatalf("expected StringKind, got %v", plan.Entries()[0].OutputKind)
		}
	})
}

// ---- Wave 3 composition tests ----

func TestWave3Composition(t *testing.T) {
	md := buildExprTestDescriptors(t)

	t.Run("hash_of_masked_value", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"name": "sensitive_data"})
		expr := FuncHash(FuncMask(PathRef("name"), 2, 2, "*"))
		plan, err := NewPlan(md, nil,
			PlanPath("r", WithExpr(expr), Alias("r")),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, _ := plan.EvalLeaves(msg)
		if !result[0][0].IsValid() {
			t.Fatal("expected valid hash of masked value")
		}
	})

	t.Run("coerce_with_enum", func(t *testing.T) {
		// Use Eq to check status, then Coerce
		msg := exprTestMsg(md, map[string]any{"status": protoreflect.EnumNumber(1), "x": int64(1)})
		// x == 1 (matching ACTIVE) → "enabled"
		expr := FuncCoerce(
			FuncEq(PathRef("x"), FuncCastInt(FuncEnumName(PathRef("status")))),
			protoreflect.ValueOfString("match"),
			protoreflect.ValueOfString("no_match"),
		)
		plan, err := NewPlan(md, nil,
			PlanPath("r", WithExpr(expr), Alias("r")),
		)
		if err != nil {
			t.Fatal(err)
		}
		// Note: CastInt("ACTIVE") will fail to parse → invalid → Eq will be false
		// This tests composition even with type mismatch graceful handling
		result, _ := plan.EvalLeaves(msg)
		if !result[0][0].IsValid() {
			t.Fatal("expected valid result")
		}
	})

	t.Run("bucket_then_cast_string", func(t *testing.T) {
		msg := exprTestMsg(md, map[string]any{"x": 157})
		expr := FuncCastString(FuncBucket(PathRef("x"), 50))
		plan, _ := NewPlan(md, nil,
			PlanPath("r", WithExpr(expr), Alias("r")),
		)
		result, _ := plan.EvalLeaves(msg)
		if got := result[0][0].String(); got != "150" {
			t.Fatalf("expected %q, got %q", "150", got)
		}
	})
}
