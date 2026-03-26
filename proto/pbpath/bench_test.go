package pbpath

import (
	"testing"

	"github.com/sryoya/protorand"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// ══════════════════════════════════════════════════════════════════════
// helpers
// ══════════════════════════════════════════════════════════════════════

// benchTestMD builds the testmessage descriptor inline:
//
//	message Nested {
//	  string stringfield = 2;
//	  int32  intfield    = 3;
//	  bytes  bytesfield  = 5;
//	  Test   nested      = 4;  // back-reference
//	}
//	message Test {
//	  Nested         nested       = 1;
//	  repeated Nested repeats     = 2;
//	  repeated int32  int32repeats = 3;
//	}
func benchTestMD(b *testing.B) protoreflect.MessageDescriptor {
	b.Helper()
	stringT := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	int32T := descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum()
	msgT := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	bytesT := descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()
	optL := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	repL := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("bench.proto"),
		Package: proto.String("pbpath.bench"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Nested"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("stringfield"), Number: proto.Int32(2), Type: stringT, Label: optL},
					{Name: proto.String("intfield"), Number: proto.Int32(3), Type: int32T, Label: optL},
					{Name: proto.String("bytesfield"), Number: proto.Int32(5), Type: bytesT, Label: optL},
					{Name: proto.String("nested"), Number: proto.Int32(4), Type: msgT, TypeName: proto.String(".pbpath.bench.Test"), Label: optL},
				},
			},
			{
				Name: proto.String("Test"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("nested"), Number: proto.Int32(1), Type: msgT, TypeName: proto.String(".pbpath.bench.Nested"), Label: optL},
					{Name: proto.String("repeats"), Number: proto.Int32(2), Type: msgT, TypeName: proto.String(".pbpath.bench.Nested"), Label: repL},
					{Name: proto.String("int32repeats"), Number: proto.Int32(3), Type: int32T, Label: repL},
				},
			},
		},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		b.Fatalf("protodesc.NewFile: %v", err)
	}
	return fd.Messages().ByName("Test")
}

func benchGenerateMessage(b *testing.B, md protoreflect.MessageDescriptor) proto.Message {
	b.Helper()
	pr := protorand.New()
	pr.Seed(42)
	pr.MaxCollectionElements = 3
	pr.MaxDepth = 4
	msg, err := pr.NewDynamicProtoRand(md)
	if err != nil {
		b.Fatalf("protorand: %v", err)
	}
	return msg
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: ParsePath
// ══════════════════════════════════════════════════════════════════════

func BenchmarkParsePath(b *testing.B) {
	md := benchTestMD(b)
	b.Run("scalar", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = ParsePath(md, "nested.stringfield")
		}
	})
	b.Run("deep_scalar", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = ParsePath(md, "nested.nested.nested.stringfield")
		}
	})
	b.Run("wildcard", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = ParsePath(md, "repeats[*].nested.stringfield")
		}
	})
	b.Run("slice", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = ParsePath(md, "repeats[0:2:1].nested.stringfield")
		}
	})
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: PathValues
// ══════════════════════════════════════════════════════════════════════

func BenchmarkPathValues(b *testing.B) {
	md := benchTestMD(b)
	msg := benchGenerateMessage(b, md)

	b.Run("scalar", func(b *testing.B) {
		path, err := ParsePath(md, "nested.stringfield")
		if err != nil {
			b.Fatalf("ParsePath: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, _ = PathValues(path, msg)
		}
	})
	b.Run("wildcard_fanout", func(b *testing.B) {
		path, err := ParsePath(md, "repeats[*].stringfield")
		if err != nil {
			b.Fatalf("ParsePath: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, _ = PathValues(path, msg)
		}
	})
	b.Run("nested_wildcard", func(b *testing.B) {
		// repeats[*] → Nested, .nested → Test, .repeats[*] → Nested, .stringfield
		path, err := ParsePath(md, "repeats[*].nested.repeats[*].stringfield")
		if err != nil {
			b.Fatalf("ParsePath: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, _ = PathValues(path, msg)
		}
	})
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: Plan.Eval
// ══════════════════════════════════════════════════════════════════════

func BenchmarkPlanEval(b *testing.B) {
	md := benchTestMD(b)
	msg := benchGenerateMessage(b, md)

	b.Run("single_scalar", func(b *testing.B) {
		plan, err := NewPlan(md, nil,
			PlanPath("nested.stringfield", Alias("greeting")),
		)
		if err != nil {
			b.Fatalf("NewPlan: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, _ = plan.Eval(msg)
		}
	})
	b.Run("5_paths_mixed", func(b *testing.B) {
		plan, err := NewPlan(md, nil,
			PlanPath("nested.stringfield", Alias("str")),
			PlanPath("nested.intfield", Alias("num")),
			PlanPath("repeats[*].stringfield", Alias("rep_str")),
			PlanPath("repeats[*].intfield", Alias("rep_num")),
			PlanPath("int32repeats[0]", Alias("first_int")),
		)
		if err != nil {
			b.Fatalf("NewPlan: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, _ = plan.Eval(msg)
		}
	})
	b.Run("shared_prefix_fanout", func(b *testing.B) {
		plan, err := NewPlan(md, nil,
			PlanPath("repeats[*].stringfield", Alias("str")),
			PlanPath("repeats[*].intfield", Alias("num")),
			PlanPath("repeats[*].bytesfield", Alias("bytes")),
			PlanPath("repeats[*].nested.repeats[*].stringfield", Alias("deep")),
		)
		if err != nil {
			b.Fatalf("NewPlan: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, _ = plan.Eval(msg)
		}
	})
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: Plan.EvalLeaves
// ══════════════════════════════════════════════════════════════════════

func BenchmarkEvalLeaves(b *testing.B) {
	md := benchTestMD(b)
	msg := benchGenerateMessage(b, md)

	b.Run("single_scalar", func(b *testing.B) {
		plan, err := NewPlan(md, nil,
			PlanPath("nested.stringfield", Alias("greeting")),
		)
		if err != nil {
			b.Fatalf("NewPlan: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, _ = plan.EvalLeaves(msg)
		}
	})
	b.Run("5_paths_mixed", func(b *testing.B) {
		plan, err := NewPlan(md, nil,
			PlanPath("nested.stringfield", Alias("str")),
			PlanPath("nested.intfield", Alias("num")),
			PlanPath("repeats[*].stringfield", Alias("rep_str")),
			PlanPath("repeats[*].intfield", Alias("rep_num")),
			PlanPath("int32repeats[0]", Alias("first_int")),
		)
		if err != nil {
			b.Fatalf("NewPlan: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, _ = plan.EvalLeaves(msg)
		}
	})
	b.Run("shared_prefix_fanout", func(b *testing.B) {
		plan, err := NewPlan(md, nil,
			PlanPath("repeats[*].stringfield", Alias("str")),
			PlanPath("repeats[*].intfield", Alias("num")),
			PlanPath("repeats[*].bytesfield", Alias("bytes")),
			PlanPath("repeats[*].nested.repeats[*].stringfield", Alias("deep")),
		)
		if err != nil {
			b.Fatalf("NewPlan: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, _ = plan.EvalLeaves(msg)
		}
	})
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: NewPlan (compile)
// ══════════════════════════════════════════════════════════════════════

func BenchmarkNewPlan(b *testing.B) {
	md := benchTestMD(b)
	b.Run("5_paths", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = NewPlan(md, nil,
				PlanPath("nested.stringfield"),
				PlanPath("nested.intfield"),
				PlanPath("repeats[*].stringfield"),
				PlanPath("repeats[*].intfield"),
				PlanPath("int32repeats[0]"),
			)
		}
	})
}

// ══════════════════════════════════════════════════════════════════════
// Benchmark: EvalLeaves with Wave 3 ETL functions
// ══════════════════════════════════════════════════════════════════════

func BenchmarkEvalLeavesWave3(b *testing.B) {
	md := benchTestMD(b)
	msg := benchGenerateMessage(b, md)

	b.Run("with_hash", func(b *testing.B) {
		plan, err := NewPlan(md, nil,
			PlanPath("h", WithExpr(FuncHash(PathRef("nested.stringfield"))), Alias("h")),
		)
		if err != nil {
			b.Fatalf("NewPlan: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, _ = plan.EvalLeaves(msg)
		}
	})
	b.Run("with_aggregates", func(b *testing.B) {
		plan, err := NewPlan(md, nil,
			PlanPath("s", WithExpr(FuncSum(PathRef("int32repeats[*]"))), Alias("s")),
			PlanPath("d", WithExpr(FuncDistinct(PathRef("int32repeats[*]"))), Alias("d")),
			PlanPath("lc", WithExpr(FuncListConcat(PathRef("repeats[*].stringfield"), ",")), Alias("lc")),
		)
		if err != nil {
			b.Fatalf("NewPlan: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, _ = plan.EvalLeaves(msg)
		}
	})
	b.Run("with_mask", func(b *testing.B) {
		plan, err := NewPlan(md, nil,
			PlanPath("m", WithExpr(FuncMask(PathRef("nested.stringfield"), 2, 2, "*")), Alias("m")),
		)
		if err != nil {
			b.Fatalf("NewPlan: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, _ = plan.EvalLeaves(msg)
		}
	})
	b.Run("with_bucket", func(b *testing.B) {
		plan, err := NewPlan(md, nil,
			PlanPath("b", WithExpr(FuncBucket(PathRef("nested.intfield"), 100)), Alias("b")),
		)
		if err != nil {
			b.Fatalf("NewPlan: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, _ = plan.EvalLeaves(msg)
		}
	})
}
