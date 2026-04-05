package bufarrowlib

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
	"github.com/loicalleyne/bufarrowlib/gen/go/samples"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPMR_Debug(t *testing.T) {
	// Test 1: ScalarTypes WITH float field — triggers the bug
	t.Run("ScalarTypes_WithFloat", func(t *testing.T) {
		msg := new(samples.ScalarTypes)
		msg.Double = 3.14
		msg.Float = 2.71 // This field triggers the bug: proto float→arrow FLOAT32 has no case in AppendValueOrNull
		msg.Int32 = -42
		msg.Int64 = -1234567890
		msg.Uint32 = 42
		msg.Uint64 = 1234567890
		msg.Sint32 = -7
		msg.Sint64 = -8
		msg.Fixed32 = 9
		msg.Fixed64 = 10
		msg.Sfixed32 = -11
		msg.Sfixed64 = -12
		msg.Bool = true
		msg.String_ = "test"
		msg.Bytes = []byte{0xDE, 0xAD}

		pmr := arrowutil.NewProtobufMessageReflection(msg)
		schema := pmr.Schema()
		t.Logf("Schema fields: %d", schema.NumFields())
		for i := 0; i < schema.NumFields(); i++ {
			t.Logf("  field[%d] %s: type=%s", i, schema.Field(i).Name, schema.Field(i).Type)
		}

		mem := memory.DefaultAllocator
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Logf("EXPECTED: PMR.Record() panicked: %v", r)
					t.Log("Root cause: arrow.FLOAT32 case missing in AppendValueOrNull (protobuf_reflect.go)")
				}
			}()
			rec := pmr.Record(mem)
			t.Logf("PMR.Record() succeeded: %d rows, %d cols", rec.NumRows(), rec.NumCols())
			rec.Release()
		}()
	})

	// Test 2: Known message — no float field, should work
	t.Run("Known_NoFloat", func(t *testing.T) {
		msg := &samples.Known{
			Ts:       &timestamppb.Timestamp{Seconds: 1000, Nanos: 500},
			Duration: &durationpb.Duration{Seconds: 42, Nanos: 100},
		}

		pmr := arrowutil.NewProtobufMessageReflection(msg)
		schema := pmr.Schema()
		t.Logf("Schema fields: %d", schema.NumFields())
		for i := 0; i < schema.NumFields(); i++ {
			t.Logf("  field[%d] %s: type=%s", i, schema.Field(i).Name, schema.Field(i).Type)
		}

		mem := memory.DefaultAllocator
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("UNEXPECTED: PMR.Record() panicked for Known (no float): %v", r)
				}
			}()
			rec := pmr.Record(mem)
			t.Logf("PMR.Record() OK: %d rows, %d cols", rec.NumRows(), rec.NumCols())
			rec.Release()
		}()
	})
}
