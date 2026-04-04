package bufarrowlib

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib/gen/go/samples"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestAppendRawMerged(t *testing.T) {
	protoDir := testProtoDir(t)

	t.Run("success_fallback_path", func(t *testing.T) {
		baseMD := new(samples.Three).ProtoReflect().Descriptor()

		customFD, err := CompileProtoToFileDescriptor("custom_fields.proto", []string{protoDir})
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		customMD, err := GetMessageDescriptorByName(customFD, "CustomFields")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		tc, err := New(baseMD, memory.DefaultAllocator, WithCustomMessage(customMD))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		defer tc.Release()

		// Marshal base message
		baseMsg := &samples.Three{Value: 42}
		baseBytes, err := proto.Marshal(baseMsg)
		if err != nil {
			t.Fatalf("marshal base: %v", err)
		}

		// Marshal custom message
		customMsg := dynamicpb.NewMessage(customMD)
		customMsg.Set(customMD.Fields().ByName("event_timestamp"), protoreflect.ValueOfInt64(1234567890))
		customMsg.Set(customMD.Fields().ByName("source_id"), protoreflect.ValueOfString("test-source"))
		customBytes, err := proto.Marshal(customMsg)
		if err != nil {
			t.Fatalf("marshal custom: %v", err)
		}

		err = tc.AppendRawMerged(baseBytes, customBytes)
		if err != nil {
			t.Fatalf("AppendRawMerged() error = %v", err)
		}

		r := tc.NewRecordBatch()
		if r.NumRows() != 1 {
			t.Errorf("expected 1 row, got %d", r.NumRows())
		}
		if r.NumCols() != 3 {
			t.Errorf("expected 3 columns (value, event_timestamp, source_id), got %d", r.NumCols())
		}
	})

	t.Run("success_multiple_appends", func(t *testing.T) {
		baseMD := new(samples.Three).ProtoReflect().Descriptor()

		customFD, err := CompileProtoToFileDescriptor("custom_fields.proto", []string{protoDir})
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		customMD, err := GetMessageDescriptorByName(customFD, "CustomFields")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		tc, err := New(baseMD, memory.DefaultAllocator, WithCustomMessage(customMD))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		defer tc.Release()

		for i := 0; i < 5; i++ {
			baseMsg := &samples.Three{Value: uint64(i * 10)}
			baseBytes, err := proto.Marshal(baseMsg)
			if err != nil {
				t.Fatalf("marshal base %d: %v", i, err)
			}

			customMsg := dynamicpb.NewMessage(customMD)
			customMsg.Set(customMD.Fields().ByName("event_timestamp"), protoreflect.ValueOfInt64(int64(i)))
			customMsg.Set(customMD.Fields().ByName("source_id"), protoreflect.ValueOfString("src"))
			customBytes, err := proto.Marshal(customMsg)
			if err != nil {
				t.Fatalf("marshal custom %d: %v", i, err)
			}

			err = tc.AppendRawMerged(baseBytes, customBytes)
			if err != nil {
				t.Fatalf("AppendRawMerged() iteration %d error = %v", i, err)
			}
		}

		r := tc.NewRecordBatch()
		if r.NumRows() != 5 {
			t.Errorf("expected 5 rows, got %d", r.NumRows())
		}
	})

	t.Run("error_without_custom_configured", func(t *testing.T) {
		baseMD := new(samples.Three).ProtoReflect().Descriptor()

		tc, err := New(baseMD, memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		defer tc.Release()

		err = tc.AppendRawMerged([]byte{}, []byte{})
		if err == nil {
			t.Fatal("expected error when calling AppendRawMerged without custom configured")
		}
		if !strings.Contains(err.Error(), "without custom message configured") {
			t.Errorf("expected 'without custom message configured' error, got: %v", err)
		}
	})

	t.Run("success_with_hypertype", func(t *testing.T) {
		baseMD := new(samples.Three).ProtoReflect().Descriptor()

		customFD, err := CompileProtoToFileDescriptor("custom_fields.proto", []string{protoDir})
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		customMD, err := GetMessageDescriptorByName(customFD, "CustomFields")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		// Build merged descriptor for HyperType
		mergedMD, err := MergeMessageDescriptors(baseMD, customMD, "ThreeCustom")
		if err != nil {
			t.Fatalf("merge: %v", err)
		}

		ht := NewHyperType(mergedMD)

		tc, err := New(baseMD, memory.DefaultAllocator,
			WithCustomMessage(customMD),
			WithHyperType(ht))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		defer tc.Release()

		baseMsg := &samples.Three{Value: 99}
		baseBytes, err := proto.Marshal(baseMsg)
		if err != nil {
			t.Fatalf("marshal base: %v", err)
		}

		customMsg := dynamicpb.NewMessage(customMD)
		customMsg.Set(customMD.Fields().ByName("event_timestamp"), protoreflect.ValueOfInt64(555))
		customMsg.Set(customMD.Fields().ByName("source_id"), protoreflect.ValueOfString("hyper"))
		customBytes, err := proto.Marshal(customMsg)
		if err != nil {
			t.Fatalf("marshal custom: %v", err)
		}

		err = tc.AppendRawMerged(baseBytes, customBytes)
		if err != nil {
			t.Fatalf("AppendRawMerged() with HyperType error = %v", err)
		}

		r := tc.NewRecordBatch()
		if r.NumRows() != 1 {
			t.Errorf("expected 1 row, got %d", r.NumRows())
		}
		if r.NumCols() != 3 {
			t.Errorf("expected 3 columns, got %d", r.NumCols())
		}
	})
}

func TestAppendDenormRawMerged(t *testing.T) {
	protoDir := testProtoDir(t)

	t.Run("error_without_custom_configured", func(t *testing.T) {
		baseMD := new(samples.Three).ProtoReflect().Descriptor()

		tc, err := New(baseMD, memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		defer tc.Release()

		err = tc.AppendDenormRawMerged([]byte{}, []byte{})
		if err == nil {
			t.Fatal("expected error when calling AppendDenormRawMerged without custom configured")
		}
		if !strings.Contains(err.Error(), "without custom message configured") {
			t.Errorf("expected 'without custom message configured' error, got: %v", err)
		}
	})

	t.Run("error_without_denorm_plan", func(t *testing.T) {
		baseMD := new(samples.Three).ProtoReflect().Descriptor()

		customFD, err := CompileProtoToFileDescriptor("custom_fields.proto", []string{protoDir})
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		customMD, err := GetMessageDescriptorByName(customFD, "CustomFields")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		tc, err := New(baseMD, memory.DefaultAllocator, WithCustomMessage(customMD))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		defer tc.Release()

		err = tc.AppendDenormRawMerged([]byte{}, []byte{})
		if err == nil {
			t.Fatal("expected error when calling AppendDenormRawMerged without denorm plan")
		}
		if !strings.Contains(err.Error(), "without denormalizer plan configured") {
			t.Errorf("expected 'without denormalizer plan configured' error, got: %v", err)
		}
	})
}

func TestAppendDenormRaw(t *testing.T) {
	t.Run("error_without_denorm_plan", func(t *testing.T) {
		baseMD := new(samples.Three).ProtoReflect().Descriptor()

		tc, err := New(baseMD, memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		defer tc.Release()

		err = tc.AppendDenormRaw([]byte{})
		if err == nil {
			t.Fatal("expected error when calling AppendDenormRaw without denorm plan")
		}
		if !strings.Contains(err.Error(), "without denormalizer plan configured") {
			t.Errorf("expected 'without denormalizer plan configured' error, got: %v", err)
		}
	})
}
