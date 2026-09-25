package bufarrowlib

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// TestJSONWKTEncode verifies decode behavior for recursive JSON-backed WKTs.
func TestJSONWKTEncode(t *testing.T) {
	md := wktDesc(t, "WithStruct")
	settingsFD := md.Fields().ByName("settings")
	enc := jsonWKTEncode()

	t.Run("valid", func(t *testing.T) {
		sb := array.NewStringBuilder(memory.DefaultAllocator)
		defer sb.Release()
		sb.Append(`{"fields":{"mode":{"stringValue":"on"}}}`)
		arr := sb.NewStringArray()
		defer arr.Release()

		msg := dynamicpb.NewMessage(settingsFD.Message())
		got := enc(protoreflect.ValueOfMessage(msg), arr, 0)
		if !got.IsValid() {
			t.Fatal("expected valid decoded value")
		}
		fieldsFD := msg.Descriptor().Fields().ByName("fields")
		if got.Message().Get(fieldsFD).Map().Len() != 1 {
			t.Fatalf("decoded struct field count = %d, want 1", got.Message().Get(fieldsFD).Map().Len())
		}
	})

	t.Run("invalid", func(t *testing.T) {
		sb := array.NewStringBuilder(memory.DefaultAllocator)
		defer sb.Release()
		sb.Append("{broken")
		arr := sb.NewStringArray()
		defer arr.Release()

		msg := dynamicpb.NewMessage(settingsFD.Message())
		got := enc(protoreflect.ValueOfMessage(msg), arr, 0)
		if got.IsValid() {
			t.Fatal("expected invalid value for malformed JSON")
		}
	})

	t.Run("null", func(t *testing.T) {
		sb := array.NewStringBuilder(memory.DefaultAllocator)
		defer sb.Release()
		sb.AppendNull()
		arr := sb.NewStringArray()
		defer arr.Release()

		msg := dynamicpb.NewMessage(settingsFD.Message())
		got := enc(protoreflect.ValueOfMessage(msg), arr, 0)
		if got.IsValid() {
			t.Fatal("expected invalid value for null cell")
		}
	})

	t.Run("dictionary valid", func(t *testing.T) {
		dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}
		db := array.NewDictionaryBuilder(memory.DefaultAllocator, dt)
		defer db.Release()

		bb, ok := db.(*array.BinaryDictionaryBuilder)
		if !ok {
			t.Fatalf("dictionary builder type = %T, want *array.BinaryDictionaryBuilder", db)
		}
		if err := bb.AppendString(`{"fields":{"mode":{"stringValue":"on"}}}`); err != nil {
			t.Fatalf("AppendString: %v", err)
		}
		arr := bb.NewDictionaryArray()
		defer arr.Release()

		msg := dynamicpb.NewMessage(settingsFD.Message())
		got := enc(protoreflect.ValueOfMessage(msg), arr, 0)
		if !got.IsValid() {
			t.Fatal("expected valid decoded dictionary value")
		}
	})

	t.Run("dictionary invalid", func(t *testing.T) {
		dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}
		db := array.NewDictionaryBuilder(memory.DefaultAllocator, dt)
		defer db.Release()

		bb, ok := db.(*array.BinaryDictionaryBuilder)
		if !ok {
			t.Fatalf("dictionary builder type = %T, want *array.BinaryDictionaryBuilder", db)
		}
		if err := bb.AppendString("{broken"); err != nil {
			t.Fatalf("AppendString: %v", err)
		}
		arr := bb.NewDictionaryArray()
		defer arr.Release()

		msg := dynamicpb.NewMessage(settingsFD.Message())
		got := enc(protoreflect.ValueOfMessage(msg), arr, 0)
		if got.IsValid() {
			t.Fatal("expected invalid value for malformed dictionary JSON")
		}
	})
}

// TestBinaryWKTEncode verifies decode behavior for binary-backed AnyValue WKTs.
func TestBinaryWKTEncode(t *testing.T) {
	md := anyHolderDesc(t)
	anyFD := md.Fields().ByName("any")
	anyMD := anyFD.Message()
	enc := binaryWKTEncode()

	t.Run("valid", func(t *testing.T) {
		in := dynamicpb.NewMessage(anyMD)
		in.Set(anyMD.Fields().ByName("string_value"), protoreflect.ValueOfString("x"))
		raw, err := proto.Marshal(in)
		if err != nil {
			t.Fatalf("marshal any value: %v", err)
		}

		bb := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		defer bb.Release()
		bb.Append(raw)
		arr := bb.NewBinaryArray()
		defer arr.Release()

		out := dynamicpb.NewMessage(anyMD)
		got := enc(protoreflect.ValueOfMessage(out), arr, 0)
		if !got.IsValid() {
			t.Fatal("expected valid decoded value")
		}
		if !proto.Equal(got.Message().Interface(), in) {
			t.Fatalf("decoded AnyValue mismatch: got %v want %v", got.Message().Interface(), in)
		}
	})

	t.Run("null", func(t *testing.T) {
		bb := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		defer bb.Release()
		bb.AppendNull()
		arr := bb.NewBinaryArray()
		defer arr.Release()

		out := dynamicpb.NewMessage(anyMD)
		got := enc(protoreflect.ValueOfMessage(out), arr, 0)
		if got.IsValid() {
			t.Fatal("expected invalid value for null cell")
		}
	})

	t.Run("dictionary valid", func(t *testing.T) {
		in := dynamicpb.NewMessage(anyMD)
		in.Set(anyMD.Fields().ByName("int_value"), protoreflect.ValueOfInt64(7))
		raw, err := proto.Marshal(in)
		if err != nil {
			t.Fatalf("marshal any value: %v", err)
		}

		dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.Binary}
		db := array.NewDictionaryBuilder(memory.DefaultAllocator, dt)
		defer db.Release()

		dbb, ok := db.(*array.BinaryDictionaryBuilder)
		if !ok {
			t.Fatalf("dictionary builder type = %T, want *array.BinaryDictionaryBuilder", db)
		}
		if err := dbb.Append(raw); err != nil {
			t.Fatalf("append raw: %v", err)
		}
		arr := dbb.NewDictionaryArray()
		defer arr.Release()

		out := dynamicpb.NewMessage(anyMD)
		got := enc(protoreflect.ValueOfMessage(out), arr, 0)
		if !got.IsValid() {
			t.Fatal("expected valid decoded dictionary value")
		}
		if !proto.Equal(got.Message().Interface(), in) {
			t.Fatalf("decoded dictionary AnyValue mismatch: got %v want %v", got.Message().Interface(), in)
		}
	})

	t.Run("invalid binary returns zero message", func(t *testing.T) {
		bb := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		defer bb.Release()
		bb.Append([]byte{0xff, 0x00, 0x01})
		arr := bb.NewBinaryArray()
		defer arr.Release()

		out := dynamicpb.NewMessage(anyMD)
		got := enc(protoreflect.ValueOfMessage(out), arr, 0)
		if !got.IsValid() {
			t.Fatal("expected valid value even when payload is malformed")
		}
		if got.Message().Has(anyMD.Fields().ByName("string_value")) || got.Message().Has(anyMD.Fields().ByName("int_value")) {
			t.Fatal("expected malformed payload to decode to zero AnyValue")
		}
	})
}
