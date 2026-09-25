package bufarrowlib

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestArrowTypeToMetadataAppendFuncCoversSupportedTypes verifies that metadata
// appenders are built for all supported Arrow scalar metadata types and handle
// value, null, and wrong-type inputs as expected.
func TestArrowTypeToMetadataAppendFuncCoversSupportedTypes(t *testing.T) {
	alloc := memory.DefaultAllocator
	now := time.Unix(1_700_000_000, 123_000_000).UTC()

	tests := []struct {
		name  string
		dt    arrow.DataType
		newB  func() array.Builder
		okVal any
		bad   any
	}{
		{name: "bool", dt: arrow.FixedWidthTypes.Boolean, newB: func() array.Builder { return array.NewBooleanBuilder(alloc) }, okVal: true, bad: "x"},
		{name: "int32", dt: arrow.PrimitiveTypes.Int32, newB: func() array.Builder { return array.NewInt32Builder(alloc) }, okVal: int32(1), bad: int64(1)},
		{name: "int64", dt: arrow.PrimitiveTypes.Int64, newB: func() array.Builder { return array.NewInt64Builder(alloc) }, okVal: int64(1), bad: int32(1)},
		{name: "uint32", dt: arrow.PrimitiveTypes.Uint32, newB: func() array.Builder { return array.NewUint32Builder(alloc) }, okVal: uint32(1), bad: int32(1)},
		{name: "uint64", dt: arrow.PrimitiveTypes.Uint64, newB: func() array.Builder { return array.NewUint64Builder(alloc) }, okVal: uint64(1), bad: int64(1)},
		{name: "float32", dt: arrow.PrimitiveTypes.Float32, newB: func() array.Builder { return array.NewFloat32Builder(alloc) }, okVal: float32(1.5), bad: float64(1.5)},
		{name: "float64", dt: arrow.PrimitiveTypes.Float64, newB: func() array.Builder { return array.NewFloat64Builder(alloc) }, okVal: float64(1.5), bad: float32(1.5)},
		{name: "string", dt: arrow.BinaryTypes.String, newB: func() array.Builder { return array.NewStringBuilder(alloc) }, okVal: "ok", bad: 1},
		{name: "large-string", dt: arrow.BinaryTypes.LargeString, newB: func() array.Builder { return array.NewLargeStringBuilder(alloc) }, okVal: "ok", bad: 1},
		{name: "binary", dt: arrow.BinaryTypes.Binary, newB: func() array.Builder { return array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary) }, okVal: []byte("ok"), bad: "x"},
		{name: "large-binary", dt: arrow.BinaryTypes.LargeBinary, newB: func() array.Builder { return array.NewBinaryBuilder(alloc, arrow.BinaryTypes.LargeBinary) }, okVal: []byte("ok"), bad: "x"},
		{name: "timestamp-from-time", dt: &arrow.TimestampType{Unit: arrow.Millisecond}, newB: func() array.Builder {
			return array.NewTimestampBuilder(alloc, &arrow.TimestampType{Unit: arrow.Millisecond})
		}, okVal: now, bad: "x"},
		{name: "timestamp-native", dt: &arrow.TimestampType{Unit: arrow.Millisecond}, newB: func() array.Builder {
			return array.NewTimestampBuilder(alloc, &arrow.TimestampType{Unit: arrow.Millisecond})
		}, okVal: arrow.Timestamp(now.UnixMilli()), bad: "x"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := tc.newB()
			defer b.Release()

			fn := arrowTypeToMetadataAppendFunc(tc.dt, b)
			if fn == nil {
				t.Fatalf("arrowTypeToMetadataAppendFunc(%s) returned nil", tc.name)
			}
			if err := fn(tc.okVal, 2); err != nil {
				t.Fatalf("append value error: %v", err)
			}
			if err := fn(nil, 1); err != nil {
				t.Fatalf("append nil error: %v", err)
			}
			if err := fn(tc.bad, 1); err == nil {
				t.Fatalf("expected type error for %T", tc.bad)
			}

			arr := b.NewArray()
			defer arr.Release()
			if arr.Len() != 3 {
				t.Fatalf("array length = %d, want 3", arr.Len())
			}
			if arr.NullN() != 1 {
				t.Fatalf("null count = %d, want 1", arr.NullN())
			}
		})
	}
}

// TestArrowTypeToMetadataAppendFuncUnsupportedType verifies unsupported Arrow
// metadata types do not produce append closures.
func TestArrowTypeToMetadataAppendFuncUnsupportedType(t *testing.T) {
	alloc := memory.DefaultAllocator
	ft := &arrow.FixedSizeBinaryType{ByteWidth: 16}
	b := array.NewFixedSizeBinaryBuilder(alloc, ft)
	defer b.Release()

	if fn := arrowTypeToMetadataAppendFunc(ft, b); fn != nil {
		t.Fatalf("expected nil append func for unsupported type %v", ft)
	}
}

// TestArrowTypeToMetadataAppendFuncTimestampUnsupportedUnit verifies the
// timestamp metadata appender returns an explicit error for unknown units.
func TestArrowTypeToMetadataAppendFuncTimestampUnsupportedUnit(t *testing.T) {
	alloc := memory.DefaultAllocator
	tt := &arrow.TimestampType{Unit: arrow.TimeUnit(99)}
	b := array.NewTimestampBuilder(alloc, tt)
	defer b.Release()

	fn := arrowTypeToMetadataAppendFunc(tt, b)
	if fn == nil {
		t.Fatal("expected timestamp append func")
	}
	err := fn(time.Unix(0, 0).UTC(), 1)
	if err == nil {
		t.Fatal("expected unsupported timestamp unit error")
	}
	if !strings.Contains(err.Error(), "unsupported timestamp unit") {
		t.Fatalf("unexpected error: %v", err)
	}
}
