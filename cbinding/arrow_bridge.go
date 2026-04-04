//go:build cgo

package main

import (
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

// exportRecordBatchPtr exports an Arrow RecordBatch to C Data Interface structs
// via raw pointers. The caller passes unsafe.Pointer values that point to
// ArrowArray and ArrowSchema structs allocated in C (or Python ctypes).
func exportRecordBatchPtr(rb arrow.RecordBatch, outArray, outSchema unsafe.Pointer) {
	cdata.ExportArrowRecordBatch(
		rb,
		cdata.ArrayFromPtr(uintptr(outArray)),
		cdata.SchemaFromPtr(uintptr(outSchema)),
	)
}

// exportSchemaPtr exports an Arrow Schema to a C Data Interface struct via raw pointer.
func exportSchemaPtr(sc *arrow.Schema, outSchema unsafe.Pointer) {
	cdata.ExportArrowSchema(
		sc,
		cdata.SchemaFromPtr(uintptr(outSchema)),
	)
}
