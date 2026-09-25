//go:build cgo

package main

/*
#include <stdlib.h>
#include <stdint.h>

#ifndef BUFARROW_META_VALUE_DEFINED
#define BUFARROW_META_VALUE_DEFINED
typedef struct {
	uint8_t kind;
	int64_t i64;
	double f64;
	char *ptr;
	int32_t len;
} BufarrowMetaValue;
#endif
*/
import "C"

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime/cgo"
	"strings"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
	bufarrowlib "github.com/loicalleyne/bufarrowlib"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
)

const version = "0.1.0"

// ── Lifecycle ───────────────────────────────────────────────────────────

//export BufarrowNewFromFile
func BufarrowNewFromFile(
	protoPath *C.char,
	msgName *C.char,
	importPaths **C.char,
	nPaths C.int,
	optsJSON *C.char,
	outHandle *unsafe.Pointer,
) C.int {
	defer func() { recoverGlobal(recover()) }()

	pp := C.GoString(protoPath)
	mn := C.GoString(msgName)

	paths := make([]string, int(nPaths))
	if nPaths > 0 && importPaths != nil {
		slice := unsafe.Slice(importPaths, int(nPaths))
		for i := range paths {
			paths[i] = C.GoString(slice[i])
		}
	}

	var opts []bufarrowlib.Option
	if optsJSON != nil {
		parsedOpts, err := parseOptsJSON(C.GoString(optsJSON))
		if err != nil {
			return setGlobalError(outHandle, err)
		}
		opts = parsedOpts
	}

	alloc := mallocator.NewMallocator()
	tc, err := bufarrowlib.NewFromFile(pp, mn, paths, alloc, opts...)
	if err != nil {
		return setGlobalError(outHandle, err)
	}

	ct := newCTranscoder(tc, alloc)
	h := cgo.NewHandle(ct)
	*outHandle = createHandle(h)
	return 0
}

//export BufarrowNewFromConfig
func BufarrowNewFromConfig(
	configPath *C.char,
	outHandle *unsafe.Pointer,
) C.int {
	defer func() { recoverGlobal(recover()) }()

	alloc := mallocator.NewMallocator()
	tc, err := bufarrowlib.NewTranscoderFromConfigFile(C.GoString(configPath), alloc)
	if err != nil {
		return setGlobalError(outHandle, err)
	}

	ct := newCTranscoder(tc, alloc)
	h := cgo.NewHandle(ct)
	*outHandle = createHandle(h)
	return 0
}

//export BufarrowNewFromConfigString
func BufarrowNewFromConfigString(
	configYAML *C.char,
	configLen C.int,
	outHandle *unsafe.Pointer,
) C.int {
	defer func() { recoverGlobal(recover()) }()

	yamlStr := C.GoStringN(configYAML, configLen)
	cfg, err := bufarrowlib.ParseDenormConfig(strings.NewReader(yamlStr))
	if err != nil {
		setGlobalErr(err)
		return -1
	}

	alloc := mallocator.NewMallocator()
	tc, err := bufarrowlib.NewTranscoderFromConfig(cfg, alloc)
	if err != nil {
		setGlobalErr(err)
		return -1
	}

	ct := newCTranscoder(tc, alloc)
	h := cgo.NewHandle(ct)
	*outHandle = createHandle(h)
	return 0
}

//export BufarrowClone
func BufarrowClone(
	handle unsafe.Pointer,
	outHandle *unsafe.Pointer,
) C.int {
	var ct *cTranscoder
	defer func() { recoverTranscoder(recover(), ct) }()
	ct = getFromHandle[cTranscoder](handle)
	alloc := mallocator.NewMallocator()
	tc, err := ct.tc.Clone(alloc)
	if err != nil {
		ct.setError(err)
		return -1
	}

	clone := newCTranscoder(tc, alloc)
	h := cgo.NewHandle(clone)
	*outHandle = createHandle(h)
	return 0
}

//export BufarrowFree
func BufarrowFree(handle unsafe.Pointer) {
	defer func() { recover() }()
	if handle == nil {
		return
	}
	ct := getFromHandle[cTranscoder](handle)
	ct.tc.Release()
	freeHandle(handle)
}

// ── Ingestion ───────────────────────────────────────────────────────────

//export BufarrowAppendRaw
func BufarrowAppendRaw(
	handle unsafe.Pointer,
	data unsafe.Pointer,
	dataLen C.int,
) C.int {
	var ct *cTranscoder
	defer func() { recoverTranscoder(recover(), ct) }()
	ct = getFromHandle[cTranscoder](handle)
	buf := C.GoBytes(data, dataLen)
	if err := ct.tc.AppendRaw(buf); err != nil {
		ct.setError(err)
		return -1
	}
	return 0
}

//export BufarrowAppendDenormRaw
func BufarrowAppendDenormRaw(
	handle unsafe.Pointer,
	data unsafe.Pointer,
	dataLen C.int,
	metaValues *C.BufarrowMetaValue,
	nMeta C.int,
	outRows *C.int,
) C.int {
	var ct *cTranscoder
	defer func() { recoverTranscoder(recover(), ct) }()
	ct = getFromHandle[cTranscoder](handle)
	if nMeta < 0 {
		ct.setError(fmt.Errorf("bufarrow: n_meta must be >= 0, got %d", int(nMeta)))
		return -1
	}
	buf := C.GoBytes(data, dataLen)
	meta, err := cMetaValuesToGo(ct.tc, metaValues, nMeta)
	if err != nil {
		ct.setError(err)
		return -1
	}
	n, err := ct.tc.AppendDenormRaw(buf, meta...)
	if err != nil {
		ct.setError(err)
		return -1
	}
	if outRows != nil {
		*outRows = C.int(n)
	}
	return 0
}

//export BufarrowAppendRawMerged
func BufarrowAppendRawMerged(
	handle unsafe.Pointer,
	baseData unsafe.Pointer,
	baseLen C.int,
	customData unsafe.Pointer,
	customLen C.int,
) C.int {
	var ct *cTranscoder
	defer func() { recoverTranscoder(recover(), ct) }()
	ct = getFromHandle[cTranscoder](handle)
	baseBuf := C.GoBytes(baseData, baseLen)
	customBuf := C.GoBytes(customData, customLen)
	if err := ct.tc.AppendRawMerged(baseBuf, customBuf); err != nil {
		ct.setError(err)
		return -1
	}
	return 0
}

//export BufarrowAppendDenormRawMerged
func BufarrowAppendDenormRawMerged(
	handle unsafe.Pointer,
	baseData unsafe.Pointer,
	baseLen C.int,
	customData unsafe.Pointer,
	customLen C.int,
	metaValues *C.BufarrowMetaValue,
	nMeta C.int,
	outRows *C.int,
) C.int {
	var ct *cTranscoder
	defer func() { recoverTranscoder(recover(), ct) }()
	ct = getFromHandle[cTranscoder](handle)
	if nMeta < 0 {
		ct.setError(fmt.Errorf("bufarrow: n_meta must be >= 0, got %d", int(nMeta)))
		return -1
	}
	baseBuf := C.GoBytes(baseData, baseLen)
	customBuf := C.GoBytes(customData, customLen)
	meta, err := cMetaValuesToGo(ct.tc, metaValues, nMeta)
	if err != nil {
		ct.setError(err)
		return -1
	}
	n, err := ct.tc.AppendDenormRawMerged(baseBuf, customBuf, meta...)
	if err != nil {
		ct.setError(err)
		return -1
	}
	if outRows != nil {
		*outRows = C.int(n)
	}
	return 0
}

// ── Flush ───────────────────────────────────────────────────────────────

//export BufarrowFlush
func BufarrowFlush(
	handle unsafe.Pointer,
	outArray unsafe.Pointer,
	outSchema unsafe.Pointer,
) C.int {
	var ct *cTranscoder
	defer func() { recoverTranscoder(recover(), ct) }()
	ct = getFromHandle[cTranscoder](handle)
	rb := ct.tc.NewRecordBatch()
	if rb == nil {
		ct.setError(fmt.Errorf("bufarrow: flush returned nil record batch"))
		return -1
	}
	defer rb.Release()

	exportRecordBatchPtr(rb, outArray, outSchema)
	return 0
}

//export BufarrowFlushDenorm
func BufarrowFlushDenorm(
	handle unsafe.Pointer,
	outArray unsafe.Pointer,
	outSchema unsafe.Pointer,
) C.int {
	var ct *cTranscoder
	defer func() { recoverTranscoder(recover(), ct) }()
	ct = getFromHandle[cTranscoder](handle)
	rb := ct.tc.NewDenormalizerRecordBatch()
	if rb == nil {
		ct.setError(fmt.Errorf("bufarrow: flush denorm returned nil record batch"))
		return -1
	}
	defer rb.Release()

	exportRecordBatchPtr(rb, outArray, outSchema)
	return 0
}

// ── Schema ──────────────────────────────────────────────────────────────

//export BufarrowGetSchema
func BufarrowGetSchema(
	handle unsafe.Pointer,
	outSchema unsafe.Pointer,
) C.int {
	var ct *cTranscoder
	defer func() { recoverTranscoder(recover(), ct) }()
	ct = getFromHandle[cTranscoder](handle)
	sc := ct.tc.Schema()
	if sc == nil {
		ct.setError(fmt.Errorf("bufarrow: schema is nil"))
		return -1
	}

	exportSchemaPtr(sc, outSchema)
	return 0
}

//export BufarrowGetDenormSchema
func BufarrowGetDenormSchema(
	handle unsafe.Pointer,
	outSchema unsafe.Pointer,
) C.int {
	var ct *cTranscoder
	defer func() { recoverTranscoder(recover(), ct) }()
	ct = getFromHandle[cTranscoder](handle)
	sc := ct.tc.DenormalizerSchema()
	if sc == nil {
		ct.setError(fmt.Errorf("bufarrow: denorm schema is nil; denormalizer not configured"))
		return -1
	}

	exportSchemaPtr(sc, outSchema)
	return 0
}

// ── Parquet ─────────────────────────────────────────────────────────────

//export BufarrowWriteParquet
func BufarrowWriteParquet(
	handle unsafe.Pointer,
	filePath *C.char,
) C.int {
	var ct *cTranscoder
	defer func() { recoverTranscoder(recover(), ct) }()
	ct = getFromHandle[cTranscoder](handle)
	fp := C.GoString(filePath)

	f, err := os.Create(fp)
	if err != nil {
		ct.setError(fmt.Errorf("bufarrow: create parquet file: %w", err))
		return -1
	}
	defer f.Close()

	if err := ct.tc.WriteParquet(f); err != nil {
		ct.setError(err)
		return -1
	}
	return 0
}

//export BufarrowWriteParquetDenorm
func BufarrowWriteParquetDenorm(
	handle unsafe.Pointer,
	filePath *C.char,
) C.int {
	var ct *cTranscoder
	defer func() { recoverTranscoder(recover(), ct) }()
	ct = getFromHandle[cTranscoder](handle)
	fp := C.GoString(filePath)

	rb := ct.tc.NewDenormalizerRecordBatch()
	if rb == nil {
		ct.setError(fmt.Errorf("bufarrow: denorm not configured"))
		return -1
	}
	defer rb.Release()

	f, err := os.Create(fp)
	if err != nil {
		ct.setError(fmt.Errorf("bufarrow: create parquet file: %w", err))
		return -1
	}
	defer f.Close()

	if err := ct.tc.WriteParquetRecords(f, rb); err != nil {
		ct.setError(err)
		return -1
	}
	return 0
}

//export BufarrowReadParquet
func BufarrowReadParquet(
	handle unsafe.Pointer,
	filePath *C.char,
	columnsJSON *C.char,
	outArray unsafe.Pointer,
	outSchema unsafe.Pointer,
) C.int {
	var ct *cTranscoder
	defer func() { recoverTranscoder(recover(), ct) }()
	ct = getFromHandle[cTranscoder](handle)
	fp := C.GoString(filePath)

	f, err := os.Open(fp)
	if err != nil {
		ct.setError(fmt.Errorf("bufarrow: open parquet file: %w", err))
		return -1
	}
	defer f.Close()

	var columns []int
	if columnsJSON != nil {
		js := C.GoString(columnsJSON)
		if js != "" {
			if err := json.Unmarshal([]byte(js), &columns); err != nil {
				ct.setError(fmt.Errorf("bufarrow: invalid columns_json: %w", err))
				return -1
			}
		}
	}

	rb, err := ct.tc.ReadParquet(context.Background(), f, columns)
	if err != nil {
		ct.setError(fmt.Errorf("bufarrow: read parquet: %w", err))
		return -1
	}
	defer rb.Release()

	exportRecordBatchPtr(rb, outArray, outSchema)
	return 0
}

// ── Info ────────────────────────────────────────────────────────────────

//export BufarrowFieldNames
func BufarrowFieldNames(handle unsafe.Pointer) *C.char {
	var ct *cTranscoder
	defer func() { recoverTranscoder(recover(), ct) }()
	ct = getFromHandle[cTranscoder](handle)
	names := ct.tc.FieldNames()
	b, _ := json.Marshal(names)
	return cString(string(b))
}

//export BufarrowLastError
func BufarrowLastError(handle unsafe.Pointer) *C.char {
	defer func() { recoverGlobal(recover()) }()

	ct := getFromHandle[cTranscoder](handle)
	return cString(ct.getError())
}

//export BufarrowFreeString
func BufarrowFreeString(s *C.char) {
	if s != nil {
		C.free(unsafe.Pointer(s))
	}
}

//export BufarrowVersion
func BufarrowVersion() *C.char {
	return cString(version)
}

//export BufarrowGetGlobalError
func BufarrowGetGlobalError() *C.char {
	return cString(getGlobalErr())
}

// ── HyperType ───────────────────────────────────────────────────────────

//export BufarrowNewHyperType
func BufarrowNewHyperType(
	protoPath *C.char,
	msgName *C.char,
	importPaths **C.char,
	nPaths C.int,
	threshold C.int64_t,
	rate C.double,
	outHandle *unsafe.Pointer,
) C.int {
	defer func() { recoverGlobal(recover()) }()

	pp := C.GoString(protoPath)
	mn := C.GoString(msgName)

	paths := make([]string, int(nPaths))
	if nPaths > 0 && importPaths != nil {
		slice := unsafe.Slice(importPaths, int(nPaths))
		for i := range paths {
			paths[i] = C.GoString(slice[i])
		}
	}

	fd, err := bufarrowlib.CompileProtoToFileDescriptor(pp, paths)
	if err != nil {
		return setGlobalError(outHandle, err)
	}
	md, err := bufarrowlib.GetMessageDescriptorByName(fd, mn)
	if err != nil {
		return setGlobalError(outHandle, err)
	}

	var htOpts []bufarrowlib.HyperTypeOption
	if int64(threshold) > 0 {
		htOpts = append(htOpts, bufarrowlib.WithAutoRecompile(int64(threshold), float64(rate)))
	}

	ht := bufarrowlib.NewHyperType(md, htOpts...)
	ch := &cHyperType{ht: ht}
	h := cgo.NewHandle(ch)
	*outHandle = createHandle(h)
	return 0
}

//export BufarrowFreeHyperType
func BufarrowFreeHyperType(handle unsafe.Pointer) {
	defer func() { recover() }()
	if handle == nil {
		return
	}
	freeHandle(handle)
}

//export BufarrowNewFromFileWithHyperType
func BufarrowNewFromFileWithHyperType(
	protoPath *C.char,
	msgName *C.char,
	importPaths **C.char,
	nPaths C.int,
	optsJSON *C.char,
	hyperHandle unsafe.Pointer,
	outHandle *unsafe.Pointer,
) C.int {
	defer func() { recoverGlobal(recover()) }()

	pp := C.GoString(protoPath)
	mn := C.GoString(msgName)

	paths := make([]string, int(nPaths))
	if nPaths > 0 && importPaths != nil {
		slice := unsafe.Slice(importPaths, int(nPaths))
		for i := range paths {
			paths[i] = C.GoString(slice[i])
		}
	}

	ch := getFromHandle[cHyperType](hyperHandle)
	opts := []bufarrowlib.Option{bufarrowlib.WithHyperType(ch.ht)}

	if optsJSON != nil {
		parsedOpts, err := parseOptsJSON(C.GoString(optsJSON))
		if err != nil {
			return setGlobalError(outHandle, err)
		}
		opts = append(opts, parsedOpts...)
	}

	alloc := mallocator.NewMallocator()
	tc, err := bufarrowlib.NewFromFile(pp, mn, paths, alloc, opts...)
	if err != nil {
		return setGlobalError(outHandle, err)
	}

	ct := newCTranscoder(tc, alloc)
	h := cgo.NewHandle(ct)
	*outHandle = createHandle(h)
	return 0
}

// ── Helpers ─────────────────────────────────────────────────────────────

// setGlobalError is used when no handle exists yet to store an error.
// The error can be retrieved via BufarrowGetGlobalError.
func setGlobalError(_ *unsafe.Pointer, err error) C.int {
	setGlobalErr(err)
	return -1
}

// optsPayload is the JSON structure for option overrides passed via opts_json.
type optsPayload struct {
	CustomProto       string          `json:"custom_proto,omitempty"`
	CustomMessage     string          `json:"custom_message,omitempty"`
	CustomImportPaths []string        `json:"custom_import_paths,omitempty"`
	DenormColumns     []string        `json:"denorm_columns,omitempty"`
	DenormMetaColumns []metaColumnDTO `json:"denorm_metadata_columns,omitempty"`
}

type metaColumnDTO struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// parseOptsJSON decodes JSON option overrides into bufarrowlib.Option values.
func parseOptsJSON(s string) ([]bufarrowlib.Option, error) {
	if s == "" {
		return nil, nil
	}
	var p optsPayload
	if err := json.Unmarshal([]byte(s), &p); err != nil {
		return nil, fmt.Errorf("bufarrow: invalid opts_json: %w", err)
	}

	var opts []bufarrowlib.Option
	if p.CustomProto != "" && p.CustomMessage != "" {
		opts = append(opts, bufarrowlib.WithCustomMessageFile(p.CustomProto, p.CustomMessage, p.CustomImportPaths))
	}
	if len(p.DenormColumns) > 0 {
		specs := make([]pbpath.PlanPathSpec, len(p.DenormColumns))
		for i, col := range p.DenormColumns {
			// Use the last path segment as alias (e.g. "imp[*].id" → "id")
			alias := col
			if idx := strings.LastIndex(col, "."); idx >= 0 {
				alias = col[idx+1:]
			}
			// Strip array subscripts from alias (e.g. "items[*]" → "items")
			if idx := strings.Index(alias, "["); idx >= 0 {
				alias = alias[:idx]
			}
			specs[i] = pbpath.PlanPath(col, pbpath.Alias(alias))
		}
		opts = append(opts, bufarrowlib.WithDenormalizerPlan(specs...))
	}
	if len(p.DenormMetaColumns) > 0 {
		cols := make([]bufarrowlib.DenormMetadataColumn, 0, len(p.DenormMetaColumns))
		for _, mc := range p.DenormMetaColumns {
			dt, err := parseMetadataArrowType(mc.Type)
			if err != nil {
				return nil, err
			}
			cols = append(cols, bufarrowlib.DenormMetadataColumn{Name: bufarrowlib.MetaCol(mc.Name), Type: dt})
		}
		opts = append(opts, bufarrowlib.WithDenormMetadataColumns(cols...))
	}
	return opts, nil
}

func parseMetadataArrowType(name string) (arrow.DataType, error) {
	switch strings.ToLower(name) {
	case "bool", "boolean":
		return arrow.FixedWidthTypes.Boolean, nil
	case "int32":
		return arrow.PrimitiveTypes.Int32, nil
	case "int64":
		return arrow.PrimitiveTypes.Int64, nil
	case "uint32":
		return arrow.PrimitiveTypes.Uint32, nil
	case "uint64":
		return arrow.PrimitiveTypes.Uint64, nil
	case "float32":
		return arrow.PrimitiveTypes.Float32, nil
	case "float64":
		return arrow.PrimitiveTypes.Float64, nil
	case "utf8", "string":
		return arrow.BinaryTypes.String, nil
	case "large_string":
		return arrow.BinaryTypes.LargeString, nil
	case "binary":
		return arrow.BinaryTypes.Binary, nil
	case "large_binary":
		return arrow.BinaryTypes.LargeBinary, nil
	case "timestamp_ms":
		return &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, nil
	case "timestamp_us":
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
	default:
		return nil, fmt.Errorf("bufarrow: unsupported denorm metadata column type %q", name)
	}
}

func cMetaValuesToGo(tc *bufarrowlib.Transcoder, metaValues *C.BufarrowMetaValue, nMeta C.int) ([]bufarrowlib.DenormMetaValue, error) {
	if nMeta == 0 {
		return nil, nil
	}
	registered := tc.DenormMetaColumns()
	if int(nMeta) != len(registered) {
		return nil, fmt.Errorf("bufarrow: metadata value count %d does not match registered metadata columns %d", int(nMeta), len(registered))
	}
	slice := unsafe.Slice(metaValues, int(nMeta))
	out := make([]bufarrowlib.DenormMetaValue, len(slice))
	for i, mv := range slice {
		val, err := decodeCMetaValue(registered[i], mv)
		if err != nil {
			return nil, err
		}
		out[i] = bufarrowlib.DenormMetaValue{Col: registered[i].Name, Value: val}
	}
	return out, nil
}

func decodeCMetaValue(col bufarrowlib.DenormMetadataColumn, mv C.BufarrowMetaValue) (any, error) {
	if mv.kind == 3 {
		return nil, nil
	}
	switch col.Type.(type) {
	case *arrow.BooleanType:
		if mv.kind != 0 {
			return nil, fmt.Errorf("metadata column %q: cannot represent kind %d as %v", col.Name, int(mv.kind), col.Type)
		}
		return mv.i64 != 0, nil
	case *arrow.Int32Type:
		if mv.kind != 0 {
			return nil, fmt.Errorf("metadata column %q: cannot represent kind %d as %v", col.Name, int(mv.kind), col.Type)
		}
		return int32(mv.i64), nil
	case *arrow.Int64Type:
		if mv.kind != 0 {
			return nil, fmt.Errorf("metadata column %q: cannot represent kind %d as %v", col.Name, int(mv.kind), col.Type)
		}
		return int64(mv.i64), nil
	case *arrow.Uint32Type:
		if mv.kind != 0 {
			return nil, fmt.Errorf("metadata column %q: cannot represent kind %d as %v", col.Name, int(mv.kind), col.Type)
		}
		return uint32(mv.i64), nil
	case *arrow.Uint64Type:
		if mv.kind != 0 {
			return nil, fmt.Errorf("metadata column %q: cannot represent kind %d as %v", col.Name, int(mv.kind), col.Type)
		}
		return uint64(mv.i64), nil
	case *arrow.Float32Type:
		if mv.kind != 1 {
			return nil, fmt.Errorf("metadata column %q: cannot represent kind %d as %v", col.Name, int(mv.kind), col.Type)
		}
		return float32(mv.f64), nil
	case *arrow.Float64Type:
		if mv.kind != 1 {
			return nil, fmt.Errorf("metadata column %q: cannot represent kind %d as %v", col.Name, int(mv.kind), col.Type)
		}
		return float64(mv.f64), nil
	case *arrow.StringType, *arrow.LargeStringType:
		if mv.kind != 2 {
			return nil, fmt.Errorf("metadata column %q: cannot represent kind %d as %v", col.Name, int(mv.kind), col.Type)
		}
		return C.GoStringN(mv.ptr, C.int(mv.len)), nil
	case *arrow.BinaryType, *arrow.LargeBinaryType:
		if mv.kind != 2 {
			return nil, fmt.Errorf("metadata column %q: cannot represent kind %d as %v", col.Name, int(mv.kind), col.Type)
		}
		return C.GoBytes(unsafe.Pointer(mv.ptr), C.int(mv.len)), nil
	case *arrow.TimestampType:
		if mv.kind != 0 {
			return nil, fmt.Errorf("metadata column %q: cannot represent kind %d as %v", col.Name, int(mv.kind), col.Type)
		}
		return arrow.Timestamp(mv.i64), nil
	default:
		return nil, fmt.Errorf("metadata column %q: unsupported type %v", col.Name, col.Type)
	}
}

// main is required by c-shared buildmode.
func main() {}
