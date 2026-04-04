//go:build cgo

package main

/*
#include <stdlib.h>
#include <stdint.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"runtime/cgo"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
	bufarrowlib "github.com/loicalleyne/bufarrowlib"
)

// ── Pool lifecycle ───────────────────────────────────────────────────────

//export BufarrowPoolNew
func BufarrowPoolNew(
	protoPath *C.char,
	msgName *C.char,
	importPaths **C.char,
	nPaths C.int,
	optsJSON *C.char,
	workers C.int,
	capacity C.int,
	outHandle *unsafe.Pointer,
) C.int {
	defer func() { recover() }()

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

	nWorkers := int(workers)
	if nWorkers <= 0 {
		nWorkers = runtime.GOMAXPROCS(0)
	}

	alloc := mallocator.NewMallocator()
	base, err := bufarrowlib.NewFromFile(pp, mn, paths, alloc, opts...)
	if err != nil {
		return setGlobalError(outHandle, fmt.Errorf("bufarrow pool new: %w", err))
	}

	// Determine pool mode from options.
	mode := poolModeRaw
	optsStr := ""
	if optsJSON != nil {
		optsStr = C.GoString(optsJSON)
	}
	if hasDenorm(optsStr) {
		mode = poolModeDenorm
	}

	pool, err := newCPool(base, nWorkers, int(capacity), mode)
	if err != nil {
		base.Release()
		return setGlobalError(outHandle, err)
	}
	// store base so it can be released on pool free
	pool.clones = append(pool.clones, base)
	pool.allocs = append(pool.allocs, alloc)

	h := cgo.NewHandle(pool)
	*outHandle = createHandle(h)
	return 0
}

//export BufarrowPoolNewWithHyperType
func BufarrowPoolNewWithHyperType(
	protoPath *C.char,
	msgName *C.char,
	importPaths **C.char,
	nPaths C.int,
	optsJSON *C.char,
	hyperHandle unsafe.Pointer,
	workers C.int,
	capacity C.int,
	outHandle *unsafe.Pointer,
) C.int {
	defer func() { recover() }()

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

	nWorkers := int(workers)
	if nWorkers <= 0 {
		nWorkers = runtime.GOMAXPROCS(0)
	}

	alloc := mallocator.NewMallocator()
	base, err := bufarrowlib.NewFromFile(pp, mn, paths, alloc, opts...)
	if err != nil {
		return setGlobalError(outHandle, fmt.Errorf("bufarrow pool new with hypertype: %w", err))
	}

	optsStr := ""
	if optsJSON != nil {
		optsStr = C.GoString(optsJSON)
	}
	mode := poolModeRaw
	if hasDenorm(optsStr) {
		mode = poolModeDenorm
	}

	pool, err := newCPool(base, nWorkers, int(capacity), mode)
	if err != nil {
		base.Release()
		return setGlobalError(outHandle, err)
	}
	pool.clones = append(pool.clones, base)
	pool.allocs = append(pool.allocs, alloc)

	h := cgo.NewHandle(pool)
	*outHandle = createHandle(h)
	return 0
}

// ── Pool ingestion ───────────────────────────────────────────────────────

//export BufarrowPoolSubmit
func BufarrowPoolSubmit(
	handle unsafe.Pointer,
	data unsafe.Pointer,
	dataLen C.int,
) C.int {
	defer func() { recover() }()

	p := getFromHandle[cPool](handle)
	buf := C.GoBytes(data, dataLen)
	if err := p.submit(buf, nil); err != nil {
		p.setError(err)
		return -1
	}
	return 0
}

//export BufarrowPoolSubmitMerged
func BufarrowPoolSubmitMerged(
	handle unsafe.Pointer,
	baseData unsafe.Pointer,
	baseLen C.int,
	customData unsafe.Pointer,
	customLen C.int,
) C.int {
	defer func() { recover() }()

	p := getFromHandle[cPool](handle)
	base := C.GoBytes(baseData, baseLen)
	custom := C.GoBytes(customData, customLen)
	if err := p.submit(base, custom); err != nil {
		p.setError(err)
		return -1
	}
	return 0
}

// ── Pool flush ───────────────────────────────────────────────────────────

//export BufarrowPoolFlush
func BufarrowPoolFlush(
	handle unsafe.Pointer,
	outArray unsafe.Pointer,
	outSchema unsafe.Pointer,
) C.int {
	defer func() { recover() }()

	p := getFromHandle[cPool](handle)
	rb, err := p.flush()
	if err != nil {
		p.setError(err)
		return -1
	}
	defer rb.Release()
	exportRecordBatchPtr(rb, outArray, outSchema)
	return 0
}

// ── Pool info ────────────────────────────────────────────────────────────

//export BufarrowPoolPending
func BufarrowPoolPending(handle unsafe.Pointer) C.int {
	defer func() { recover() }()
	p := getFromHandle[cPool](handle)
	return C.int(p.pending.Load())
}

//export BufarrowPoolLastError
func BufarrowPoolLastError(handle unsafe.Pointer) *C.char {
	defer func() { recover() }()
	p := getFromHandle[cPool](handle)
	return cString(p.getError())
}

//export BufarrowPoolFree
func BufarrowPoolFree(handle unsafe.Pointer) {
	defer func() { recover() }()
	if handle == nil {
		return
	}
	p := getFromHandle[cPool](handle)
	p.release()
	freeHandle(handle)
}

// ── helper ───────────────────────────────────────────────────────────────

// hasDenorm returns true if the opts JSON payload specifies denorm_columns,
// indicating the pool should use the denorm append/flush path.
func hasDenorm(optsStr string) bool {
	if optsStr == "" {
		return false
	}
	// Simple string check — avoids a full JSON parse here.
	// The full parse is done in parseOptsJSON; we just need the mode flag.
	for i := range len(optsStr) - len("denorm_columns") + 1 {
		if optsStr[i:i+len("denorm_columns")] == "denorm_columns" {
			return true
		}
	}
	return false
}
