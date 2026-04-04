//go:build cgo

// Package main provides C shared-library exports for bufarrowlib.
// Build with: CGO_ENABLED=1 go build -buildmode=c-shared -o libbufarrow.so ./cbinding
package main

/*
#include <stdlib.h>
#include <stdint.h>
*/
import "C"

import (
	"runtime/cgo"
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
	bufarrowlib "github.com/loicalleyne/bufarrowlib"
)

// cTranscoder wraps a bufarrowlib.Transcoder for use across the CGo boundary.
type cTranscoder struct {
	tc        *bufarrowlib.Transcoder
	alloc     *mallocator.Mallocator
	lastError string
	mu        sync.Mutex // protects lastError
}

// cHyperType wraps a bufarrowlib.HyperType for sharing across multiple transcoders.
type cHyperType struct {
	ht        *bufarrowlib.HyperType
	lastError string
	mu        sync.Mutex
}

// newCTranscoder creates a cTranscoder with a Mallocator.
func newCTranscoder(tc *bufarrowlib.Transcoder, alloc *mallocator.Mallocator) *cTranscoder {
	return &cTranscoder{tc: tc, alloc: alloc}
}

// setError records the last error for retrieval via BufarrowLastError.
func (ct *cTranscoder) setError(err error) {
	ct.mu.Lock()
	ct.lastError = err.Error()
	ct.mu.Unlock()
}

// getError returns and clears the last recorded error.
func (ct *cTranscoder) getError() string {
	ct.mu.Lock()
	e := ct.lastError
	ct.lastError = ""
	ct.mu.Unlock()
	return e
}

// setError records the last error for a HyperType handle.
func (ch *cHyperType) setError(err error) {
	ch.mu.Lock()
	ch.lastError = err.Error()
	ch.mu.Unlock()
}

// createHandle allocates a cgo.Handle and stores it in C-heap memory
// (a malloc'd uintptr_t) so the Go GC does not move or collect the pointer
// that C code holds onto. This follows the ADBC driver pattern.
func createHandle(h cgo.Handle) unsafe.Pointer {
	p := C.malloc(C.size_t(unsafe.Sizeof(uintptr(0))))
	*(*uintptr)(p) = uintptr(h)
	return p
}

// getFromHandle retrieves the Go object stored behind a C-heap handle pointer.
func getFromHandle[T any](ptr unsafe.Pointer) *T {
	h := cgo.Handle(*(*uintptr)(ptr))
	return h.Value().(*T)
}

// freeHandle deletes the cgo.Handle and frees the C-heap pointer.
func freeHandle(ptr unsafe.Pointer) {
	h := cgo.Handle(*(*uintptr)(ptr))
	h.Delete()
	C.free(ptr)
}

// cString returns a C string allocated on the C heap. Caller must free.
func cString(s string) *C.char {
	return C.CString(s)
}

// globalLastError stores the most recent error that occurred before a handle
// was created (e.g. during NewFromFile). Protected by globalMu.
var (
	globalLastError string
	globalMu        sync.Mutex
)

// setGlobalErr stores an error message that can be retrieved via BufarrowGetGlobalError.
func setGlobalErr(err error) {
	globalMu.Lock()
	globalLastError = err.Error()
	globalMu.Unlock()
}

// getGlobalErr returns and clears the global error.
func getGlobalErr() string {
	globalMu.Lock()
	e := globalLastError
	globalLastError = ""
	globalMu.Unlock()
	return e
}
