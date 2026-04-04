//go:build cgo

package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
	bufarrowlib "github.com/loicalleyne/bufarrowlib"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
)

// poolTestDir returns the python test fixtures directory relative to this file.
func poolTestDir() string {
	_, f, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(f), "..", "python", "tests", "fixtures")
}

// newPoolBase returns a base Transcoder for TestMsg pool tests.
// HyperType is configured so AppendRaw (poolModeRaw) works correctly.
func newPoolBase(t testing.TB) *bufarrowlib.Transcoder {
	t.Helper()
	dir := poolTestDir()
	alloc := mallocator.NewMallocator()

	// Compile descriptor for HyperType.
	fd, err := bufarrowlib.CompileProtoToFileDescriptor("test_msg.proto", []string{dir})
	if err != nil {
		t.Fatalf("CompileProtoToFileDescriptor: %v", err)
	}
	md, err := bufarrowlib.GetMessageDescriptorByName(fd, "TestMsg")
	if err != nil {
		t.Fatalf("GetMessageDescriptorByName: %v", err)
	}
	ht := bufarrowlib.NewHyperType(md, bufarrowlib.WithAutoRecompile(0, 1.0))

	tc, err := bufarrowlib.NewFromFile("test_msg.proto", "TestMsg", []string{dir}, alloc,
		bufarrowlib.WithHyperType(ht),
	)
	if err != nil {
		t.Fatalf("NewFromFile TestMsg: %v", err)
	}
	t.Cleanup(func() { tc.Release() })
	return tc
}

// newOrderPoolBase returns a base Transcoder for Order denorm pool tests.
// HyperType is configured so AppendDenormRaw (poolModeDenorm) works correctly.
func newOrderPoolBase(t testing.TB) *bufarrowlib.Transcoder {
	t.Helper()
	dir := poolTestDir()
	alloc := mallocator.NewMallocator()

	fd, err := bufarrowlib.CompileProtoToFileDescriptor("order.proto", []string{dir})
	if err != nil {
		t.Fatalf("CompileProtoToFileDescriptor Order: %v", err)
	}
	md, err := bufarrowlib.GetMessageDescriptorByName(fd, "Order")
	if err != nil {
		t.Fatalf("GetMessageDescriptorByName Order: %v", err)
	}
	ht := bufarrowlib.NewHyperType(md, bufarrowlib.WithAutoRecompile(0, 1.0))

	tc, err := bufarrowlib.NewFromFile("order.proto", "Order", []string{dir}, alloc,
		bufarrowlib.WithHyperType(ht),
		bufarrowlib.WithDenormalizerPlan(
			pbpath.PlanPath("name"),
			pbpath.PlanPath("items[*].id"),
			pbpath.PlanPath("items[*].price"),
			pbpath.PlanPath("seq"),
		),
	)
	if err != nil {
		t.Fatalf("NewFromFile Order: %v", err)
	}
	t.Cleanup(func() { tc.Release() })
	return tc
}

// ── proto wire-format helpers ─────────────────────────────────────────────

func poolEncVarint(v uint64) []byte {
	var out []byte
	for v > 0x7F {
		out = append(out, byte(v&0x7F)|0x80)
		v >>= 7
	}
	return append(out, byte(v))
}

// poolEncodeTestMsg encodes TestMsg{name, age} in protobuf wire format.
func poolEncodeTestMsg(name string, age int) []byte {
	var buf []byte
	if name != "" {
		b := []byte(name)
		buf = append(buf, 0x0A)
		buf = append(buf, poolEncVarint(uint64(len(b)))...)
		buf = append(buf, b...)
	}
	if age != 0 {
		buf = append(buf, 0x10)
		buf = append(buf, poolEncVarint(uint64(age))...)
	}
	return buf
}

// poolEncodeItem encodes Item{id, price} in protobuf wire format.
func poolEncodeItem(id string, price float64) []byte {
	var buf []byte
	if id != "" {
		b := []byte(id)
		buf = append(buf, 0x0A)
		buf = append(buf, poolEncVarint(uint64(len(b)))...)
		buf = append(buf, b...)
	}
	if price != 0 {
		bits := math.Float64bits(price)
		var b8 [8]byte
		binary.LittleEndian.PutUint64(b8[:], bits)
		buf = append(buf, 0x11) // field 2, wire type 1 (fixed64)
		buf = append(buf, b8[:]...)
	}
	return buf
}

// poolEncodeOrder encodes Order{name, nItems items} in protobuf wire format.
func poolEncodeOrder(name string, nItems int) []byte {
	var buf []byte
	if name != "" {
		b := []byte(name)
		buf = append(buf, 0x0A)
		buf = append(buf, poolEncVarint(uint64(len(b)))...)
		buf = append(buf, b...)
	}
	for i := range nItems {
		item := poolEncodeItem(fmt.Sprintf("item-%d", i), float64(i)+0.99)
		buf = append(buf, 0x12) // field 2, wire type 2
		buf = append(buf, poolEncVarint(uint64(len(item)))...)
		buf = append(buf, item...)
	}
	return buf
}

// ── pool construction ──────────────────────────────────────────────────────

func TestNewCPool_Basic(t *testing.T) {
	base := newPoolBase(t)
	pool, err := newCPool(base, 2, 0, poolModeRaw)
	if err != nil {
		t.Fatalf("newCPool: %v", err)
	}
	defer pool.release()

	if pool.workers != 2 {
		t.Errorf("workers = %d, want 2", pool.workers)
	}
	if len(pool.clones) != 2 {
		t.Errorf("clones = %d, want 2", len(pool.clones))
	}
	if !pool.running {
		t.Error("pool should be running after newCPool")
	}
}

func TestNewCPool_DefaultCapacity(t *testing.T) {
	base := newPoolBase(t)
	pool, err := newCPool(base, 4, 0, poolModeRaw) // capacity 0 → workers*64
	if err != nil {
		t.Fatalf("newCPool: %v", err)
	}
	defer pool.release()
	if cap(pool.jobs) != 4*64 {
		t.Errorf("channel capacity = %d, want %d", cap(pool.jobs), 4*64)
	}
}

func TestNewCPool_ExplicitCapacity(t *testing.T) {
	base := newPoolBase(t)
	pool, err := newCPool(base, 2, 128, poolModeRaw)
	if err != nil {
		t.Fatalf("newCPool: %v", err)
	}
	defer pool.release()
	if cap(pool.jobs) != 128 {
		t.Errorf("channel capacity = %d, want 128", cap(pool.jobs))
	}
}

// ── submit and flush ──────────────────────────────────────────────────────

func TestPool_SubmitFlush_Empty(t *testing.T) {
	base := newPoolBase(t)
	pool, err := newCPool(base, 2, 0, poolModeRaw)
	if err != nil {
		t.Fatalf("newCPool: %v", err)
	}
	defer pool.release()

	rb, err := pool.flush()
	if err != nil {
		t.Fatalf("flush empty: %v", err)
	}
	defer rb.Release()
	if rb.NumRows() != 0 {
		t.Errorf("empty flush NumRows = %d, want 0", rb.NumRows())
	}
	if rb.Schema() == nil {
		t.Error("schema is nil after empty flush")
	}
}

func TestPool_SubmitFlush_KnownRowCount(t *testing.T) {
	const nMsgs = 200
	base := newPoolBase(t)
	pool, err := newCPool(base, 4, 0, poolModeRaw)
	if err != nil {
		t.Fatalf("newCPool: %v", err)
	}
	defer pool.release()

	for i := range nMsgs {
		msg := poolEncodeTestMsg(fmt.Sprintf("user-%d", i), i%50+1)
		if err := pool.submit(msg, nil); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	rb, err := pool.flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	defer rb.Release()
	if rb.NumRows() != int64(nMsgs) {
		t.Errorf("NumRows = %d, want %d", rb.NumRows(), nMsgs)
	}
}

func TestPool_MultipleFlushWindows(t *testing.T) {
	const nPerWindow = 50
	const windows = 4
	base := newPoolBase(t)
	pool, err := newCPool(base, 2, 0, poolModeRaw)
	if err != nil {
		t.Fatalf("newCPool: %v", err)
	}
	defer pool.release()

	for w := range windows {
		for i := range nPerWindow {
			msg := poolEncodeTestMsg(fmt.Sprintf("u%d-%d", w, i), i+1)
			if err := pool.submit(msg, nil); err != nil {
				t.Fatalf("window %d submit %d: %v", w, i, err)
			}
		}
		rb, err := pool.flush()
		if err != nil {
			t.Fatalf("window %d flush: %v", w, err)
		}
		if rb.NumRows() != int64(nPerWindow) {
			t.Errorf("window %d NumRows = %d, want %d", w, rb.NumRows(), nPerWindow)
		}
		rb.Release()
	}
}

func TestPool_PendingCounter(t *testing.T) {
	base := newPoolBase(t)
	pool, err := newCPool(base, 1, 2048, poolModeRaw)
	if err != nil {
		t.Fatalf("newCPool: %v", err)
	}
	defer pool.release()

	if pool.pending.Load() != 0 {
		t.Errorf("initial pending = %d, want 0", pool.pending.Load())
	}
	for i := range 20 {
		msg := poolEncodeTestMsg(fmt.Sprintf("u-%d", i), i+1)
		if err := pool.submit(msg, nil); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	rb, err := pool.flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	rb.Release()
	if pool.pending.Load() != 0 {
		t.Errorf("after flush pending = %d, want 0", pool.pending.Load())
	}
}

func TestPool_SingleWorker(t *testing.T) {
	const nMsgs = 50
	base := newPoolBase(t)
	pool, err := newCPool(base, 1, 0, poolModeRaw)
	if err != nil {
		t.Fatalf("newCPool: %v", err)
	}
	defer pool.release()

	for i := range nMsgs {
		pool.submit(poolEncodeTestMsg(fmt.Sprintf("u%d", i), i+1), nil)
	}
	rb, err := pool.flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	defer rb.Release()
	if rb.NumRows() != int64(nMsgs) {
		t.Errorf("NumRows = %d, want %d", rb.NumRows(), nMsgs)
	}
}

// ── denorm pool path ──────────────────────────────────────────────────────

func TestPool_Denorm_SubmitFlush(t *testing.T) {
	const nOrders = 10
	const itemsPerOrder = 3
	base := newOrderPoolBase(t)
	pool, err := newCPool(base, 2, 0, poolModeDenorm)
	if err != nil {
		t.Fatalf("newCPool denorm: %v", err)
	}
	defer pool.release()

	for i := range nOrders {
		msg := poolEncodeOrder(fmt.Sprintf("order-%d", i), itemsPerOrder)
		if err := pool.submit(msg, nil); err != nil {
			t.Fatalf("submit order %d: %v", i, err)
		}
	}
	rb, err := pool.flush()
	if err != nil {
		t.Fatalf("flush denorm: %v", err)
	}
	defer rb.Release()
	want := int64(nOrders * itemsPerOrder)
	if rb.NumRows() != want {
		t.Errorf("denorm NumRows = %d, want %d", rb.NumRows(), want)
	}
}

func TestPool_Denorm_MultipleWindows(t *testing.T) {
	const nOrders = 5
	const itemsPerOrder = 3
	const windows = 3
	base := newOrderPoolBase(t)
	pool, err := newCPool(base, 2, 0, poolModeDenorm)
	if err != nil {
		t.Fatalf("newCPool denorm: %v", err)
	}
	defer pool.release()

	for w := range windows {
		for i := range nOrders {
			msg := poolEncodeOrder(fmt.Sprintf("w%d-order-%d", w, i), itemsPerOrder)
			if err := pool.submit(msg, nil); err != nil {
				t.Fatalf("window %d submit %d: %v", w, i, err)
			}
		}
		rb, err := pool.flush()
		if err != nil {
			t.Fatalf("window %d flush denorm: %v", w, err)
		}
		want := int64(nOrders * itemsPerOrder)
		if rb.NumRows() != want {
			t.Errorf("window %d denorm NumRows = %d, want %d", w, rb.NumRows(), want)
		}
		rb.Release()
	}
}

// ── lifecycle ─────────────────────────────────────────────────────────────

func TestPool_Release_Idempotent(t *testing.T) {
	base := newPoolBase(t)
	pool, err := newCPool(base, 1, 0, poolModeRaw)
	if err != nil {
		t.Fatalf("newCPool: %v", err)
	}
	pool.release()
	pool.release() // must not panic
}

func TestPool_FlushAfterRelease_ReturnsError(t *testing.T) {
	base := newPoolBase(t)
	pool, err := newCPool(base, 1, 0, poolModeRaw)
	if err != nil {
		t.Fatalf("newCPool: %v", err)
	}
	pool.release()
	_, err = pool.flush()
	if err == nil {
		t.Error("expected error flushing after release, got nil")
	}
}

func TestPool_SubmitAfterRelease_ReturnsError(t *testing.T) {
	base := newPoolBase(t)
	pool, err := newCPool(base, 1, 0, poolModeRaw)
	if err != nil {
		t.Fatalf("newCPool: %v", err)
	}
	pool.release()
	err = pool.submit(poolEncodeTestMsg("x", 1), nil)
	if err == nil {
		t.Error("expected error submitting after release, got nil")
	}
}

// ── merge helper ──────────────────────────────────────────────────────────

func TestMergeRecordBatches_NilInput(t *testing.T) {
	_, err := mergeRecordBatches(nil, nil)
	if err == nil {
		t.Error("expected error merging zero batches")
	}
}

func TestMergeRecordBatches_SingleBatch(t *testing.T) {
	const nMsgs = 5
	base := newPoolBase(t)
	pool, err := newCPool(base, 1, 0, poolModeRaw)
	if err != nil {
		t.Fatalf("newCPool: %v", err)
	}
	defer pool.release()
	for i := range nMsgs {
		pool.submit(poolEncodeTestMsg(fmt.Sprintf("x-%d", i), i+1), nil)
	}
	rb, err := pool.flush()
	if err != nil {
		t.Fatalf("flush single: %v", err)
	}
	defer rb.Release()
	if rb.NumRows() != int64(nMsgs) {
		t.Errorf("NumRows = %d, want %d", rb.NumRows(), nMsgs)
	}
}

// ── hasDenorm helper ──────────────────────────────────────────────────────

func TestHasDenorm(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{``, false},
		{`{"custom_proto":"foo.proto"}`, false},
		{`{"denorm_columns":["name"]}`, true},
		{`{"other":"val","denorm_columns":[]}`, true},
	}
	for _, c := range cases {
		got := hasDenorm(c.in)
		if got != c.want {
			t.Errorf("hasDenorm(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
