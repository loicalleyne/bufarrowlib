package bufarrowlib

// ══════════════════════════════════════════════════════════════════════
// Correctness: bufarrowlib vs arrow/util.ProtobufMessageReflection (PMR)
// ══════════════════════════════════════════════════════════════════════
//
// These tests verify schema and value equivalence between bufarrowlib's
// Transcoder and the upstream ProtobufMessageReflection from arrow-go/v18.
// They serve as a living spec for where the two APIs agree and diverge.

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
	"github.com/loicalleyne/bufarrowlib/gen/go/samples"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ── Schema comparison: ScalarTypes ──────────────────────────────────

// TestSchema_VsPMR_ScalarTypes verifies that bufarrowlib and PMR produce
// the same field names and compatible types for a flat message containing
// only proto3 scalar types (no enums, no well-known types, no maps).
func TestSchema_VsPMR_ScalarTypes(t *testing.T) {
	msg := &samples.ScalarTypes{
		Double:   1.0,
		Float:    2.0,
		Int32:    3,
		Int64:    4,
		Uint32:   5,
		Uint64:   6,
		Sint32:   7,
		Sint64:   8,
		Fixed32:  9,
		Fixed64:  10,
		Sfixed32: 11,
		Sfixed64: 12,
		Bool:     true,
		String_:  "hello",
		Bytes:    []byte("world"),
	}

	// --- PMR schema ---
	pmr := arrowutil.NewProtobufMessageReflection(msg)
	pmrSchema := pmr.Schema()

	// --- bufarrowlib schema ---
	md := msg.ProtoReflect().Descriptor()
	tc, err := New(md, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()
	baSchema := tc.Schema()

	// Field count must agree.
	if pmrSchema.NumFields() != baSchema.NumFields() {
		t.Fatalf("field count: PMR=%d bufarrowlib=%d", pmrSchema.NumFields(), baSchema.NumFields())
	}

	// Field names must agree (order may differ, so build lookup maps).
	pmrFields := make(map[string]arrow.DataType, pmrSchema.NumFields())
	for i := range pmrSchema.NumFields() {
		f := pmrSchema.Field(i)
		pmrFields[f.Name] = f.Type
	}

	baFields := make(map[string]arrow.DataType, baSchema.NumFields())
	for i := range baSchema.NumFields() {
		f := baSchema.Field(i)
		baFields[f.Name] = f.Type
	}

	for name := range pmrFields {
		if _, ok := baFields[name]; !ok {
			t.Errorf("field %q in PMR but missing in bufarrowlib", name)
		}
	}
	for name := range baFields {
		if _, ok := pmrFields[name]; !ok {
			t.Errorf("field %q in bufarrowlib but missing in PMR", name)
		}
	}

	// Log both schemas for visual inspection.
	t.Logf("PMR schema:\n%s", pmrSchema)
	t.Logf("bufarrowlib schema:\n%s", baSchema)
}

// ── Schema comparison: Known types (Timestamp, Duration) ────────────

// TestSchema_VsPMR_TypeDifferences documents the type mapping for
// well-known protobuf types (Timestamp, Duration). Both libraries
// currently map these as Struct{seconds int64, nanos int32}.
// This test acts as a living spec — if either side changes its mapping
// it will surface here.
func TestSchema_VsPMR_TypeDifferences(t *testing.T) {
	msg := &samples.Known{
		Ts:       timestamppb.Now(),
		Duration: durationpb.New(42),
	}

	pmr := arrowutil.NewProtobufMessageReflection(msg)
	pmrSchema := pmr.Schema()

	md := msg.ProtoReflect().Descriptor()
	tc, err := New(md, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()
	baSchema := tc.Schema()

	t.Logf("PMR schema:\n%s", pmrSchema)
	t.Logf("bufarrowlib schema:\n%s", baSchema)

	// Verify both schemas have the same number of fields.
	if pmrSchema.NumFields() != baSchema.NumFields() {
		t.Errorf("field count: PMR=%d bufarrowlib=%d", pmrSchema.NumFields(), baSchema.NumFields())
	}

	// Check each field by name and compare types.
	for i := range pmrSchema.NumFields() {
		name := pmrSchema.Field(i).Name
		pmrType := pmrSchema.Field(i).Type

		baIdx := baSchema.FieldIndices(name)
		if len(baIdx) == 0 {
			t.Errorf("field %q in PMR but missing in bufarrowlib", name)
			continue
		}
		baType := baSchema.Field(baIdx[0]).Type

		if arrow.TypeEqual(pmrType, baType) {
			t.Logf("field %q: both map to %s", name, pmrType)
		} else {
			t.Logf("field %q diverges: PMR=%s, bufarrowlib=%s", name, pmrType, baType)
		}
	}
}

// ── Value comparison: ScalarTypes ───────────────────────────────────

// TestValues_VsPMR_ScalarTypes verifies that for a fully-populated
// ScalarTypes message the column values produced by bufarrowlib and PMR
// are identical for every scalar field.
//
// Known upstream bug: PMR's AppendValueOrNull (protobuf_reflect.go) is
// missing the arrow.FLOAT32 case. Messages with proto float fields
// trigger a nil-pointer panic in RecordFromStructArray because the Float32
// builder receives 0 appended values while all other builders receive 1,
// causing NewStructArray to return nil (array length mismatch). The error
// is silently ignored (structArray, _ := ...).
// ScalarTypes has a float field, so PMR.Record() panics for this type.
func TestValues_VsPMR_ScalarTypes(t *testing.T) {
	msg := &samples.ScalarTypes{
		Double:   3.14,
		Float:    2.71,
		Int32:    -42,
		Int64:    -1234567890,
		Uint32:   42,
		Uint64:   1234567890,
		Sint32:   -7,
		Sint64:   -8,
		Fixed32:  9,
		Fixed64:  10,
		Sfixed32: -11,
		Sfixed64: -12,
		Bool:     true,
		String_:  "test",
		Bytes:    []byte{0xDE, 0xAD},
	}

	mem := memory.DefaultAllocator

	// --- PMR record (protected: PMR panics on messages with float fields) ---
	var pmrRec arrow.RecordBatch
	var pmrPanicked bool
	func() {
		defer func() {
			if r := recover(); r != nil {
				pmrPanicked = true
				t.Logf("PMR.Record() panicked (expected — FLOAT32 bug): %v", r)
			}
		}()
		pmr := arrowutil.NewProtobufMessageReflection(msg)
		pmrRec = pmr.Record(mem)
	}()
	if pmrRec != nil {
		defer pmrRec.Release()
	}

	// --- bufarrowlib record ---
	md := msg.ProtoReflect().Descriptor()
	tc, err := New(md, mem)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()
	tc.Append(msg)
	baRec := tc.NewRecordBatch()
	defer baRec.Release()

	if baRec.NumRows() != 1 {
		t.Fatalf("bufarrowlib rows: %d, want 1", baRec.NumRows())
	}

	if pmrPanicked {
		// Still validate bufarrowlib produced a sane record.
		baSchema := baRec.Schema()
		if baSchema.NumFields() != 15 {
			t.Errorf("bufarrowlib field count: %d, want 15", baSchema.NumFields())
		}
		t.Skipf("PMR panicked (upstream FLOAT32 bug) — column-by-column comparison skipped; see TestValues_VsPMR_Known for working comparison")
		return
	}

	if pmrRec.NumRows() != 1 {
		t.Fatalf("PMR rows: %d, want 1", pmrRec.NumRows())
	}

	// For each PMR column, find the matching bufarrowlib column by name
	// and compare JSON-serialised values (type-agnostic comparison).
	pmrSchema := pmrRec.Schema()
	baSchema := baRec.Schema()

	for i := range pmrSchema.NumFields() {
		name := pmrSchema.Field(i).Name
		baIdxs := baSchema.FieldIndices(name)
		if len(baIdxs) == 0 {
			t.Errorf("field %q in PMR but not in bufarrowlib record", name)
			continue
		}

		pmrCol := pmrRec.Column(i)
		baCol := baRec.Column(baIdxs[0])

		pmrJSON, err1 := pmrCol.MarshalJSON()
		baJSON, err2 := baCol.MarshalJSON()
		if err1 != nil || err2 != nil {
			t.Errorf("field %q: JSON marshal error: PMR=%v, BA=%v", name, err1, err2)
			continue
		}

		if string(pmrJSON) != string(baJSON) {
			t.Errorf("field %q values differ:\n  PMR: %s\n  BA:  %s", name, pmrJSON, baJSON)
		}
	}
}

// ── Value comparison: Known (Timestamp, Duration) ───────────────────

// TestValues_VsPMR_Known performs a full column-by-column value comparison
// between PMR and bufarrowlib for the Known message type (Timestamp +
// Duration). Known has no proto float fields, so PMR.Record() works.
func TestValues_VsPMR_Known(t *testing.T) {
	msg := &samples.Known{
		Ts:       &timestamppb.Timestamp{Seconds: 1718000000, Nanos: 123456789},
		Duration: &durationpb.Duration{Seconds: 3600, Nanos: 500000000},
	}

	mem := memory.DefaultAllocator

	// --- PMR record ---
	pmr := arrowutil.NewProtobufMessageReflection(msg)
	pmrRec := pmr.Record(mem)
	defer pmrRec.Release()

	if pmrRec.NumRows() != 1 {
		t.Fatalf("PMR rows: %d, want 1", pmrRec.NumRows())
	}

	// --- bufarrowlib record ---
	md := msg.ProtoReflect().Descriptor()
	tc, err := New(md, mem)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()
	tc.Append(msg)
	baRec := tc.NewRecordBatch()
	defer baRec.Release()

	if baRec.NumRows() != 1 {
		t.Fatalf("bufarrowlib rows: %d, want 1", baRec.NumRows())
	}

	// Compare columns by name.
	pmrSchema := pmrRec.Schema()
	baSchema := baRec.Schema()

	t.Logf("PMR record: %d cols", pmrRec.NumCols())
	t.Logf("bufarrowlib record: %d cols", baRec.NumCols())

	for i := range pmrSchema.NumFields() {
		name := pmrSchema.Field(i).Name
		baIdxs := baSchema.FieldIndices(name)
		if len(baIdxs) == 0 {
			t.Errorf("field %q in PMR but not in bufarrowlib record", name)
			continue
		}

		pmrCol := pmrRec.Column(i)
		baCol := baRec.Column(baIdxs[0])

		pmrJSON, err1 := pmrCol.MarshalJSON()
		baJSON, err2 := baCol.MarshalJSON()
		if err1 != nil || err2 != nil {
			t.Errorf("field %q: JSON marshal error: PMR=%v, BA=%v", name, err1, err2)
			continue
		}

		if string(pmrJSON) != string(baJSON) {
			t.Errorf("field %q values differ:\n  PMR: %s\n  BA:  %s", name, pmrJSON, baJSON)
		} else {
			t.Logf("field %q: values match (%s)", name, string(pmrJSON))
		}
	}
}
