package bufarrowlib

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib/gen/go/samples"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// --- Schema tests ---

func TestMessage_ScalarMap(t *testing.T) {
	m := &samples.ScalarMap{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/scalar_map.txt", schema)
}

func TestMessage_MapVariety(t *testing.T) {
	m := &samples.MapVariety{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/map_variety.txt", schema, struct{}{})
}

func TestMessage_MapWithNested(t *testing.T) {
	m := &samples.MapWithNested{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/map_nested.txt", schema, struct{}{})
}

func TestMessage_MapWithTimestamp(t *testing.T) {
	m := &samples.MapWithTimestamp{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/map_timestamp.txt", schema, struct{}{})
}

func TestMessage_NestedWithMap(t *testing.T) {
	m := &samples.NestedWithMap{}
	msg := build(m.ProtoReflect())
	schema := msg.schema.String()
	match(t, "testdata/nested_with_map.txt", schema, struct{}{})
}

// --- Append tests ---

func TestAppendMessage_ScalarMap(t *testing.T) {
	msg := &samples.ScalarMap{}
	b := build(msg.ProtoReflect())
	b.build(memory.DefaultAllocator)

	// Row 0: empty map (zero value)
	b.append(msg.ProtoReflect())

	// Row 1: one entry
	msg.Labels = map[string]string{"key": "value"}
	b.append(msg.ProtoReflect())

	r := b.NewRecordBatch()
	data, err := r.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	match(t, "testdata/scalar_map.json", string(data))
}

func TestAppendMessage_MapVariety(t *testing.T) {
	msg := &samples.MapVariety{}
	b := build(msg.ProtoReflect())
	b.build(memory.DefaultAllocator)

	// Row 0: empty maps
	b.append(msg.ProtoReflect())

	// Row 1: populated maps
	msg.StringMap = map[string]string{"a": "b"}
	msg.StringIntMap = map[string]int32{"x": 42}
	msg.IntStringMap = map[int32]string{7: "seven"}
	msg.Int64DoubleMap = map[int64]float64{100: 3.14}
	msg.BoolStringMap = map[bool]string{true: "yes"}
	msg.UintMap = map[uint32]uint64{1: 1000}
	b.append(msg.ProtoReflect())

	r := b.NewRecordBatch()
	data, err := r.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	matchJSON(t, "testdata/map_variety.json", string(data), struct{}{})
}

func TestAppendMessage_MapWithNested(t *testing.T) {
	msg := &samples.MapWithNested{}
	b := build(msg.ProtoReflect())
	b.build(memory.DefaultAllocator)

	// Row 0: empty
	b.append(msg.ProtoReflect())

	// Row 1: map with nested message value
	msg.NestedMap = map[string]*samples.Three{
		"first": {Value: 42},
	}
	b.append(msg.ProtoReflect())

	r := b.NewRecordBatch()
	data, err := r.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	matchJSON(t, "testdata/map_nested.json", string(data), struct{}{})
}

func TestAppendMessage_MapWithTimestamp(t *testing.T) {
	msg := &samples.MapWithTimestamp{}
	b := build(msg.ProtoReflect())
	b.build(memory.DefaultAllocator)

	// Row 0: empty
	b.append(msg.ProtoReflect())

	// Row 1: map with well-known timestamp value
	x, _ := time.Parse(time.RFC822, time.RFC822)
	msg.TsMap = map[string]*timestamppb.Timestamp{
		"t1": timestamppb.New(x),
	}
	b.append(msg.ProtoReflect())

	r := b.NewRecordBatch()
	data, err := r.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	matchJSON(t, "testdata/map_timestamp.json", string(data), struct{}{})
}

func TestAppendMessage_NestedWithMap(t *testing.T) {
	msg := &samples.NestedWithMap{}
	b := build(msg.ProtoReflect())
	b.build(memory.DefaultAllocator)

	// Row 0: empty
	b.append(msg.ProtoReflect())

	// Row 1: nested struct with inner map
	msg.Name = "test"
	msg.Inner = &samples.ScalarMap{
		Labels: map[string]string{"env": "prod"},
	}
	b.append(msg.ProtoReflect())

	r := b.NewRecordBatch()
	data, err := r.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	matchJSON(t, "testdata/nested_with_map.json", string(data), struct{}{})
}

// --- Round-trip (Proto) tests ---

func TestRoundTrip_ScalarMap(t *testing.T) {
	original := &samples.ScalarMap{
		Labels: map[string]string{"key1": "val1", "key2": "val2"},
	}

	tc, err := New(original.ProtoReflect().Descriptor(), memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	tc.Append(original)

	rec := tc.NewRecordBatch()
	defer rec.Release()

	msgs := tc.Proto(rec, nil)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	if !proto.Equal(original, msgs[0]) {
		wantJSON, _ := protojson.Marshal(original)
		gotJSON, _ := protojson.Marshal(msgs[0])
		t.Errorf("ScalarMap round-trip mismatch\n  want: %s\n  got:  %s", wantJSON, gotJSON)
	}
}

func TestRoundTrip_MapVariety(t *testing.T) {
	original := &samples.MapVariety{
		StringMap:      map[string]string{"a": "b", "c": "d"},
		StringIntMap:   map[string]int32{"x": 10, "y": 20},
		IntStringMap:   map[int32]string{1: "one", 2: "two"},
		Int64DoubleMap: map[int64]float64{100: 1.5, 200: 2.5},
		BoolStringMap:  map[bool]string{true: "yes", false: "no"},
		UintMap:        map[uint32]uint64{3: 300, 4: 400},
	}

	tc, err := New(original.ProtoReflect().Descriptor(), memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	tc.Append(original)

	rec := tc.NewRecordBatch()
	defer rec.Release()

	msgs := tc.Proto(rec, nil)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	if !proto.Equal(original, msgs[0]) {
		wantJSON, _ := protojson.Marshal(original)
		gotJSON, _ := protojson.Marshal(msgs[0])
		t.Errorf("MapVariety round-trip mismatch\n  want: %s\n  got:  %s", wantJSON, gotJSON)
	}
}

func TestRoundTrip_MapWithNested(t *testing.T) {
	original := &samples.MapWithNested{
		NestedMap: map[string]*samples.Three{
			"first":  {Value: 42},
			"second": {Value: 99},
		},
	}

	tc, err := New(original.ProtoReflect().Descriptor(), memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	tc.Append(original)

	rec := tc.NewRecordBatch()
	defer rec.Release()

	msgs := tc.Proto(rec, nil)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	if !proto.Equal(original, msgs[0]) {
		wantJSON, _ := protojson.Marshal(original)
		gotJSON, _ := protojson.Marshal(msgs[0])
		t.Errorf("MapWithNested round-trip mismatch\n  want: %s\n  got:  %s", wantJSON, gotJSON)
	}
}

func TestRoundTrip_MapWithTimestamp(t *testing.T) {
	x, _ := time.Parse(time.RFC822, time.RFC822)
	ts := timestamppb.New(x)
	original := &samples.MapWithTimestamp{
		TsMap: map[string]*timestamppb.Timestamp{
			"t1": ts,
		},
	}

	tc, err := New(original.ProtoReflect().Descriptor(), memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	tc.Append(original)

	rec := tc.NewRecordBatch()
	defer rec.Release()

	msgs := tc.Proto(rec, nil)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	// For well-known timestamps, compare via JSON since dynamic messages
	// represent timestamps differently than concrete types
	wantJSON, _ := protojson.Marshal(original)
	gotJSON, _ := protojson.Marshal(msgs[0])
	if string(wantJSON) != string(gotJSON) {
		t.Errorf("MapWithTimestamp round-trip mismatch\n  want: %s\n  got:  %s", wantJSON, gotJSON)
	}
}

func TestRoundTrip_EmptyMap(t *testing.T) {
	original := &samples.ScalarMap{} // nil map

	tc, err := New(original.ProtoReflect().Descriptor(), memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	tc.Append(original)

	rec := tc.NewRecordBatch()
	defer rec.Release()

	msgs := tc.Proto(rec, nil)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	if !proto.Equal(original, msgs[0]) {
		wantJSON, _ := protojson.Marshal(original)
		gotJSON, _ := protojson.Marshal(msgs[0])
		t.Errorf("EmptyMap round-trip mismatch\n  want: %s\n  got:  %s", wantJSON, gotJSON)
	}
}

func TestRoundTrip_MultipleRows_Map(t *testing.T) {
	tc, err := New(
		new(samples.ScalarMap).ProtoReflect().Descriptor(),
		memory.DefaultAllocator,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	inputs := []*samples.ScalarMap{
		{},
		{Labels: map[string]string{"a": "1"}},
		{Labels: map[string]string{"b": "2", "c": "3"}},
	}

	for _, m := range inputs {
		tc.Append(m)
	}

	rec := tc.NewRecordBatch()
	defer rec.Release()

	if rec.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", rec.NumRows())
	}

	msgs := tc.Proto(rec, nil)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}

	for i, input := range inputs {
		if !proto.Equal(input, msgs[i]) {
			wantJSON, _ := protojson.Marshal(input)
			gotJSON, _ := protojson.Marshal(msgs[i])
			t.Errorf("row %d mismatch\n  want: %s\n  got:  %s", i, wantJSON, gotJSON)
		}
	}
}

// --- Schema type verification ---

func TestMapSchemaType(t *testing.T) {
	tc, err := New(
		new(samples.ScalarMap).ProtoReflect().Descriptor(),
		memory.DefaultAllocator,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	schema := tc.Schema()
	if schema.NumFields() != 1 {
		t.Fatalf("expected 1 field, got %d", schema.NumFields())
	}

	f := schema.Field(0)
	if f.Name != "labels" {
		t.Errorf("field name = %q, want %q", f.Name, "labels")
	}

	mt, ok := f.Type.(*arrow.MapType)
	if !ok {
		t.Fatalf("expected MapType, got %T", f.Type)
	}

	if mt.KeyType() != arrow.BinaryTypes.String {
		t.Errorf("key type = %v, want utf8", mt.KeyType())
	}
	if mt.ItemType() != arrow.BinaryTypes.String {
		t.Errorf("item type = %v, want utf8", mt.ItemType())
	}
}

func TestMapVarietySchemaTypes(t *testing.T) {
	tc, err := New(
		new(samples.MapVariety).ProtoReflect().Descriptor(),
		memory.DefaultAllocator,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	schema := tc.Schema()

	expected := []struct {
		name    string
		keyType arrow.DataType
		valType arrow.DataType
	}{
		{"string_map", arrow.BinaryTypes.String, arrow.BinaryTypes.String},
		{"string_int_map", arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32},
		{"int_string_map", arrow.PrimitiveTypes.Int32, arrow.BinaryTypes.String},
		{"int64_double_map", arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Float64},
		{"bool_string_map", arrow.FixedWidthTypes.Boolean, arrow.BinaryTypes.String},
		{"uint_map", arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Uint64},
	}

	for i, exp := range expected {
		f := schema.Field(i)
		if f.Name != exp.name {
			t.Errorf("field %d: name = %q, want %q", i, f.Name, exp.name)
		}
		mt, ok := f.Type.(*arrow.MapType)
		if !ok {
			t.Errorf("field %d: expected MapType, got %T", i, f.Type)
			continue
		}
		if mt.KeyType() != exp.keyType {
			t.Errorf("field %d (%s): key type = %v, want %v", i, exp.name, mt.KeyType(), exp.keyType)
		}
		if mt.ItemType() != exp.valType {
			t.Errorf("field %d (%s): item type = %v, want %v", i, exp.name, mt.ItemType(), exp.valType)
		}
	}
}

// --- Parquet schema test ---

func TestMapParquetSchema(t *testing.T) {
	m := &samples.ScalarMap{}
	msg := build(m.ProtoReflect())
	pqSchema := msg.Parquet()
	if pqSchema == nil {
		t.Fatal("Parquet() returned nil")
	}
	match(t, "testdata/scalar_map_parquet.txt", pqSchema.String(), struct{}{})
}

// --- Proto() round-trip type assertion test ---

func TestRoundTrip_MapProtoEquality(t *testing.T) {
	original := &samples.ScalarMap{
		Labels: map[string]string{"hello": "world"},
	}

	tc, err := New(original.ProtoReflect().Descriptor(), memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Release()

	tc.Append(original)

	rec := tc.NewRecordBatch()
	defer rec.Release()

	msgs := tc.Proto(rec, nil)
	got := msgs[0]

	if !proto.Equal(original, got) {
		t.Errorf("proto.Equal failed\n  got:  %v\n  want: %v", got, original)
	}
}
