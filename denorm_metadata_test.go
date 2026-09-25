package bufarrowlib

import (
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
)

const (
	metaPartition MetaCol = "kafka_partition"
	metaOffset    MetaCol = "kafka_offset"
)

func TestDenormMetadataSchemaOrderAndNullability(t *testing.T) {
	orderMD, _ := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("name", pbpath.Alias("order_name")),
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
		),
		WithDenormMetadataColumns(
			DenormMetadataColumn{Name: metaPartition, Type: arrow.PrimitiveTypes.Int32},
			DenormMetadataColumn{Name: metaOffset, Type: arrow.PrimitiveTypes.Int64},
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	sc := tc.DenormalizerSchema()
	if got := sc.Field(2).Name; got != string(metaPartition) {
		t.Fatalf("field 2 name = %q, want %q", got, metaPartition)
	}
	if got := sc.Field(3).Name; got != string(metaOffset) {
		t.Fatalf("field 3 name = %q, want %q", got, metaOffset)
	}
	if !sc.Field(2).Nullable || !sc.Field(3).Nullable {
		t.Fatalf("metadata columns must be nullable")
	}
}

func TestAppendDenormWithMetadataRowCountAndReplication(t *testing.T) {
	orderMD, itemMD := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(
			pbpath.PlanPath("items[*].id", pbpath.Alias("item_id")),
		),
		WithDenormMetadataColumns(
			DenormMetadataColumn{Name: metaPartition, Type: arrow.PrimitiveTypes.Int32},
			DenormMetadataColumn{Name: metaOffset, Type: arrow.PrimitiveTypes.Int64},
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	msg := makeOrder(t, orderMD, itemMD, "order", []struct {
		id    string
		price float64
	}{{"A", 1}, {"B", 2}, {"C", 3}}, nil, 1)

	rows, err := tc.AppendDenorm(msg,
		Meta(metaPartition, int32(4)),
		Meta(metaOffset, int64(99)),
	)
	if err != nil {
		t.Fatalf("AppendDenorm: %v", err)
	}
	if rows != 3 {
		t.Fatalf("rows = %d, want 3", rows)
	}

	rb := tc.NewDenormalizerRecordBatch()
	defer rb.Release()
	if rb.NumRows() != 3 {
		t.Fatalf("record rows = %d, want 3", rb.NumRows())
	}
	part := rb.Column(1)
	off := rb.Column(2)
	for i := 0; i < int(rb.NumRows()); i++ {
		if part.IsNull(i) || off.IsNull(i) {
			t.Fatalf("row %d metadata is null", i)
		}
	}
}

func TestAppendDenormMetadataOmittedBecomesNull(t *testing.T) {
	orderMD, itemMD := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(pbpath.PlanPath("items[*].id", pbpath.Alias("item_id"))),
		WithDenormMetadataColumns(
			DenormMetadataColumn{Name: metaPartition, Type: arrow.PrimitiveTypes.Int32},
			DenormMetadataColumn{Name: metaOffset, Type: arrow.PrimitiveTypes.Int64},
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	msg := makeOrder(t, orderMD, itemMD, "order", []struct {
		id    string
		price float64
	}{{"A", 1}}, nil, 1)

	rows, err := tc.AppendDenorm(msg, Meta(metaPartition, int32(1)))
	if err != nil {
		t.Fatalf("AppendDenorm: %v", err)
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}

	rb := tc.NewDenormalizerRecordBatch()
	defer rb.Release()
	if !rb.Column(2).IsNull(0) {
		t.Fatalf("omitted metadata column should be null")
	}
}

func TestAppendDenormMetadataAllNullPlanColumnsStillAppendsMetadata(t *testing.T) {
	orderMD, itemMD := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(pbpath.PlanPath("items[*].id", pbpath.Alias("item_id"))),
		WithDenormMetadataColumns(
			DenormMetadataColumn{Name: metaPartition, Type: arrow.PrimitiveTypes.Int32},
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	msg := makeOrder(t, orderMD, itemMD, "order", nil, nil, 1)
	rows, err := tc.AppendDenorm(msg, Meta(metaPartition, int32(7)))
	if err != nil {
		t.Fatalf("AppendDenorm: %v", err)
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}

	rb := tc.NewDenormalizerRecordBatch()
	defer rb.Release()
	if rb.Column(0).IsNull(0) == false {
		t.Fatalf("plan column expected null row")
	}
	if rb.Column(1).IsNull(0) {
		t.Fatalf("metadata should be populated even when plan columns are all null")
	}
}

func TestAppendDenormUnknownMetadataColumn(t *testing.T) {
	orderMD, itemMD := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(pbpath.PlanPath("name")),
		WithDenormMetadataColumns(DenormMetadataColumn{Name: metaPartition, Type: arrow.PrimitiveTypes.Int32}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	msg := makeOrder(t, orderMD, itemMD, "order", nil, nil, 1)
	if _, err := tc.AppendDenorm(msg, Meta(MetaCol("unknown"), int32(1))); err == nil {
		t.Fatal("expected unknown MetaCol error")
	}
}

func TestWithDenormMetadataColumnsRequiresPlan(t *testing.T) {
	orderMD, _ := buildDenormTestSchema(t)
	_, err := New(orderMD, memory.DefaultAllocator,
		WithDenormMetadataColumns(DenormMetadataColumn{Name: metaPartition, Type: arrow.PrimitiveTypes.Int32}),
	)
	if !errors.Is(err, ErrDenormMetadataWithoutPlan) {
		t.Fatalf("expected ErrDenormMetadataWithoutPlan, got %v", err)
	}
}

func TestWithDenormMetadataColumnsNameCollision(t *testing.T) {
	orderMD, _ := buildDenormTestSchema(t)
	_, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(pbpath.PlanPath("name", pbpath.Alias("dup"))),
		WithDenormMetadataColumns(DenormMetadataColumn{Name: MetaCol("dup"), Type: arrow.PrimitiveTypes.Int64}),
	)
	if err == nil {
		t.Fatal("expected name collision error")
	}
}

func TestDenormMetaColumnsReturnsCopy(t *testing.T) {
	orderMD, _ := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(pbpath.PlanPath("name", pbpath.Alias("order_name"))),
		WithDenormMetadataColumns(
			DenormMetadataColumn{Name: metaPartition, Type: arrow.PrimitiveTypes.Int32},
			DenormMetadataColumn{Name: MetaCol("event_ts"), Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}},
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	cols := tc.DenormMetaColumns()
	if len(cols) != 2 {
		t.Fatalf("len(cols) = %d, want 2", len(cols))
	}
	if cols[0].Name != metaPartition {
		t.Fatalf("cols[0].Name = %q, want %q", cols[0].Name, metaPartition)
	}
	if _, ok := cols[1].Type.(*arrow.TimestampType); !ok {
		t.Fatalf("cols[1].Type = %T, want *arrow.TimestampType", cols[1].Type)
	}

	// Must return a copy, not alias internal state.
	cols[0].Name = "mutated"
	again := tc.DenormMetaColumns()
	if again[0].Name != metaPartition {
		t.Fatalf("DenormMetaColumns leaked internal state, got %q", again[0].Name)
	}
}

func TestDenormMetaColumnsNilWhenUnconfigured(t *testing.T) {
	orderMD, _ := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(pbpath.PlanPath("name", pbpath.Alias("order_name"))),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	if got := tc.DenormMetaColumns(); got != nil {
		t.Fatalf("DenormMetaColumns() = %v, want nil", got)
	}
}

func TestAppendDenormTypedNilMetadataValueReturnsTypeError(t *testing.T) {
	orderMD, itemMD := buildDenormTestSchema(t)
	tc, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(pbpath.PlanPath("items[*].id", pbpath.Alias("item_id"))),
		WithDenormMetadataColumns(
			DenormMetadataColumn{Name: MetaCol("event_ts"), Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}},
		),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tc.Release()

	msg := makeOrder(t, orderMD, itemMD, "order", []struct {
		id    string
		price float64
	}{{"A", 1}}, nil, 1)

	var typedNil *time.Time
	if _, err := tc.AppendDenorm(msg, Meta(MetaCol("event_ts"), typedNil)); err == nil {
		t.Fatal("expected type error for typed nil metadata value")
	}
}

func TestCloneDenormMetadataIndependentBuilderState(t *testing.T) {
	orderMD, itemMD := buildDenormTestSchema(t)
	base, err := New(orderMD, memory.DefaultAllocator,
		WithDenormalizerPlan(pbpath.PlanPath("items[*].id", pbpath.Alias("item_id"))),
		WithDenormMetadataColumns(DenormMetadataColumn{Name: metaPartition, Type: arrow.PrimitiveTypes.Int32}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer base.Release()

	clone, err := base.Clone(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}
	defer clone.Release()

	msg := makeOrder(t, orderMD, itemMD, "order", []struct {
		id    string
		price float64
	}{{"A", 1}}, nil, 1)

	if _, err := base.AppendDenorm(msg, Meta(metaPartition, int32(1))); err != nil {
		t.Fatalf("base AppendDenorm: %v", err)
	}
	if _, err := clone.AppendDenorm(msg, Meta(metaPartition, int32(2))); err != nil {
		t.Fatalf("clone AppendDenorm: %v", err)
	}

	baseRB := base.NewDenormalizerRecordBatch()
	defer baseRB.Release()
	cloneRB := clone.NewDenormalizerRecordBatch()
	defer cloneRB.Release()

	if baseRB.Column(1).IsNull(0) || cloneRB.Column(1).IsNull(0) {
		t.Fatal("metadata columns should be populated")
	}
}
