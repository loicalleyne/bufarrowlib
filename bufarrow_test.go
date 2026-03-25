package bufarrowlib

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib/gen/go/samples"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestNew(t *testing.T) {
	three := new(samples.Three)
	schema, err := New(three.ProtoReflect().Descriptor(), memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	defer schema.Release()
	schema.Append(&samples.Three{
		Value: 10,
	})
	r := schema.NewRecordBatch()
	data, err := r.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	match(t, "testdata/new.json", string(data))
}

// --- NewFromFile tests ---

func TestNewFromFile(t *testing.T) {
	protoDir := testProtoDir(t)

	t.Run("success_scalar_types", func(t *testing.T) {
		schema, err := NewFromFile("samples.proto", "ScalarTypes", []string{protoDir}, memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("NewFromFile() error = %v", err)
		}
		defer schema.Release()

		fieldNames := schema.FieldNames()
		if len(fieldNames) != 15 {
			t.Errorf("expected 15 fields, got %d", len(fieldNames))
		}
	})

	t.Run("success_three", func(t *testing.T) {
		schema, err := NewFromFile("samples.proto", "Three", []string{protoDir}, memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("NewFromFile() error = %v", err)
		}
		defer schema.Release()

		fieldNames := schema.FieldNames()
		if len(fieldNames) != 1 || fieldNames[0] != "value" {
			t.Errorf("expected [value], got %v", fieldNames)
		}
	})

	t.Run("success_bidrequest", func(t *testing.T) {
		schema, err := NewFromFile("BidRequest.proto", "BidRequestEvent",
			[]string{protoDir, filepath.Join(protoDir, "custom")},
			memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("NewFromFile() error = %v", err)
		}
		defer schema.Release()

		fieldNames := schema.FieldNames()
		if len(fieldNames) == 0 {
			t.Error("expected at least one field in BidRequestEvent schema")
		}
	})

	t.Run("error_nonexistent_proto", func(t *testing.T) {
		_, err := NewFromFile("nonexistent.proto", "Foo", []string{protoDir}, memory.DefaultAllocator)
		if err == nil {
			t.Fatal("expected error for nonexistent proto file")
		}
	})

	t.Run("error_nonexistent_message", func(t *testing.T) {
		_, err := NewFromFile("samples.proto", "DoesNotExist", []string{protoDir}, memory.DefaultAllocator)
		if err == nil {
			t.Fatal("expected error for nonexistent message")
		}
	})
}

// --- WithCustomMessage tests ---

func TestNewWithCustomMessage(t *testing.T) {
	protoDir := testProtoDir(t)

	t.Run("success_adds_custom_fields", func(t *testing.T) {
		// Base: Three has field "value" (field number 1)
		baseMD := new(samples.Three).ProtoReflect().Descriptor()

		// Custom: CustomFields has "event_timestamp" and "source_id"
		customFD, err := CompileProtoToFileDescriptor("custom_fields.proto", []string{protoDir})
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		customMD, err := GetMessageDescriptorByName(customFD, "CustomFields")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		schema, err := New(baseMD, memory.DefaultAllocator, WithCustomMessage(customMD))
		if err != nil {
			t.Fatalf("New() with WithCustomMessage error = %v", err)
		}
		defer schema.Release()

		// Schema should have base + custom fields
		fieldNames := schema.FieldNames()
		want := map[string]bool{"value": false, "event_timestamp": false, "source_id": false}
		for _, name := range fieldNames {
			if _, ok := want[name]; ok {
				want[name] = true
			}
		}
		for name, found := range want {
			if !found {
				t.Errorf("expected field %q in schema, fields are: %v", name, fieldNames)
			}
		}
	})

	t.Run("success_stencil_custom_is_set", func(t *testing.T) {
		baseMD := new(samples.Three).ProtoReflect().Descriptor()

		customFD, err := CompileProtoToFileDescriptor("custom_fields.proto", []string{protoDir})
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		customMD, err := GetMessageDescriptorByName(customFD, "CustomFields")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		schema, err := New(baseMD, memory.DefaultAllocator, WithCustomMessage(customMD))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		defer schema.Release()

		if schema.stencilCustom == nil {
			t.Error("expected stencilCustom to be non-nil")
		}
	})

	t.Run("error_field_name_conflict", func(t *testing.T) {
		baseMD := new(samples.Three).ProtoReflect().Descriptor()

		// CustomFieldsConflict has "value" which conflicts with Three.value
		customFD, err := CompileProtoToFileDescriptor("custom_fields.proto", []string{protoDir})
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		customMD, err := GetMessageDescriptorByName(customFD, "CustomFieldsConflict")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		_, err = New(baseMD, memory.DefaultAllocator, WithCustomMessage(customMD))
		if err == nil {
			t.Fatal("expected field name conflict error")
		}
		if !strings.Contains(err.Error(), "field name conflict") {
			t.Errorf("expected 'field name conflict' error, got: %v", err)
		}
	})

	t.Run("no_custom_message_stencil_nil", func(t *testing.T) {
		baseMD := new(samples.Three).ProtoReflect().Descriptor()
		schema, err := New(baseMD, memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		defer schema.Release()

		if schema.stencilCustom != nil {
			t.Error("expected stencilCustom to be nil without custom fields")
		}
	})
}

// --- WithCustomMessageFile tests ---

func TestNewWithCustomMessageFile(t *testing.T) {
	protoDir := testProtoDir(t)

	t.Run("success", func(t *testing.T) {
		baseMD := new(samples.Three).ProtoReflect().Descriptor()

		schema, err := New(baseMD, memory.DefaultAllocator,
			WithCustomMessageFile("custom_fields.proto", "CustomFields", []string{protoDir}))
		if err != nil {
			t.Fatalf("New() with WithCustomMessageFile error = %v", err)
		}
		defer schema.Release()

		fieldNames := schema.FieldNames()
		want := map[string]bool{"value": false, "event_timestamp": false, "source_id": false}
		for _, name := range fieldNames {
			if _, ok := want[name]; ok {
				want[name] = true
			}
		}
		for name, found := range want {
			if !found {
				t.Errorf("expected field %q in schema, fields are: %v", name, fieldNames)
			}
		}
	})

	t.Run("error_nonexistent_proto", func(t *testing.T) {
		baseMD := new(samples.Three).ProtoReflect().Descriptor()

		_, err := New(baseMD, memory.DefaultAllocator,
			WithCustomMessageFile("nonexistent.proto", "Foo", []string{protoDir}))
		if err == nil {
			t.Fatal("expected error for nonexistent custom proto file")
		}
	})

	t.Run("error_nonexistent_message", func(t *testing.T) {
		baseMD := new(samples.Three).ProtoReflect().Descriptor()

		_, err := New(baseMD, memory.DefaultAllocator,
			WithCustomMessageFile("custom_fields.proto", "NonExistent", []string{protoDir}))
		if err == nil {
			t.Fatal("expected error for nonexistent message in custom proto file")
		}
	})

	t.Run("error_field_name_conflict", func(t *testing.T) {
		baseMD := new(samples.Three).ProtoReflect().Descriptor()

		_, err := New(baseMD, memory.DefaultAllocator,
			WithCustomMessageFile("custom_fields.proto", "CustomFieldsConflict", []string{protoDir}))
		if err == nil {
			t.Fatal("expected field name conflict error")
		}
	})
}

// --- Mutual exclusivity tests ---

func TestWithCustomMessage_MutualExclusivity(t *testing.T) {
	protoDir := testProtoDir(t)
	baseMD := new(samples.Three).ProtoReflect().Descriptor()

	customFD, err := CompileProtoToFileDescriptor("custom_fields.proto", []string{protoDir})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	customMD, err := GetMessageDescriptorByName(customFD, "CustomFields")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	_, err = New(baseMD, memory.DefaultAllocator,
		WithCustomMessage(customMD),
		WithCustomMessageFile("custom_fields.proto", "CustomFields", []string{protoDir}),
	)
	if err == nil {
		t.Fatal("expected mutual exclusivity error")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("expected 'mutually exclusive' error, got: %v", err)
	}
}

// --- NewFromFile + WithCustomMessageFile combined ---

func TestNewFromFileWithCustomMessageFile(t *testing.T) {
	protoDir := testProtoDir(t)

	schema, err := NewFromFile("samples.proto", "Three", []string{protoDir},
		memory.DefaultAllocator,
		WithCustomMessageFile("custom_fields.proto", "CustomFields", []string{protoDir}))
	if err != nil {
		t.Fatalf("NewFromFile() with WithCustomMessageFile error = %v", err)
	}
	defer schema.Release()

	fieldNames := schema.FieldNames()
	want := map[string]bool{"value": false, "event_timestamp": false, "source_id": false}
	for _, name := range fieldNames {
		if _, ok := want[name]; ok {
			want[name] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("expected field %q in schema, fields are: %v", name, fieldNames)
		}
	}
}

// --- AppendWithCustom tests ---

func TestAppendWithCustom(t *testing.T) {
	protoDir := testProtoDir(t)

	t.Run("success", func(t *testing.T) {
		baseMD := new(samples.Three).ProtoReflect().Descriptor()

		customFD, err := CompileProtoToFileDescriptor("custom_fields.proto", []string{protoDir})
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		customMD, err := GetMessageDescriptorByName(customFD, "CustomFields")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		schema, err := New(baseMD, memory.DefaultAllocator, WithCustomMessage(customMD))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		defer schema.Release()

		// Create base message
		baseMsg := &samples.Three{Value: 42}

		// Create custom message using dynamicpb
		customMsg := dynamicpb.NewMessage(customMD)
		customMsg.Set(customMD.Fields().ByName("event_timestamp"), protoreflect.ValueOfInt64(1234567890))
		customMsg.Set(customMD.Fields().ByName("source_id"), protoreflect.ValueOfString("test-source"))

		err = schema.AppendWithCustom(baseMsg, customMsg)
		if err != nil {
			t.Fatalf("AppendWithCustom() error = %v", err)
		}

		r := schema.NewRecordBatch()
		if r.NumRows() != 1 {
			t.Errorf("expected 1 row, got %d", r.NumRows())
		}
		if r.NumCols() != 3 {
			t.Errorf("expected 3 columns (value, event_timestamp, source_id), got %d", r.NumCols())
		}
	})

	t.Run("success_multiple_appends", func(t *testing.T) {
		baseMD := new(samples.Three).ProtoReflect().Descriptor()

		customFD, err := CompileProtoToFileDescriptor("custom_fields.proto", []string{protoDir})
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		customMD, err := GetMessageDescriptorByName(customFD, "CustomFields")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		schema, err := New(baseMD, memory.DefaultAllocator, WithCustomMessage(customMD))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		defer schema.Release()

		for i := 0; i < 5; i++ {
			baseMsg := &samples.Three{Value: uint64(i * 10)}
			customMsg := dynamicpb.NewMessage(customMD)
			customMsg.Set(customMD.Fields().ByName("event_timestamp"), protoreflect.ValueOfInt64(int64(i)))
			customMsg.Set(customMD.Fields().ByName("source_id"), protoreflect.ValueOfString("src"))

			err = schema.AppendWithCustom(baseMsg, customMsg)
			if err != nil {
				t.Fatalf("AppendWithCustom() iteration %d error = %v", i, err)
			}
		}

		r := schema.NewRecordBatch()
		if r.NumRows() != 5 {
			t.Errorf("expected 5 rows, got %d", r.NumRows())
		}
	})

	t.Run("error_without_custom_configured", func(t *testing.T) {
		baseMD := new(samples.Three).ProtoReflect().Descriptor()

		schema, err := New(baseMD, memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		defer schema.Release()

		err = schema.AppendWithCustom(&samples.Three{Value: 1}, dynamicpb.NewMessage(baseMD))
		if err == nil {
			t.Fatal("expected error when calling AppendWithCustom without custom configured")
		}
		if !strings.Contains(err.Error(), "without custom message configured") {
			t.Errorf("expected 'without custom message configured' error, got: %v", err)
		}
	})
}

// --- Clone tests ---

func TestClone(t *testing.T) {
	t.Run("clone_without_custom", func(t *testing.T) {
		baseMD := new(samples.Three).ProtoReflect().Descriptor()
		schema, err := New(baseMD, memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		defer schema.Release()

		cloned, err := schema.Clone(memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("Clone() error = %v", err)
		}
		defer cloned.Release()

		if cloned.stencilCustom != nil {
			t.Error("cloned schema should have nil stencilCustom")
		}

		// Append to cloned and verify independence
		cloned.Append(&samples.Three{Value: 99})
		r := cloned.NewRecordBatch()
		if r.NumRows() != 1 {
			t.Errorf("expected 1 row in cloned, got %d", r.NumRows())
		}
	})

	t.Run("clone_with_custom", func(t *testing.T) {
		protoDir := testProtoDir(t)
		baseMD := new(samples.Three).ProtoReflect().Descriptor()

		customFD, err := CompileProtoToFileDescriptor("custom_fields.proto", []string{protoDir})
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		customMD, err := GetMessageDescriptorByName(customFD, "CustomFields")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		schema, err := New(baseMD, memory.DefaultAllocator, WithCustomMessage(customMD))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		defer schema.Release()

		cloned, err := schema.Clone(memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("Clone() error = %v", err)
		}
		defer cloned.Release()

		if cloned.stencilCustom == nil {
			t.Error("cloned schema should have non-nil stencilCustom")
		}

		// AppendWithCustom on cloned should work
		customMsg := dynamicpb.NewMessage(customMD)
		customMsg.Set(customMD.Fields().ByName("event_timestamp"), protoreflect.ValueOfInt64(999))
		customMsg.Set(customMD.Fields().ByName("source_id"), protoreflect.ValueOfString("cloned"))
		err = cloned.AppendWithCustom(&samples.Three{Value: 55}, customMsg)
		if err != nil {
			t.Fatalf("AppendWithCustom() on cloned schema error = %v", err)
		}

		r := cloned.NewRecordBatch()
		if r.NumRows() != 1 {
			t.Errorf("expected 1 row in cloned, got %d", r.NumRows())
		}
	})
}

// --- Schema accessor tests ---

func TestSchemaAccessors(t *testing.T) {
	protoDir := testProtoDir(t)

	t.Run("parquet_schema_not_nil", func(t *testing.T) {
		schema, err := NewFromFile("samples.proto", "ScalarTypes", []string{protoDir}, memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("NewFromFile() error = %v", err)
		}
		defer schema.Release()

		if schema.Parquet() == nil {
			t.Error("Parquet() returned nil")
		}
	})

	t.Run("arrow_schema_not_nil", func(t *testing.T) {
		schema, err := NewFromFile("samples.proto", "ScalarTypes", []string{protoDir}, memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("NewFromFile() error = %v", err)
		}
		defer schema.Release()

		if schema.Schema() == nil {
			t.Error("Schema() returned nil")
		}
	})

	t.Run("field_names_match", func(t *testing.T) {
		schema, err := NewFromFile("samples.proto", "Three", []string{protoDir}, memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("NewFromFile() error = %v", err)
		}
		defer schema.Release()

		names := schema.FieldNames()
		if len(names) != 1 || names[0] != "value" {
			t.Errorf("expected [value], got %v", names)
		}
	})
}
