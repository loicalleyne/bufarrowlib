package bufarrowlib_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib"
)

// TestParseDenormConfig validates the YAML parser and structural checks.
func TestParseDenormConfig(t *testing.T) {
	t.Run("valid_simple", func(t *testing.T) {
		src := `
proto:
  file: schema.proto
  message: Order
denormalizer:
  columns:
    - name: order_name
      path: name
    - name: item_id
      path: items[*].id
`
		cfg, err := bufarrowlib.ParseDenormConfig(strings.NewReader(src))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Proto.File != "schema.proto" {
			t.Errorf("proto.file: got %q, want schema.proto", cfg.Proto.File)
		}
		if len(cfg.Denormalizer.Columns) != 2 {
			t.Errorf("columns: got %d, want 2", len(cfg.Denormalizer.Columns))
		}
	})

	t.Run("missing_proto_file", func(t *testing.T) {
		src := `
proto:
  message: Order
denormalizer:
  columns:
    - name: x
      path: y
`
		_, err := bufarrowlib.ParseDenormConfig(strings.NewReader(src))
		if err == nil {
			t.Fatal("expected error for missing proto.file, got nil")
		}
	})

	t.Run("missing_proto_message", func(t *testing.T) {
		src := `
proto:
  file: schema.proto
denormalizer:
  columns:
    - name: x
      path: y
`
		_, err := bufarrowlib.ParseDenormConfig(strings.NewReader(src))
		if err == nil {
			t.Fatal("expected error for missing proto.message, got nil")
		}
	})

	t.Run("no_columns", func(t *testing.T) {
		src := `
proto:
  file: schema.proto
  message: Order
denormalizer:
  columns: []
`
		_, err := bufarrowlib.ParseDenormConfig(strings.NewReader(src))
		if err == nil {
			t.Fatal("expected error for empty columns, got nil")
		}
	})

	t.Run("unknown_field_rejected", func(t *testing.T) {
		src := `
proto:
  file: schema.proto
  message: Order
  typo_field: oops
denormalizer:
  columns:
    - name: x
      path: y
`
		_, err := bufarrowlib.ParseDenormConfig(strings.NewReader(src))
		if err == nil {
			t.Fatal("expected error for unknown YAML field, got nil")
		}
	})

	t.Run("expr_coalesce_parsed", func(t *testing.T) {
		src := `
proto:
  file: schema.proto
  message: Order
denormalizer:
  columns:
    - name: primary
      expr:
        func: coalesce
        args:
          - path: name
          - path: seq
`
		cfg, err := bufarrowlib.ParseDenormConfig(strings.NewReader(src))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		col := cfg.Denormalizer.Columns[0]
		if col.Expr == nil || col.Expr.Func != "coalesce" || len(col.Expr.Args) != 2 {
			t.Errorf("unexpected expr: %+v", col.Expr)
		}
	})

	t.Run("nested_expr_parsed", func(t *testing.T) {
		src := `
proto:
  file: schema.proto
  message: Order
denormalizer:
  columns:
    - name: imp_type
      expr:
        func: cond
        args:
          - expr:
              func: has
              args:
                - path: tags[*]
          - literal: "yes"
          - literal: "no"
`
		cfg, err := bufarrowlib.ParseDenormConfig(strings.NewReader(src))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		col := cfg.Denormalizer.Columns[0]
		if col.Expr == nil || col.Expr.Func != "cond" {
			t.Fatalf("expected cond expr, got %+v", col.Expr)
		}
		if col.Expr.Args[0].Expr == nil || col.Expr.Args[0].Expr.Func != "has" {
			t.Errorf("expected nested has expr in arg[0], got %+v", col.Expr.Args[0])
		}
		if col.Expr.Args[1].Literal != "yes" || col.Expr.Args[2].Literal != "no" {
			t.Errorf("unexpected literals: %v / %v", col.Expr.Args[1].Literal, col.Expr.Args[2].Literal)
		}
	})
}

// TestNewTranscoderFromConfig exercises the full pipeline against a real
// (temporary) .proto file using the same schema as the example tests.
func TestNewTranscoderFromConfig(t *testing.T) {
	protoContent := `syntax = "proto3";
package example;
message Item {
  string id    = 1;
  double price = 2;
}
message Order {
  string          name  = 1;
  repeated Item   items = 2;
  repeated string tags  = 3;
  int64           seq   = 4;
}
`
	dir := t.TempDir()
	protoFile := filepath.Join(dir, "example.proto")
	if err := os.WriteFile(protoFile, []byte(protoContent), 0o600); err != nil {
		t.Fatalf("write proto file: %v", err)
	}

	src := `
proto:
  file: ` + protoFile + `
  message: Order
denormalizer:
  columns:
    - name: order_name
      path: name
    - name: item_id
      path: items[*].id
    - name: item_price
      path: items[*].price
    - name: has_tags
      expr:
        func: has
        args:
          - path: tags[*]
    - name: name_item_key
      expr:
        func: concat
        sep: ":"
        args:
          - path: name
          - path: items[*].id
`
	cfg, err := bufarrowlib.ParseDenormConfig(strings.NewReader(src))
	if err != nil {
		t.Fatalf("ParseDenormConfig: %v", err)
	}

	tc, err := bufarrowlib.NewTranscoderFromConfig(cfg, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("NewTranscoderFromConfig: %v", err)
	}
	defer tc.Release()

	schema := tc.DenormalizerSchema()
	if schema == nil {
		t.Fatal("DenormalizerSchema returned nil")
	}
	if schema.NumFields() != 5 {
		t.Errorf("schema fields: got %d, want 5", schema.NumFields())
	}
	wantNames := []string{"order_name", "item_id", "item_price", "has_tags", "name_item_key"}
	for i, want := range wantNames {
		if got := schema.Field(i).Name; got != want {
			t.Errorf("field[%d]: got %q, want %q", i, got, want)
		}
	}
}
