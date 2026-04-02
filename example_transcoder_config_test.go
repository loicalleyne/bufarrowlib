package bufarrowlib_test

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib"
)

// ExampleParseDenormConfig demonstrates parsing a YAML denormalizer
// configuration from an [strings.Reader] and inspecting the result.
func ExampleParseDenormConfig() {
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
    - name: item_price
      path: items[*].price
    - name: has_tags
      expr:
        func: has
        args:
          - path: tags[*]
`
	cfg, err := bufarrowlib.ParseDenormConfig(strings.NewReader(src))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("proto:", cfg.Proto.Message)
	fmt.Println("columns:", len(cfg.Denormalizer.Columns))
	for _, col := range cfg.Denormalizer.Columns {
		if col.Expr != nil {
			fmt.Printf("  %-18s expr:%s\n", col.Name, col.Expr.Func)
		} else {
			fmt.Printf("  %-18s path:%s\n", col.Name, col.Path)
		}
	}
	// Output:
	// proto: Order
	// columns: 4
	//   order_name         path:name
	//   item_id            path:items[*].id
	//   item_price         path:items[*].price
	//   has_tags           expr:has
}

// ExampleParseDenormConfig_nestedExpr shows a cond expression whose predicate
// is itself a nested expression (recursive [bufarrowlib.ExprDef] trees).
func ExampleParseDenormConfig_nestedExpr() {
	src := `
proto:
  file: schema.proto
  message: Order
denormalizer:
  columns:
    - name: category
      expr:
        func: cond
        args:
          - expr:
              func: gt
              args:
                - path: seq
                - literal: 100
          - literal: "premium"
          - literal: "standard"
`
	cfg, err := bufarrowlib.ParseDenormConfig(strings.NewReader(src))
	if err != nil {
		log.Fatal(err)
	}

	col := cfg.Denormalizer.Columns[0]
	fmt.Println("func:", col.Expr.Func)
	fmt.Println("predicate:", col.Expr.Args[0].Expr.Func)
	fmt.Println("then:", col.Expr.Args[1].Literal)
	fmt.Println("else:", col.Expr.Args[2].Literal)
	// Output:
	// func: cond
	// predicate: gt
	// then: premium
	// else: standard
}

// ExampleNewTranscoderFromConfig demonstrates building a [bufarrowlib.Transcoder]
// from a [bufarrowlib.DenormConfig] and inspecting the resulting denormalizer schema.
// It uses the same Order/Item schema as in [ExampleTranscoder_AppendDenorm].
func ExampleNewTranscoderFromConfig() {
	// writeProtoFile (defined in example_transcoder_test.go) creates a
	// temporary .proto file and returns its path and the temp directory.
	protoFile, dir := writeProtoFile("example.proto", `
syntax = "proto3";
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
`)
	defer os.RemoveAll(dir)

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
`
	cfg, err := bufarrowlib.ParseDenormConfig(strings.NewReader(src))
	if err != nil {
		log.Fatal(err)
	}

	tc, err := bufarrowlib.NewTranscoderFromConfig(cfg, memory.DefaultAllocator)
	if err != nil {
		log.Fatal(err)
	}
	defer tc.Release()

	// Inspect the compiled denormalizer schema.
	schema := tc.DenormalizerSchema()
	fmt.Printf("columns: %d\n", schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		f := schema.Field(i)
		fmt.Printf("  %-15s %s\n", f.Name, f.Type)
	}
	// Output:
	// columns: 4
	//   order_name      utf8
	//   item_id         utf8
	//   item_price      float64
	//   has_tags        bool
}
