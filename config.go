package bufarrowlib

import (
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
	"google.golang.org/protobuf/reflect/protoreflect"
	"gopkg.in/yaml.v3"
)

// DenormConfig is the top-level declarative configuration for building a
// [Transcoder] with a denormalizer plan from a YAML source.
//
// Example YAML:
//
//	proto:
//	  file: path/to/schema.proto
//	  message: BidRequestEvent
//	  import_paths:
//	    - ./proto
//
//	# optional: merge an additional message's fields into the schema
//	custom_message:
//	  file: path/to/extensions.proto
//	  message: BidRequestExtension
//
//	denormalizer:
//	  columns:
//	    - name: auction_id
//	      path: auction_id
//
//	    - name: imp_id
//	      path: imp[*].id
//
//	    - name: floor_price
//	      path: imp[*].bidfloor
//	      strict: true
//
//	    - name: has_video
//	      expr:
//	        func: has
//	        args:
//	          - path: imp[*].video.id
//
//	    - name: full_imp_id
//	      expr:
//	        func: concat
//	        sep: "-"
//	        args:
//	          - path: imp[*].id
//	          - path: imp[*].banner.id
//
//	    - name: region
//	      expr:
//	        func: default
//	        args:
//	          - path: user.geo.region
//	        literal: "unknown"
type DenormConfig struct {
	Proto         ProtoConfig      `yaml:"proto"`
	CustomMessage *CustomMsgConfig `yaml:"custom_message,omitempty"`
	Denormalizer  DenormPlanConfig `yaml:"denormalizer"`
}

// ProtoConfig identifies the .proto source file and the root message type.
type ProtoConfig struct {
	// File is the path to the .proto file.
	File string `yaml:"file"`
	// Message is the top-level message name to use as the Transcoder root.
	Message string `yaml:"message"`
	// ImportPaths are additional directories searched when resolving imports
	// within the .proto file.
	ImportPaths []string `yaml:"import_paths,omitempty"`
}

// CustomMsgConfig optionally merges fields from a second .proto message into
// the base schema, enabling [Transcoder.AppendWithCustom].
type CustomMsgConfig struct {
	File        string   `yaml:"file"`
	Message     string   `yaml:"message"`
	ImportPaths []string `yaml:"import_paths,omitempty"`
}

// DenormPlanConfig holds the ordered list of output columns for the
// denormalizer plan.
type DenormPlanConfig struct {
	Columns []ColumnDef `yaml:"columns"`
}

// ColumnDef defines one output Arrow column in the denormalizer.
//
// Exactly one of Path or Expr must be set:
//   - Path: a raw protobuf path string (e.g. "imp[*].bidfloor"). The column
//     name in the output schema is taken from Name.
//   - Expr: a computed expression tree. All source paths come from the
//     expression's leaf [ArgDef] entries; the Name field becomes the output
//     column alias.
//
// Strict only applies to path-based columns; it is ignored for expr columns.
type ColumnDef struct {
	// Name is the output Arrow column name.
	Name string `yaml:"name"`
	// Path is a pbpath path string used when no Expr is needed.
	Path string `yaml:"path,omitempty"`
	// Strict makes out-of-bounds range/index access return an error instead
	// of being silently clamped. Only meaningful for path-based columns.
	Strict bool `yaml:"strict,omitempty"`
	// Expr defines a computed value from one or more source paths.
	Expr *ExprDef `yaml:"expr,omitempty"`
}

// ExprDef describes a single node in a composable expression tree.
//
// # Supported func names
//
//	┌──────────────┬──────────────────────────────────────────────────────────────────────┐
//	│ Category     │ func values                                                          │
//	├──────────────┼──────────────────────────────────────────────────────────────────────┤
//	│ Aggregation  │ coalesce, default                                                    │
//	│ Control flow │ cond                                                                 │
//	│ Predicates   │ has, eq, ne, lt, le, gt, ge                                          │
//	│ Arithmetic   │ add, sub, mul, div, mod, abs, ceil, floor, round, min, max           │
//	│ String       │ concat, upper, lower, trim, trim_prefix, trim_suffix, len            │
//	│ Cast         │ cast_int, cast_float, cast_string                                    │
//	│ Timestamp    │ age, strptime, try_strptime, extract_year, extract_month,            │
//	│              │ extract_day, extract_hour, extract_minute, extract_second            │
//	│ ETL          │ hash, epoch_to_date, date_part, bucket, mask, coerce, enum_name,     │
//	│              │ sum, distinct, list_concat                                           │
//	│ Logic        │ and, or, not                                                         │
//	└──────────────┴──────────────────────────────────────────────────────────────────────┘
//
// # Auxiliary fields
//
//   - Args — ordered list of child [ArgDef] values.
//   - Sep — string parameter, interpretation depends on func:
//   - concat: separator string (e.g. "-", ",").
//   - trim_prefix, trim_suffix: the affix string to remove.
//   - strptime, try_strptime: the Go time-format string (e.g. "2006-01-02T15:04:05Z").
//   - date_part: part name ("year", "month", "day", "hour", "minute", "second", "epoch").
//   - list_concat: separator between collected values.
//   - mask: the replacement character (single rune, e.g. "*").
//   - Literal — first scalar constant:
//   - default: the fallback value when the child is null/zero.
//   - coerce: the ifTrue replacement value.
//   - YAML type determines Go type: string → string, integer → int64, float → float64, bool → bool.
//   - Literal2 — second scalar constant:
//   - coerce: the ifFalse replacement value (same type rules as Literal).
//   - Param — integer parameter:
//   - bucket: bucket size (the child value is divided into buckets of this width).
//   - mask: number of leading characters to keep unmasked (keepFirst); the number
//     of trailing characters to keep is always 0 (use a cond+mask tree for both).
type ExprDef struct {
	Func     string      `yaml:"func"`
	Args     []ArgDef    `yaml:"args,omitempty"`
	Sep      string      `yaml:"sep,omitempty"`
	Literal  interface{} `yaml:"literal,omitempty"`
	Literal2 interface{} `yaml:"literal2,omitempty"`
	Param    int         `yaml:"param,omitempty"`
}

// ArgDef is one argument in an [ExprDef]. Exactly one field should be set:
//   - Path: a protobuf field path (leaf [pbpath.PathRef]).
//   - Literal: a scalar constant (string, int, float64, or bool).
//   - Expr: a nested expression sub-tree.
type ArgDef struct {
	Path    string      `yaml:"path,omitempty"`
	Literal interface{} `yaml:"literal,omitempty"`
	Expr    *ExprDef    `yaml:"expr,omitempty"`
}

// ParseDenormConfig decodes a YAML [DenormConfig] from r.
// Unknown fields are rejected to surface typos early.
func ParseDenormConfig(r io.Reader) (*DenormConfig, error) {
	var cfg DenormConfig
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("bufarrow: parse denorm config: %w", err)
	}
	if cfg.Proto.File == "" {
		return nil, fmt.Errorf("bufarrow: denorm config: proto.file is required")
	}
	if cfg.Proto.Message == "" {
		return nil, fmt.Errorf("bufarrow: denorm config: proto.message is required")
	}
	if len(cfg.Denormalizer.Columns) == 0 {
		return nil, fmt.Errorf("bufarrow: denorm config: at least one denormalizer column is required")
	}
	return &cfg, nil
}

// ParseDenormConfigFile reads and parses a YAML [DenormConfig] from a file.
func ParseDenormConfigFile(path string) (*DenormConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("bufarrow: open denorm config %q: %w", path, err)
	}
	defer f.Close()
	return ParseDenormConfig(f)
}

// NewTranscoderFromConfig builds a [Transcoder] from a parsed [DenormConfig].
// mem is the Arrow memory allocator to use; pass nil for the default allocator.
func NewTranscoderFromConfig(cfg *DenormConfig, mem memory.Allocator) (*Transcoder, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}

	specs, err := configToPlanSpecs(cfg)
	if err != nil {
		return nil, err
	}

	opts := []Option{WithDenormalizerPlan(specs...)}

	if cfg.CustomMessage != nil {
		opts = append(opts, WithCustomMessageFile(
			cfg.CustomMessage.File,
			cfg.CustomMessage.Message,
			cfg.CustomMessage.ImportPaths,
		))
	}

	return NewFromFile(
		cfg.Proto.File,
		cfg.Proto.Message,
		cfg.Proto.ImportPaths,
		mem,
		opts...,
	)
}

// NewTranscoderFromConfigFile reads a YAML file and calls [NewTranscoderFromConfig].
func NewTranscoderFromConfigFile(configPath string, mem memory.Allocator) (*Transcoder, error) {
	cfg, err := ParseDenormConfigFile(configPath)
	if err != nil {
		return nil, err
	}
	return NewTranscoderFromConfig(cfg, mem)
}

// ColumnsToPlanSpecs converts a slice of [ColumnDef] into
// []pbpath.PlanPathSpec ready for [WithDenormalizerPlan].
func ColumnsToPlanSpecs(columns []ColumnDef) ([]pbpath.PlanPathSpec, error) {
	specs := make([]pbpath.PlanPathSpec, 0, len(columns))
	for i, col := range columns {
		if col.Name == "" {
			return nil, fmt.Errorf("bufarrow: denorm config: column[%d] missing name", i)
		}
		spec, err := columnToPlanSpec(col)
		if err != nil {
			return nil, fmt.Errorf("bufarrow: denorm config: column %q: %w", col.Name, err)
		}
		specs = append(specs, spec)
	}
	return specs, nil
}

// ---- internal helpers -------------------------------------------------------

// configToPlanSpecs converts the Denormalizer.Columns slice into
// []pbpath.PlanPathSpec ready for [WithDenormalizerPlan].
func configToPlanSpecs(cfg *DenormConfig) ([]pbpath.PlanPathSpec, error) {
	return ColumnsToPlanSpecs(cfg.Denormalizer.Columns)
}

func columnToPlanSpec(col ColumnDef) (pbpath.PlanPathSpec, error) {
	if col.Expr != nil && col.Path != "" {
		return pbpath.PlanPathSpec{}, fmt.Errorf("path and expr are mutually exclusive; set one or the other")
	}
	if col.Expr == nil && col.Path == "" {
		return pbpath.PlanPathSpec{}, fmt.Errorf("either path or expr must be set")
	}

	if col.Expr != nil {
		expr, err := buildExpr(col.Expr)
		if err != nil {
			return pbpath.PlanPathSpec{}, err
		}
		// When WithExpr is set the path string is ignored for traversal;
		// the column name is used as the entry alias.
		return pbpath.PlanPath(col.Name, pbpath.WithExpr(expr)), nil
	}

	// Simple path column.
	entryOpts := []pbpath.EntryOption{pbpath.Alias(col.Name)}
	if col.Strict {
		entryOpts = append(entryOpts, pbpath.StrictPath())
	}
	return pbpath.PlanPath(col.Path, entryOpts...), nil
}

// buildExpr recursively constructs a [pbpath.Expr] from an [ExprDef].
func buildExpr(def *ExprDef) (pbpath.Expr, error) {
	if def == nil {
		return nil, fmt.Errorf("nil expr definition")
	}

	// Resolve children first (used by most functions).
	children, err := buildArgs(def.Args)
	if err != nil {
		return nil, err
	}

	switch def.Func {
	// ---- Aggregation ----
	case "coalesce":
		if len(children) < 2 {
			return nil, fmt.Errorf("coalesce requires at least 2 args")
		}
		return pbpath.FuncCoalesce(children...), nil

	case "default":
		if len(children) != 1 {
			return nil, fmt.Errorf("default requires exactly 1 arg")
		}
		lit, err := literalToValue(def.Literal)
		if err != nil {
			return nil, fmt.Errorf("default literal: %w", err)
		}
		return pbpath.FuncDefault(children[0], lit), nil

	// ---- Control flow ----
	case "cond":
		if len(children) != 3 {
			return nil, fmt.Errorf("cond requires exactly 3 args (predicate, then, else)")
		}
		return pbpath.FuncCond(children[0], children[1], children[2]), nil

	// ---- Predicates ----
	case "has":
		if len(children) != 1 {
			return nil, fmt.Errorf("has requires exactly 1 arg")
		}
		return pbpath.FuncHas(children[0]), nil

	case "eq":
		return requireBinary("eq", children, pbpath.FuncEq)
	case "ne":
		return requireBinary("ne", children, pbpath.FuncNe)
	case "lt":
		return requireBinary("lt", children, pbpath.FuncLt)
	case "le":
		return requireBinary("le", children, pbpath.FuncLe)
	case "gt":
		return requireBinary("gt", children, pbpath.FuncGt)
	case "ge":
		return requireBinary("ge", children, pbpath.FuncGe)

	// ---- Arithmetic ----
	case "add":
		return requireBinary("add", children, pbpath.FuncAdd)
	case "sub":
		return requireBinary("sub", children, pbpath.FuncSub)
	case "mul":
		return requireBinary("mul", children, pbpath.FuncMul)
	case "div":
		return requireBinary("div", children, pbpath.FuncDiv)
	case "mod":
		return requireBinary("mod", children, pbpath.FuncMod)
	case "min":
		return requireBinary("min", children, pbpath.FuncMin)
	case "max":
		return requireBinary("max", children, pbpath.FuncMax)
	case "abs":
		return requireUnary("abs", children, pbpath.FuncAbs)
	case "ceil":
		return requireUnary("ceil", children, pbpath.FuncCeil)
	case "floor":
		return requireUnary("floor", children, pbpath.FuncFloor)
	case "round":
		return requireUnary("round", children, pbpath.FuncRound)

	// ---- String ----
	case "concat":
		if len(children) < 1 {
			return nil, fmt.Errorf("concat requires at least 1 arg")
		}
		return pbpath.FuncConcat(def.Sep, children...), nil
	case "upper":
		return requireUnary("upper", children, pbpath.FuncUpper)
	case "lower":
		return requireUnary("lower", children, pbpath.FuncLower)
	case "trim":
		return requireUnary("trim", children, pbpath.FuncTrim)
	case "trim_prefix":
		return requireUnary("trim_prefix", children, func(c pbpath.Expr) pbpath.Expr {
			return pbpath.FuncTrimPrefix(c, def.Sep)
		})
	case "trim_suffix":
		return requireUnary("trim_suffix", children, func(c pbpath.Expr) pbpath.Expr {
			return pbpath.FuncTrimSuffix(c, def.Sep)
		})
	case "len":
		return requireUnary("len", children, pbpath.FuncLen)

	// ---- Cast ----
	case "cast_int":
		return requireUnary("cast_int", children, pbpath.FuncCastInt)
	case "cast_float":
		return requireUnary("cast_float", children, pbpath.FuncCastFloat)
	case "cast_string":
		return requireUnary("cast_string", children, pbpath.FuncCastString)

	// ---- Timestamp ----
	case "age":
		if len(children) < 1 {
			return nil, fmt.Errorf("age requires at least 1 arg")
		}
		return pbpath.FuncAge(children...), nil
	case "strptime":
		return requireUnary("strptime", children, func(c pbpath.Expr) pbpath.Expr {
			return pbpath.FuncStrptime(def.Sep, c)
		})
	case "try_strptime":
		return requireUnary("try_strptime", children, func(c pbpath.Expr) pbpath.Expr {
			return pbpath.FuncTryStrptime(def.Sep, c)
		})
	case "extract_year":
		return requireUnary("extract_year", children, pbpath.FuncExtractYear)
	case "extract_month":
		return requireUnary("extract_month", children, pbpath.FuncExtractMonth)
	case "extract_day":
		return requireUnary("extract_day", children, pbpath.FuncExtractDay)
	case "extract_hour":
		return requireUnary("extract_hour", children, pbpath.FuncExtractHour)
	case "extract_minute":
		return requireUnary("extract_minute", children, pbpath.FuncExtractMinute)
	case "extract_second":
		return requireUnary("extract_second", children, pbpath.FuncExtractSecond)
	case "epoch_to_date":
		return requireUnary("epoch_to_date", children, pbpath.FuncEpochToDate)
	case "date_part":
		return requireUnary("date_part", children, func(c pbpath.Expr) pbpath.Expr {
			return pbpath.FuncDatePart(def.Sep, c)
		})

	// ---- ETL ----
	case "hash":
		if len(children) < 1 {
			return nil, fmt.Errorf("hash requires at least 1 arg")
		}
		return pbpath.FuncHash(children...), nil
	case "bucket":
		return requireUnary("bucket", children, func(c pbpath.Expr) pbpath.Expr {
			return pbpath.FuncBucket(c, def.Param)
		})
	case "mask":
		if len(children) != 1 {
			return nil, fmt.Errorf("mask requires exactly 1 arg")
		}
		// Param holds keepFirst; Sep holds the mask character; keepLast defaults to 0.
		return pbpath.FuncMask(children[0], def.Param, 0, def.Sep), nil
	case "enum_name":
		return requireUnary("enum_name", children, pbpath.FuncEnumName)
	case "sum":
		return requireUnary("sum", children, pbpath.FuncSum)
	case "distinct":
		return requireUnary("distinct", children, pbpath.FuncDistinct)
	case "list_concat":
		return requireUnary("list_concat", children, func(c pbpath.Expr) pbpath.Expr {
			return pbpath.FuncListConcat(c, def.Sep)
		})
	case "coerce":
		if len(children) != 1 {
			return nil, fmt.Errorf("coerce requires exactly 1 arg")
		}
		ifTrue, err := literalToValue(def.Literal)
		if err != nil {
			return nil, fmt.Errorf("coerce literal: %w", err)
		}
		ifFalse, err := literalToValue(def.Literal2)
		if err != nil {
			return nil, fmt.Errorf("coerce literal2: %w", err)
		}
		return pbpath.FuncCoerce(children[0], ifTrue, ifFalse), nil

	// ---- Logic ----
	case "and":
		return requireBinary("and", children, pbpath.FuncAnd)
	case "or":
		return requireBinary("or", children, pbpath.FuncOr)
	case "not":
		return requireUnary("not", children, pbpath.FuncNot)

	default:
		return nil, fmt.Errorf("unknown func %q", def.Func)
	}
}

// buildArgs converts a slice of [ArgDef] into a slice of [pbpath.Expr].
func buildArgs(args []ArgDef) ([]pbpath.Expr, error) {
	result := make([]pbpath.Expr, 0, len(args))
	for i, arg := range args {
		expr, err := buildArg(arg)
		if err != nil {
			return nil, fmt.Errorf("arg[%d]: %w", i, err)
		}
		result = append(result, expr)
	}
	return result, nil
}

// buildArg converts a single [ArgDef] into a [pbpath.Expr].
func buildArg(arg ArgDef) (pbpath.Expr, error) {
	set := 0
	if arg.Path != "" {
		set++
	}
	if arg.Literal != nil {
		set++
	}
	if arg.Expr != nil {
		set++
	}
	if set != 1 {
		return nil, fmt.Errorf("each arg must have exactly one of: path, literal, expr (got %d)", set)
	}

	switch {
	case arg.Path != "":
		return pbpath.PathRef(arg.Path), nil
	case arg.Expr != nil:
		return buildExpr(arg.Expr)
	default:
		// literal
		val, kind, err := literalToValueKind(arg.Literal)
		if err != nil {
			return nil, err
		}
		return pbpath.Literal(val, kind), nil
	}
}

// literalToValue converts a raw YAML-decoded value to a [pbpath.Value] for
// use with [pbpath.FuncDefault] and [pbpath.FuncCoerce].
func literalToValue(v interface{}) (pbpath.Value, error) {
	val, _, err := literalToValueKind(v)
	return val, err
}

// literalToValueKind converts a raw YAML-decoded value to a [pbpath.Value]
// and its [protoreflect.Kind], so callers can create a typed [pbpath.Literal].
func literalToValueKind(v interface{}) (pbpath.Value, protoreflect.Kind, error) {
	if v == nil {
		return pbpath.Null(), 0, nil
	}
	switch t := v.(type) {
	case string:
		return pbpath.ScalarString(t), protoreflect.StringKind, nil
	case int:
		return pbpath.ScalarInt64(int64(t)), protoreflect.Int64Kind, nil
	case int64:
		return pbpath.ScalarInt64(t), protoreflect.Int64Kind, nil
	case float64:
		return pbpath.ScalarFloat64(t), protoreflect.DoubleKind, nil
	case bool:
		return pbpath.ScalarBool(t), protoreflect.BoolKind, nil
	default:
		return pbpath.Null(), 0, fmt.Errorf("unsupported literal type %T (use string, int, float, or bool)", v)
	}
}

// requireUnary validates a single-child arg list and calls fn.
func requireUnary(name string, children []pbpath.Expr, fn func(pbpath.Expr) pbpath.Expr) (pbpath.Expr, error) {
	if len(children) != 1 {
		return nil, fmt.Errorf("%s requires exactly 1 arg, got %d", name, len(children))
	}
	return fn(children[0]), nil
}

// requireBinary validates a two-child arg list and calls fn.
func requireBinary(name string, children []pbpath.Expr, fn func(a, b pbpath.Expr) pbpath.Expr) (pbpath.Expr, error) {
	if len(children) != 2 {
		return nil, fmt.Errorf("%s requires exactly 2 args, got %d", name, len(children))
	}
	return fn(children[0], children[1]), nil
}
