package bufarrowlib

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"buf.build/go/hyperpb"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// maxDepth is the maximum recursion depth when building the Arrow schema tree
// from a protobuf message descriptor. Prevents infinite recursion from
// circular or excessively nested message definitions.
const (
	maxDepth = 10
)

// Sentinel errors returned during schema construction and path lookup.
var (
	// ErrMxDepth is returned when the protobuf message nesting exceeds maxDepth.
	ErrMxDepth = errors.New("max depth reached, either the message is deeply nested or a circular dependency was introduced")
	// ErrPathNotFound is returned by node.getPath when a field name is not
	// found in the node's hash map.
	ErrPathNotFound = errors.New("path not found")
)

// valueFn appends a single protobuf field value to the corresponding Arrow
// array builder. The bool parameter indicates whether the field is set
// (relevant for oneof fields where an unset field should produce a null).
type valueFn func(protoreflect.Value, bool) error

// encodeFn decodes an Arrow array cell back into a protoreflect.Value.
// It is the inverse of valueFn and used by the Proto() decode path.
type encodeFn func(value protoreflect.Value, a arrow.Array, row int) protoreflect.Value

// node is a single field in the Arrow schema tree mirroring the protobuf
// message structure. Each node holds the Arrow field metadata, closures for
// appending values (setup/write) and decoding them back (encode), and
// references to child nodes for nested messages.
type node struct {
	parent   *node
	field    arrow.Field
	setup    func(array.Builder) valueFn
	write    valueFn
	desc     protoreflect.Descriptor
	children []*node
	encode   encodeFn
	hash     map[string]*node
	depth    int
}

// getPath returns a field found at a defined path, otherwise returns ErrPathNotFound.
func (n *node) getPath(path []string) (*node, error) {
	if len(path) == 0 { // degenerate input
		return nil, fmt.Errorf("getPath needs at least one key")
	}
	if node, ok := n.hash[path[0]]; !ok {
		return nil, ErrPathNotFound
	} else if len(path) == 1 { // we've reached the final key
		return node, nil
	} else { // 1+ more keys
		return node.getPath(path[1:])
	}
}

// dotPath returns the fully-qualified protobuf name of this node's descriptor
// as a dot-separated string.
func (n *node) dotPath() string { return string(n.desc.FullName()) }

// unmarshal decodes selected rows from an Arrow RecordBatch back into protobuf
// messages using the node tree's encode closures. If rows is nil, all rows are
// decoded.
func unmarshal(msgType *hyperpb.MessageType, n *node, r arrow.RecordBatch, rows []int) []proto.Message {
	if rows == nil {
		rows = make([]int, r.NumRows())
		for i := range rows {
			rows[i] = i
		}
	}
	o := make([]proto.Message, len(rows))
	msgDesc := msgType.Descriptor()
	ref := dynamicpb.NewMessage(msgDesc)
	for idx, row := range rows {
		msg := ref.New()
		for i := 0; i < int(r.NumCols()); i++ {
			name := r.ColumnName(i)
			nx, ok := n.hash[name]
			if !ok {
				panic(fmt.Sprintf("bufarrow: field %s not found in node %v", name, n.field.Name))
			}
			if r.Column(i).IsNull(row) {
				continue
			}
			fs := nx.desc.(protoreflect.FieldDescriptor)
			switch {
			case fs.IsList():
				ls := r.Column(i).(*array.List)
				start, end := ls.ValueOffsets(row)
				val := ls.ListValues()
				if start != end {
					lv := msg.NewField(fs)
					list := lv.List()
					for k := start; k < end; k++ {
						list.Append(
							nx.encode(
								list.NewElement(),
								val,
								int(k),
							),
						)
					}
					// fmt.Printf("%v %v %v \n", n.desc.FullName(), fs.FullName(), fs.Kind()) // debug
					msg.Set(fs, lv)
				}
			case fs.IsMap():
				panic("MAP not supported")
			default:
				// fmt.Printf("i: %d, %v %v %v %v \n", i, n.desc.FullName(), fs.FullName(), fs.Kind(), r.Column(i).DataType().String()) // debug
				msg.Set(fs,
					nx.encode(msg.NewField(fs), r.Column(i), row),
				)
			}
		}
		o[idx] = msg.Interface()
	}
	return o
}

// build constructs the full Arrow schema tree and Parquet schema from a
// protobuf message. It recursively creates nodes for every field and returns
// a message struct ready for builder initialisation via message.build.
func build(msg protoreflect.Message) *message {
	root := &node{
		desc:  msg.Descriptor(),
		field: arrow.Field{},
		hash:  make(map[string]*node),
	}
	fields := msg.Descriptor().Fields()
	root.children = make([]*node, fields.Len())
	a := make([]arrow.Field, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		x := createNode(root, fields.Get(i), 0)
		root.children[i] = x
		root.hash[x.field.Name] = x
		a[i] = root.children[i].field
	}
	as := arrow.NewSchema(a, nil)

	// we need to apply compression on all fields and use dictionary for binary and
	// string columns.
	bs, err := pqarrow.ToParquet(as, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		panic(err)
	}
	var props []parquet.WriterProperty

	for i := 0; i < bs.NumColumns(); i++ {
		col := bs.Column(i)
		if col.PhysicalType() == parquet.Types.ByteArray {
			props = append(props, parquet.WithDictionaryPath(col.ColumnPath(), true))
		}
	}
	// ZSTD is pretty good for all cases. Default level is reasonable.
	props = append(props, parquet.WithCompression(compress.Codecs.Zstd))
	// All writes are on a single row group. This is needed because we treat rows
	// are sample ID and we need to keep the mapping
	props = append(props, parquet.WithMaxRowGroupLength(math.MaxInt))

	ps, err := pqarrow.ToParquet(as, parquet.NewWriterProperties(props...), pqarrow.DefaultWriterProps())
	if err != nil {
		panic(err)
	}
	return &message{
		root:    root,
		schema:  as,
		parquet: ps,
		props:   props,
	}
}

// message bundles the node tree, Arrow schema, Parquet schema, and record
// builder for a single protobuf message type. It is the core internal
// representation used by Transcoder.
type message struct {
	root    *node
	schema  *arrow.Schema
	parquet *schema.Schema
	builder *array.RecordBuilder
	props   []parquet.WriterProperty
}

// build allocates an Arrow RecordBuilder from the schema and wires up each
// node's write closure so that subsequent calls to message.append populate
// the builder.
func (m *message) build(mem memory.Allocator) {
	b := array.NewRecordBuilder(mem, m.schema)
	for i, ch := range m.root.children {
		ch.build(b.Field(i))
	}
	m.builder = b
}

// append walks the protobuf message and appends every field value to the
// corresponding Arrow builder via the node tree's write closures.
func (m *message) append(msg protoreflect.Message) {
	m.root.WriteMessage(msg)
}

// NewRecordBatch flushes the accumulated builder contents into an Arrow
// RecordBatch and resets the builder for the next batch.
func (m *message) NewRecordBatch() arrow.RecordBatch {
	return m.builder.NewRecordBatch()
}

// createNode recursively builds a node for a single protobuf field, resolving
// the Arrow type via baseType for scalar kinds and recursing into sub-messages
// for message kinds. It also handles well-known types (otelAnyDescriptor),
// list wrapping, and oneof nullability.
func createNode(parent *node, field protoreflect.FieldDescriptor, depth int) *node {
	if depth >= maxDepth {
		panic(ErrMxDepth)
	}
	name, ok := parent.field.Metadata.GetValue("path")
	if ok {
		name += "." + string(field.Name())
	} else {
		name = string(field.Name())
	}
	n := &node{parent: parent, desc: field, depth: depth, field: arrow.Field{
		Name:     string(field.Name()),
		Nullable: nullable(field),
		Metadata: arrow.MetadataFrom(map[string]string{
			"path":             name,
			"PARQUET:field_id": strconv.Itoa(int(field.Number())),
		}),
	}, hash: make(map[string]*node)}
	n.field.Type = n.baseType(field)

	if n.field.Type != nil {
		return n
	}
	// Try a message
	if msg := field.Message(); msg != nil {
		switch msg {
		case otelAnyDescriptor:
			n.field.Type = arrow.BinaryTypes.Binary
			n.field.Nullable = true
			n.setup = func(b array.Builder) valueFn {
				a := b.(*array.BinaryBuilder)
				return func(v protoreflect.Value, set bool) error {
					if !v.IsValid() {
						a.AppendNull()
						return nil
					}
					e := v.Message().Interface().(*commonv1.AnyValue)
					bs, err := proto.Marshal(e)
					if err != nil {
						return err
					}
					a.Append(bs)
					return nil
				}
			}
			n.encode = func(value protoreflect.Value, a arrow.Array, row int) protoreflect.Value {
				if a.IsNull(row) {
					return protoreflect.Value{}
				}
				msg := value.Message()
				var v []byte
				if a.DataType().ID() == arrow.DICTIONARY {
					d := a.(*array.Dictionary)
					v = d.Dictionary().(*array.Binary).Value(d.GetValueIndex(row))
				} else {
					v = a.(*array.Binary).Value(row)
				}
				proto.Unmarshal(v, msg.Interface())
				return value
			}
		}
		if n.field.Type != nil {
			if field.IsList() {
				n.field.Type = arrow.ListOf(n.field.Type)
				setup := n.setup
				n.setup = func(b array.Builder) valueFn {
					ls := b.(*array.ListBuilder)
					value := setup(ls.ValueBuilder())
					return func(v protoreflect.Value, set bool) error {
						if !v.IsValid() {
							ls.AppendNull()
							return nil
						}
						ls.Append(true)
						list := v.List()
						for i := 0; i < list.Len(); i++ {
							err := value(list.Get(i), true)
							if err != nil {
								return err
							}
						}
						return nil
					}
				}
			}
			return n
		}
		f := msg.Fields()
		n.children = make([]*node, f.Len())
		a := make([]arrow.Field, f.Len())
		for i := 0; i < f.Len(); i++ {
			x := createNode(n, f.Get(i), depth+1)
			n.children[i] = x
			n.hash[x.field.Name] = x
			a[i] = n.children[i].field
		}
		n.field.Type = arrow.StructOf(a...)
		n.field.Nullable = true
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.StructBuilder)
			fs := make([]valueFn, len(n.children))
			for i := range n.children {
				fs[i] = n.children[i].setup(a.FieldBuilder(i))
			}
			return func(v protoreflect.Value, set bool) error {
				if !v.IsValid() {
					a.AppendNull()
					return nil
				}
				a.Append(true)
				msg := v.Message()
				fields := msg.Descriptor().Fields()
				for i := 0; i < fields.Len(); i++ {
					err := fs[i](msg.Get(fields.Get(i)), msg.Has(fields.Get(i)))
					if err != nil {
						return err
					}
				}
				return nil
			}
		}
		n.encode = func(value protoreflect.Value, a arrow.Array, row int) protoreflect.Value {
			msg := value.Message()
			if t, ok := a.(*array.Timestamp); ok {
				tv := t.Value(row).ToTime(arrow.Millisecond)
				ts := timestamppb.New(tv)
				return protoreflect.ValueOfMessage(ts.ProtoReflect())
			}
			s := a.(*array.Struct)
			typ := a.DataType().(*arrow.StructType)
			for j := 0; j < s.NumField(); j++ {
				f := typ.Field(j)
				nx, ok := n.hash[f.Name]
				if !ok {
					panic(fmt.Sprintf("bufarrow: field %s not found in node %v", f.Name, n.field.Name))
				}
				if s.Field(j).IsNull(row) {
					continue
				}
				fs := nx.desc.(protoreflect.FieldDescriptor)
				switch {
				case fs.IsList():
					ls := s.Field(j).(*array.List)
					start, end := ls.ValueOffsets(row)
					if start != end {
						lv := msg.Mutable(fs)
						list := lv.List()
						va := ls.ListValues()
						for k := start; k < end; k++ {
							list.Append(
								nx.encode(list.NewElement(), va, int(k)),
							)
						}
						msg.Set(fs, lv)
					}
				case fs.IsMap():
					panic("MAP not supported")
				default:
					msg.Set(fs, nx.encode(msg.NewField(fs), s.Field(j), row))
				}
			}
			return value
		}
		if field.IsList() {
			n.field.Type = arrow.ListOf(n.field.Type)
			setup := n.setup
			n.setup = func(b array.Builder) valueFn {
				ls := b.(*array.ListBuilder)
				value := setup(ls.ValueBuilder())
				return func(v protoreflect.Value, set bool) error {
					if !v.IsValid() {
						ls.AppendNull()
						return nil
					}
					ls.Append(true)
					list := v.List()
					for i := 0; i < list.Len(); i++ {
						err := value(list.Get(i), true)
						if err != nil {
							return err
						}
					}
					return nil
				}
			}
		}
		if field.ContainingOneof() != nil {
			setup := n.setup
			n.setup = func(b array.Builder) valueFn {
				do := setup(b)
				return func(v protoreflect.Value, set bool) error {
					if !set {
						b.AppendNull()
						return nil
					}
					return do(v, set)
				}
			}
		}
		return n
	}
	panic(fmt.Sprintf("%v is not supported ", field.Name()))
}

// build initialises the node's write closure by calling its setup function
// with the concrete Arrow builder for this field.
func (n *node) build(a array.Builder) {
	n.write = n.setup(a)
}

// WriteMessage appends all fields of the given protobuf message by invoking
// each child node's write closure in field-number order.
func (n *node) WriteMessage(msg protoreflect.Message) {
	f := msg.Descriptor().Fields()
	for i := 0; i < f.Len(); i++ {
		n.children[i].write(msg.Get(f.Get(i)), msg.Has(f.Get(i)))
	}
}

// baseType resolves the Arrow data type for a scalar protobuf field and
// wires up the setup (write) and encode (decode) closures on the node.
// Returns nil for message-typed fields, which are handled by createNode.
func (n *node) baseType(field protoreflect.FieldDescriptor) (t arrow.DataType) {
	// fmt.Printf("%v %v \n", n.desc.FullName(), field.Kind()) // debug
	switch field.Kind() {
	case protoreflect.EnumKind:
		t = arrow.PrimitiveTypes.Int32

		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.Int32Builder)
			return func(v protoreflect.Value, set bool) error {
				a.Append(int32(v.Enum()))
				return nil
			}
		}
		n.encode = func(value protoreflect.Value, a arrow.Array, i int) protoreflect.Value {
			return protoreflect.ValueOfEnum(
				protoreflect.EnumNumber(
					a.(*array.Int32).Value(i),
				),
			)
		}
	case protoreflect.BoolKind:
		t = arrow.FixedWidthTypes.Boolean

		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.BooleanBuilder)
			return func(v protoreflect.Value, set bool) error {
				a.Append(v.Bool())
				return nil
			}
		}
		n.encode = func(value protoreflect.Value, a arrow.Array, i int) protoreflect.Value {
			return protoreflect.ValueOfBool(a.(*array.Boolean).Value(i))
		}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		t = arrow.PrimitiveTypes.Int32
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.Int32Builder)
			return func(v protoreflect.Value, set bool) error {
				a.Append(int32(v.Int()))
				return nil
			}
		}
		n.encode = func(value protoreflect.Value, a arrow.Array, i int) protoreflect.Value {
			return protoreflect.ValueOfInt32(a.(*array.Int32).Value(i))
		}
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.Uint32Builder)
			return func(v protoreflect.Value, set bool) error {
				a.Append(uint32(v.Uint()))
				return nil
			}
		}
		t = arrow.PrimitiveTypes.Uint32
		n.encode = func(value protoreflect.Value, a arrow.Array, i int) protoreflect.Value {
			switch at := a.(type) {
			case *array.Uint32:
				return protoreflect.ValueOfUint32(at.Value(i))
			case *array.Int32:
				return protoreflect.ValueOfUint32(uint32(at.Value(i)))
			case *array.Int64:
				return protoreflect.ValueOfUint32(uint32(at.Value(i)))
			}
			return protoreflect.ValueOfUint32(a.(*array.Uint32).Value(i))
		}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.Int64Builder)
			return func(v protoreflect.Value, set bool) error {
				a.Append(v.Int())
				return nil
			}
		}
		t = arrow.PrimitiveTypes.Int64
		n.encode = func(value protoreflect.Value, a arrow.Array, i int) protoreflect.Value {
			return protoreflect.ValueOfInt64(a.(*array.Int64).Value(i))
		}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.Uint64Builder)
			return func(v protoreflect.Value, set bool) error {
				a.Append(v.Uint())
				return nil
			}
		}
		t = arrow.PrimitiveTypes.Uint64
		n.encode = func(value protoreflect.Value, a arrow.Array, i int) protoreflect.Value {
			switch at := a.(type) {
			case *array.Uint64:
				return protoreflect.ValueOfUint64(at.Value(i))
			case *array.Int64:
				return protoreflect.ValueOfUint64(uint64(at.Value(i)))
			}
			return protoreflect.ValueOfUint64(a.(*array.Uint64).Value(i))
		}
	case protoreflect.DoubleKind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.Float64Builder)
			return func(v protoreflect.Value, set bool) error {
				a.Append(v.Float())
				return nil
			}
		}
		t = arrow.PrimitiveTypes.Float64
		n.encode = func(value protoreflect.Value, a arrow.Array, i int) protoreflect.Value {
			return protoreflect.ValueOfFloat64(a.(*array.Float64).Value(i))
		}
	case protoreflect.FloatKind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.Float32Builder)
			return func(v protoreflect.Value, set bool) error {
				a.Append(float32(v.Float()))
				return nil
			}
		}
		t = arrow.PrimitiveTypes.Float32
		n.encode = func(value protoreflect.Value, a arrow.Array, i int) protoreflect.Value {
			return protoreflect.ValueOfFloat32(a.(*array.Float32).Value(i))
		}
	case protoreflect.StringKind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.StringBuilder)
			return func(v protoreflect.Value, set bool) error {
				a.Append(v.String())
				return nil
			}
		}
		t = arrow.BinaryTypes.String
		n.encode = func(value protoreflect.Value, a arrow.Array, i int) protoreflect.Value {
			if a.DataType().ID() == arrow.DICTIONARY {
				d := a.(*array.Dictionary)
				return protoreflect.ValueOfString(
					d.Dictionary().(*array.String).Value(d.GetValueIndex(i)),
				)
			}
			return protoreflect.ValueOfString(a.(*array.String).Value(i))
		}
	case protoreflect.BytesKind:
		n.setup = func(b array.Builder) valueFn {
			a := b.(*array.BinaryBuilder)
			return func(v protoreflect.Value, set bool) error {
				if !v.IsValid() {
					a.AppendNull()
					return nil
				}
				a.Append(v.Bytes())
				return nil
			}
		}
		t = arrow.BinaryTypes.Binary
		n.encode = func(value protoreflect.Value, a arrow.Array, i int) protoreflect.Value {
			if a.DataType().ID() == arrow.DICTIONARY {
				d := a.(*array.Dictionary)
				return protoreflect.ValueOfBytes(
					d.Dictionary().(*array.Binary).Value(d.GetValueIndex(i)),
				)
			}
			return protoreflect.ValueOfBytes(a.(*array.Binary).Value(i))
		}
	}
	if field.IsList() {
		if t != nil {
			setup := n.setup
			n.setup = func(b array.Builder) valueFn {
				ls := b.(*array.ListBuilder)
				vb := setup(ls.ValueBuilder())
				return func(v protoreflect.Value, set bool) error {
					if !v.IsValid() {
						ls.AppendNull()
						return nil
					}
					ls.Append(true)
					list := v.List()
					for i := 0; i < list.Len(); i++ {
						err := vb(list.Get(i), true)
						if err != nil {
							return err
						}
					}
					return nil
				}
			}
			t = arrow.ListOf(t)
		}
	}
	if t != nil && field.ContainingOneof() != nil {
		// Handle oneof for base types
		setup := n.setup
		n.setup = func(b array.Builder) valueFn {
			do := setup(b)
			return func(v protoreflect.Value, set bool) error {
				if !set {
					b.AppendNull()
					return nil
				}
				return do(v, set)
			}
		}

	}
	if field.IsMap() {
		panic("MAP not supported")
	}
	return
}

// nullable reports whether a protobuf field should be represented as a
// nullable Arrow field. Fields with the optional keyword, oneof members,
// and bytes fields are nullable.
func nullable(f protoreflect.FieldDescriptor) bool {
	return f.HasOptionalKeyword() || f.ContainingOneof() != nil ||
		f.Kind() == protoreflect.BytesKind
}
