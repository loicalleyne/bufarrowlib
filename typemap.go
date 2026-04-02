package bufarrowlib

import (
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ProtoKindToArrowType returns the Arrow data type corresponding to a protobuf
// field descriptor's scalar kind. In addition to primitive kinds (bool, int32,
// string, etc.), the following well-known and common message types are
// recognised and mapped to flat Arrow scalars:
//
//   - google.protobuf.Timestamp   → Timestamp(ms, UTC)
//   - google.protobuf.Duration    → Duration(ms)
//   - google.protobuf.FieldMask   → String (comma-joined paths)
//   - google.protobuf.*Value      → unwrapped scalar (BoolValue→Boolean, etc.)
//   - google.type.Date            → Date32
//   - google.type.TimeOfDay       → Time64(µs)
//   - google.type.Money           → String (protojson)
//   - google.type.LatLng          → String (protojson)
//   - google.type.Color           → String (protojson)
//   - google.type.PostalAddress   → String (protojson)
//   - google.type.Interval        → String (protojson)
//   - opentelemetry AnyValue      → Binary (proto-marshalled)
//
// Returns nil if the field's kind or message type is not a recognized scalar
// mapping (e.g. a generic message, map, or group).
//
// TODO: add a WithTimestampUnit(arrow.TimeUnit) option to allow callers to
// override the default Millisecond precision.
func ProtoKindToArrowType(fd protoreflect.FieldDescriptor) arrow.DataType {
	// Map fields are represented as repeated MapEntry messages;
	// resolve key/value types and return an Arrow MAP.
	if fd.IsMap() {
		mapMsg := fd.Message()
		keyFD := mapMsg.Fields().ByName("key")
		valueFD := mapMsg.Fields().ByName("value")
		keyType := ProtoKindToArrowType(keyFD)
		if keyType == nil {
			return nil
		}
		valueType := ProtoKindToArrowType(valueFD)
		if valueType == nil {
			return nil
		}
		return arrow.MapOf(keyType, valueType)
	}
	switch fd.Kind() {
	case protoreflect.EnumKind:
		return arrow.PrimitiveTypes.Int32
	case protoreflect.BoolKind:
		return arrow.FixedWidthTypes.Boolean
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return arrow.PrimitiveTypes.Int32
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return arrow.PrimitiveTypes.Uint32
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return arrow.PrimitiveTypes.Int64
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return arrow.PrimitiveTypes.Uint64
	case protoreflect.DoubleKind:
		return arrow.PrimitiveTypes.Float64
	case protoreflect.FloatKind:
		return arrow.PrimitiveTypes.Float32
	case protoreflect.StringKind:
		return arrow.BinaryTypes.String
	case protoreflect.BytesKind:
		return arrow.BinaryTypes.Binary
	case protoreflect.MessageKind, protoreflect.GroupKind:
		msg := fd.Message()
		if msg == nil {
			return nil
		}
		switch msg.FullName() {
		// Well-known types
		case "google.protobuf.Timestamp":
			return &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}
		case "google.protobuf.Duration":
			return arrow.FixedWidthTypes.Duration_ms
		case "google.protobuf.FieldMask":
			return arrow.BinaryTypes.String
		// Wrapper types → unwrapped scalar
		case "google.protobuf.BoolValue":
			return arrow.FixedWidthTypes.Boolean
		case "google.protobuf.Int32Value":
			return arrow.PrimitiveTypes.Int32
		case "google.protobuf.Int64Value":
			return arrow.PrimitiveTypes.Int64
		case "google.protobuf.UInt32Value":
			return arrow.PrimitiveTypes.Uint32
		case "google.protobuf.UInt64Value":
			return arrow.PrimitiveTypes.Uint64
		case "google.protobuf.FloatValue":
			return arrow.PrimitiveTypes.Float32
		case "google.protobuf.DoubleValue":
			return arrow.PrimitiveTypes.Float64
		case "google.protobuf.StringValue":
			return arrow.BinaryTypes.String
		case "google.protobuf.BytesValue":
			return arrow.BinaryTypes.Binary
		// Google common types with native Arrow equivalents
		case "google.type.Date":
			return arrow.FixedWidthTypes.Date32
		case "google.type.TimeOfDay":
			return &arrow.Time64Type{Unit: arrow.Microsecond}
		// Google common types → protojson-serialized String
		case "google.type.Money", "google.type.LatLng", "google.type.Color",
			"google.type.PostalAddress", "google.type.Interval":
			return arrow.BinaryTypes.String
		default:
			// OpenTelemetry AnyValue → proto-serialized Binary
			if msg.FullName() == otelAnyDescriptor.FullName() {
				return arrow.BinaryTypes.Binary
			}
			return nil
		}
	}
	return nil
}

// protoAppendFunc is a function that appends a protoreflect.Value to an Arrow
// array builder. It is returned by ProtoKindToAppendFunc and captures the
// concrete builder type via a closure.
type protoAppendFunc func(protoreflect.Value)

// ProtoKindToAppendFunc returns a closure that appends a protoreflect.Value of
// the appropriate kind to the given Arrow array builder. The builder must match
// the Arrow data type returned by ProtoKindToArrowType for the same field
// descriptor.
//
// Returns nil if the field's kind is not a recognized scalar mapping.
func ProtoKindToAppendFunc(fd protoreflect.FieldDescriptor, b array.Builder) protoAppendFunc {
	// Map fields: iterate the protoreflect.Map and append key/value pairs.
	if fd.IsMap() {
		mapMsg := fd.Message()
		keyFD := mapMsg.Fields().ByName("key")
		valueFD := mapMsg.Fields().ByName("value")
		mb := b.(*array.MapBuilder)
		keyAppend := ProtoKindToAppendFunc(keyFD, mb.KeyBuilder())
		valueAppend := ProtoKindToAppendFunc(valueFD, mb.ItemBuilder())
		if keyAppend == nil || valueAppend == nil {
			return nil
		}
		return func(v protoreflect.Value) {
			mb.Append(true)
			v.Map().Range(func(mk protoreflect.MapKey, mv protoreflect.Value) bool {
				keyAppend(mk.Value())
				valueAppend(mv)
				return true
			})
		}
	}
	switch fd.Kind() {
	case protoreflect.EnumKind:
		a := b.(*array.Int32Builder)
		return func(v protoreflect.Value) { a.Append(int32(v.Enum())) }
	case protoreflect.BoolKind:
		a := b.(*array.BooleanBuilder)
		return func(v protoreflect.Value) { a.Append(v.Bool()) }
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		a := b.(*array.Int32Builder)
		return func(v protoreflect.Value) { a.Append(int32(v.Int())) }
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		a := b.(*array.Uint32Builder)
		return func(v protoreflect.Value) { a.Append(uint32(v.Uint())) }
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		a := b.(*array.Int64Builder)
		return func(v protoreflect.Value) { a.Append(v.Int()) }
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		a := b.(*array.Uint64Builder)
		return func(v protoreflect.Value) { a.Append(v.Uint()) }
	case protoreflect.DoubleKind:
		a := b.(*array.Float64Builder)
		return func(v protoreflect.Value) { a.Append(v.Float()) }
	case protoreflect.FloatKind:
		a := b.(*array.Float32Builder)
		return func(v protoreflect.Value) { a.Append(float32(v.Float())) }
	case protoreflect.StringKind:
		a := b.(*array.StringBuilder)
		return func(v protoreflect.Value) { a.Append(v.String()) }
	case protoreflect.BytesKind:
		a := b.(*array.BinaryBuilder)
		return func(v protoreflect.Value) { a.Append(v.Bytes()) }
	case protoreflect.MessageKind, protoreflect.GroupKind:
		msg := fd.Message()
		if msg == nil {
			return nil
		}
		switch msg.FullName() {
		case "google.protobuf.Timestamp":
			a := b.(*array.TimestampBuilder)
			secFD := msg.Fields().ByName("seconds")
			nanoFD := msg.Fields().ByName("nanos")
			return func(v protoreflect.Value) {
				m := v.Message()
				sec := m.Get(secFD).Int()
				ns := m.Get(nanoFD).Int()
				a.Append(arrow.Timestamp(sec*1000 + ns/1_000_000))
			}
		case "google.protobuf.Duration":
			a := b.(*array.DurationBuilder)
			secFD := msg.Fields().ByName("seconds")
			nanoFD := msg.Fields().ByName("nanos")
			return func(v protoreflect.Value) {
				m := v.Message()
				sec := m.Get(secFD).Int()
				ns := m.Get(nanoFD).Int()
				a.Append(arrow.Duration(sec*1000 + ns/1_000_000))
			}
		case "google.protobuf.FieldMask":
			a := b.(*array.StringBuilder)
			pathsFD := msg.Fields().ByName("paths")
			return func(v protoreflect.Value) {
				paths := v.Message().Get(pathsFD).List()
				parts := make([]string, paths.Len())
				for i := range parts {
					parts[i] = paths.Get(i).String()
				}
				a.Append(strings.Join(parts, ","))
			}
		// Wrapper types → extract inner "value" field
		case "google.protobuf.BoolValue":
			a := b.(*array.BooleanBuilder)
			valFD := msg.Fields().ByName("value")
			return func(v protoreflect.Value) { a.Append(v.Message().Get(valFD).Bool()) }
		case "google.protobuf.Int32Value":
			a := b.(*array.Int32Builder)
			valFD := msg.Fields().ByName("value")
			return func(v protoreflect.Value) { a.Append(int32(v.Message().Get(valFD).Int())) }
		case "google.protobuf.Int64Value":
			a := b.(*array.Int64Builder)
			valFD := msg.Fields().ByName("value")
			return func(v protoreflect.Value) { a.Append(v.Message().Get(valFD).Int()) }
		case "google.protobuf.UInt32Value":
			a := b.(*array.Uint32Builder)
			valFD := msg.Fields().ByName("value")
			return func(v protoreflect.Value) { a.Append(uint32(v.Message().Get(valFD).Uint())) }
		case "google.protobuf.UInt64Value":
			a := b.(*array.Uint64Builder)
			valFD := msg.Fields().ByName("value")
			return func(v protoreflect.Value) { a.Append(v.Message().Get(valFD).Uint()) }
		case "google.protobuf.FloatValue":
			a := b.(*array.Float32Builder)
			valFD := msg.Fields().ByName("value")
			return func(v protoreflect.Value) { a.Append(float32(v.Message().Get(valFD).Float())) }
		case "google.protobuf.DoubleValue":
			a := b.(*array.Float64Builder)
			valFD := msg.Fields().ByName("value")
			return func(v protoreflect.Value) { a.Append(v.Message().Get(valFD).Float()) }
		case "google.protobuf.StringValue":
			a := b.(*array.StringBuilder)
			valFD := msg.Fields().ByName("value")
			return func(v protoreflect.Value) { a.Append(v.Message().Get(valFD).String()) }
		case "google.protobuf.BytesValue":
			a := b.(*array.BinaryBuilder)
			valFD := msg.Fields().ByName("value")
			return func(v protoreflect.Value) { a.Append(v.Message().Get(valFD).Bytes()) }
		// Google common types with native Arrow equivalents
		case "google.type.Date":
			a := b.(*array.Date32Builder)
			yearFD := msg.Fields().ByName("year")
			monthFD := msg.Fields().ByName("month")
			dayFD := msg.Fields().ByName("day")
			return func(v protoreflect.Value) {
				m := v.Message()
				y := int(m.Get(yearFD).Int())
				mo := time.Month(m.Get(monthFD).Int())
				d := int(m.Get(dayFD).Int())
				a.Append(arrow.Date32FromTime(time.Date(y, mo, d, 0, 0, 0, 0, time.UTC)))
			}
		case "google.type.TimeOfDay":
			a := b.(*array.Time64Builder)
			hoursFD := msg.Fields().ByName("hours")
			minutesFD := msg.Fields().ByName("minutes")
			secondsFD := msg.Fields().ByName("seconds")
			nanosFD := msg.Fields().ByName("nanos")
			return func(v protoreflect.Value) {
				m := v.Message()
				h := m.Get(hoursFD).Int()
				mi := m.Get(minutesFD).Int()
				s := m.Get(secondsFD).Int()
				ns := m.Get(nanosFD).Int()
				us := (h*3600+mi*60+s)*1_000_000 + ns/1000
				a.Append(arrow.Time64(us))
			}
		// Google common types → protojson-serialized String
		case "google.type.Money", "google.type.LatLng", "google.type.Color",
			"google.type.PostalAddress", "google.type.Interval":
			a := b.(*array.StringBuilder)
			return func(v protoreflect.Value) {
				bs, err := protojson.Marshal(v.Message().Interface())
				if err != nil {
					a.AppendNull()
					return
				}
				a.Append(string(bs))
			}
		default:
			// OpenTelemetry AnyValue → proto-serialized Binary
			if msg.FullName() == otelAnyDescriptor.FullName() {
				a := b.(*array.BinaryBuilder)
				return func(v protoreflect.Value) {
					bs, err := proto.Marshal(v.Message().Interface())
					if err != nil {
						a.AppendNull()
						return
					}
					a.Append(bs)
				}
			}
			return nil
		}
	}
	return nil
}

// isScalarLeaf reports whether the given field descriptor is a recognized
// scalar type for denormalization — either a primitive protobuf kind or a
// well-known / common message type (Timestamp, Duration, wrappers, Date,
// TimeOfDay, Money, etc.) that maps to a flat Arrow type.
func isScalarLeaf(fd protoreflect.FieldDescriptor) bool {
	return ProtoKindToArrowType(fd) != nil
}

// ExprKindToArrowType returns the Arrow data type corresponding to a
// [protoreflect.Kind]. This is used for denormalizer columns whose type is
// determined by an [Expr] function's output kind rather than by a leaf field
// descriptor.
//
// Only the primitive scalar kinds that Expr functions can produce are handled:
//
//	BoolKind   → Boolean
//	Int32Kind  → Int32     Int64Kind  → Int64
//	Uint32Kind → Uint32    Uint64Kind → Uint64
//	FloatKind  → Float32   DoubleKind → Float64
//	StringKind → String    BytesKind  → Binary
//	EnumKind   → Int32
//
// Returns nil for message, group, or unrecognised kinds.
func ExprKindToArrowType(kind protoreflect.Kind) arrow.DataType {
	switch kind {
	case protoreflect.BoolKind:
		return arrow.FixedWidthTypes.Boolean
	case protoreflect.EnumKind:
		return arrow.PrimitiveTypes.Int32
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return arrow.PrimitiveTypes.Int32
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return arrow.PrimitiveTypes.Uint32
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return arrow.PrimitiveTypes.Int64
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return arrow.PrimitiveTypes.Uint64
	case protoreflect.FloatKind:
		return arrow.PrimitiveTypes.Float32
	case protoreflect.DoubleKind:
		return arrow.PrimitiveTypes.Float64
	case protoreflect.StringKind:
		return arrow.BinaryTypes.String
	case protoreflect.BytesKind:
		return arrow.BinaryTypes.Binary
	default:
		return nil
	}
}

// ExprKindToAppendFunc returns a closure that appends a [protoreflect.Value]
// of the given kind to the Arrow array builder b. The builder must match the
// type returned by [ExprKindToArrowType] for the same kind.
//
// This is the Expr-output counterpart of [ProtoKindToAppendFunc]; it handles
// only the primitive scalar kinds that Expr functions can produce.
// Returns nil for unsupported kinds.
func ExprKindToAppendFunc(kind protoreflect.Kind, b array.Builder) protoAppendFunc {
	switch kind {
	case protoreflect.BoolKind:
		a := b.(*array.BooleanBuilder)
		return func(v protoreflect.Value) { a.Append(v.Bool()) }
	case protoreflect.EnumKind:
		a := b.(*array.Int32Builder)
		return func(v protoreflect.Value) { a.Append(int32(v.Enum())) }
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		a := b.(*array.Int32Builder)
		return func(v protoreflect.Value) { a.Append(int32(v.Int())) }
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		a := b.(*array.Uint32Builder)
		return func(v protoreflect.Value) { a.Append(uint32(v.Uint())) }
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		a := b.(*array.Int64Builder)
		return func(v protoreflect.Value) { a.Append(v.Int()) }
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		a := b.(*array.Uint64Builder)
		return func(v protoreflect.Value) { a.Append(v.Uint()) }
	case protoreflect.FloatKind:
		a := b.(*array.Float32Builder)
		return func(v protoreflect.Value) { a.Append(float32(v.Float())) }
	case protoreflect.DoubleKind:
		a := b.(*array.Float64Builder)
		return func(v protoreflect.Value) { a.Append(v.Float()) }
	case protoreflect.StringKind:
		a := b.(*array.StringBuilder)
		return func(v protoreflect.Value) { a.Append(v.String()) }
	case protoreflect.BytesKind:
		a := b.(*array.BinaryBuilder)
		return func(v protoreflect.Value) { a.Append(v.Bytes()) }
	default:
		return nil
	}
}

// leafFieldDescriptor extracts the leaf FieldDescriptor from a pbpath step
// descriptor. The desc should be the descriptor at the terminal node of a
// compiled path — either a FieldDescriptor for scalar fields or a
// MessageDescriptor for message-typed terminal nodes.
// Returns an error if the terminal node is not a FieldDescriptor.
func leafFieldDescriptor(desc protoreflect.Descriptor) (protoreflect.FieldDescriptor, error) {
	fd, ok := desc.(protoreflect.FieldDescriptor)
	if !ok {
		return nil, fmt.Errorf("bufarrow: terminal path node is %T, not a FieldDescriptor", desc)
	}
	return fd, nil
}
