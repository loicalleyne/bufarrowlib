package bufarrowlib

import (
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// flattenableWKT reports whether fd is a non-recursive well-known type that
// WithWellKnownTypes maps to a flat Arrow scalar. Map fields are excluded:
// they carry their own key/value node structure, and ProtoKindToArrowType
// would report the map type rather than a leaf.
//
// google.protobuf.Duration is excluded as well. The denormalizer maps it to
// Arrow Duration(ms), but arrow-go's Parquet writer has no DURATION support
// ("not implemented: support for DURATION"), and the full-fidelity schema is
// converted to a Parquet schema at construction time. Flattening it would make
// every message containing a Duration unconstructable, so it keeps its
// struct{seconds, nanos} form.
func flattenableWKT(fd protoreflect.FieldDescriptor) bool {
	if fd.IsMap() {
		return false
	}
	if fd.Kind() != protoreflect.MessageKind && fd.Kind() != protoreflect.GroupKind {
		return false
	}
	msg := fd.Message()
	if msg == nil || isRecursiveWKT(msg) || msg.FullName() == otelAnyName {
		return false
	}
	if msg.FullName() == "google.protobuf.Duration" {
		return false
	}
	return ProtoKindToArrowType(fd) != nil
}

// wktFlattenSetup adapts ProtoKindToAppendFunc to the node tree's valueFn.
// Unset fields become nulls; the closure never returns an error because
// node.WriteMessage discards it and a non-appending path would desynchronise
// the builder.
func wktFlattenSetup(fd protoreflect.FieldDescriptor) func(array.Builder) valueFn {
	return func(b array.Builder) valueFn {
		appendFn := ProtoKindToAppendFunc(fd, b)
		if appendFn == nil {
			panic("bufarrow: no append func for flattened well-known type " + string(fd.Message().FullName()))
		}
		return func(v protoreflect.Value, set bool) error {
			if !v.IsValid() || !set || !v.Message().IsValid() {
				b.AppendNull()
				return nil
			}
			appendFn(v)
			return nil
		}
	}
}

// wktFlattenEncode decodes a flattened Arrow cell back into the well-known
// message it came from. It is the inverse of wktFlattenSetup and is only used
// when WithWellKnownTypes is enabled.
//
// On an unrecognised type or malformed cell it returns the zero Value, which
// leaves the field unset rather than producing a partially populated message.
func wktFlattenEncode(fd protoreflect.FieldDescriptor) encodeFn {
	md := fd.Message()
	fieldByName := func(n string) protoreflect.FieldDescriptor {
		return md.Fields().ByName(protoreflect.Name(n))
	}

	switch md.FullName() {
	case "google.protobuf.Timestamp":
		secFD, nanoFD := fieldByName("seconds"), fieldByName("nanos")
		return func(value protoreflect.Value, a arrow.Array, row int) protoreflect.Value {
			if a.IsNull(row) {
				return protoreflect.Value{}
			}
			ms := int64(a.(*array.Timestamp).Value(row))
			m := value.Message()
			m.Set(secFD, protoreflect.ValueOfInt64(floorDiv(ms, 1000)))
			m.Set(nanoFD, protoreflect.ValueOfInt32(int32(floorMod(ms, 1000)*1_000_000)))
			return value
		}

	case "google.protobuf.Duration":
		secFD, nanoFD := fieldByName("seconds"), fieldByName("nanos")
		return func(value protoreflect.Value, a arrow.Array, row int) protoreflect.Value {
			if a.IsNull(row) {
				return protoreflect.Value{}
			}
			ms := int64(a.(*array.Duration).Value(row))
			m := value.Message()
			// Duration is signed; seconds and nanos must share a sign.
			m.Set(secFD, protoreflect.ValueOfInt64(ms/1000))
			m.Set(nanoFD, protoreflect.ValueOfInt32(int32((ms%1000)*1_000_000)))
			return value
		}

	case "google.protobuf.FieldMask":
		pathsFD := fieldByName("paths")
		return func(value protoreflect.Value, a arrow.Array, row int) protoreflect.Value {
			if a.IsNull(row) {
				return protoreflect.Value{}
			}
			s := stringCell(a, row)
			m := value.Message()
			if s == "" {
				return value
			}
			l := m.Mutable(pathsFD).List()
			for _, p := range strings.Split(s, ",") {
				l.Append(protoreflect.ValueOfString(p))
			}
			return value
		}

	case "google.protobuf.BoolValue", "google.protobuf.Int32Value",
		"google.protobuf.Int64Value", "google.protobuf.UInt32Value",
		"google.protobuf.UInt64Value", "google.protobuf.FloatValue",
		"google.protobuf.DoubleValue", "google.protobuf.StringValue",
		"google.protobuf.BytesValue":
		valFD := fieldByName("value")
		name := md.FullName()
		return func(value protoreflect.Value, a arrow.Array, row int) protoreflect.Value {
			if a.IsNull(row) {
				return protoreflect.Value{}
			}
			m := value.Message()
			switch name {
			case "google.protobuf.BoolValue":
				m.Set(valFD, protoreflect.ValueOfBool(a.(*array.Boolean).Value(row)))
			case "google.protobuf.Int32Value":
				m.Set(valFD, protoreflect.ValueOfInt32(a.(*array.Int32).Value(row)))
			case "google.protobuf.Int64Value":
				m.Set(valFD, protoreflect.ValueOfInt64(a.(*array.Int64).Value(row)))
			case "google.protobuf.UInt32Value":
				m.Set(valFD, protoreflect.ValueOfUint32(a.(*array.Uint32).Value(row)))
			case "google.protobuf.UInt64Value":
				m.Set(valFD, protoreflect.ValueOfUint64(a.(*array.Uint64).Value(row)))
			case "google.protobuf.FloatValue":
				m.Set(valFD, protoreflect.ValueOfFloat32(a.(*array.Float32).Value(row)))
			case "google.protobuf.DoubleValue":
				m.Set(valFD, protoreflect.ValueOfFloat64(a.(*array.Float64).Value(row)))
			case "google.protobuf.StringValue":
				m.Set(valFD, protoreflect.ValueOfString(stringCell(a, row)))
			case "google.protobuf.BytesValue":
				m.Set(valFD, protoreflect.ValueOfBytes(binaryCell(a, row)))
			}
			return value
		}

	case "google.type.Date":
		yFD, moFD, dFD := fieldByName("year"), fieldByName("month"), fieldByName("day")
		return func(value protoreflect.Value, a arrow.Array, row int) protoreflect.Value {
			if a.IsNull(row) {
				return protoreflect.Value{}
			}
			t := a.(*array.Date32).Value(row).ToTime()
			m := value.Message()
			m.Set(yFD, protoreflect.ValueOfInt32(int32(t.Year())))
			m.Set(moFD, protoreflect.ValueOfInt32(int32(t.Month())))
			m.Set(dFD, protoreflect.ValueOfInt32(int32(t.Day())))
			return value
		}

	case "google.type.TimeOfDay":
		hFD, miFD, sFD, nFD := fieldByName("hours"), fieldByName("minutes"), fieldByName("seconds"), fieldByName("nanos")
		return func(value protoreflect.Value, a arrow.Array, row int) protoreflect.Value {
			if a.IsNull(row) {
				return protoreflect.Value{}
			}
			us := int64(a.(*array.Time64).Value(row))
			m := value.Message()
			m.Set(hFD, protoreflect.ValueOfInt32(int32(us/int64(time.Hour/time.Microsecond))))
			m.Set(miFD, protoreflect.ValueOfInt32(int32(us/int64(time.Minute/time.Microsecond)%60)))
			m.Set(sFD, protoreflect.ValueOfInt32(int32(us/1_000_000%60)))
			m.Set(nFD, protoreflect.ValueOfInt32(int32(us%1_000_000*1000)))
			return value
		}

	case "google.type.Money", "google.type.LatLng", "google.type.Color",
		"google.type.PostalAddress", "google.type.Interval":
		return func(value protoreflect.Value, a arrow.Array, row int) protoreflect.Value {
			if a.IsNull(row) {
				return protoreflect.Value{}
			}
			if err := protojson.Unmarshal([]byte(stringCell(a, row)), value.Message().Interface()); err != nil {
				return protoreflect.Value{}
			}
			return value
		}
	}
	return nil
}

// stringCell reads a string cell, transparently handling dictionary encoding.
func stringCell(a arrow.Array, row int) string {
	if a.DataType().ID() == arrow.DICTIONARY {
		d := a.(*array.Dictionary)
		return d.Dictionary().(*array.String).Value(d.GetValueIndex(row))
	}
	return a.(*array.String).Value(row)
}

// binaryCell reads a binary cell, transparently handling dictionary encoding.
func binaryCell(a arrow.Array, row int) []byte {
	if a.DataType().ID() == arrow.DICTIONARY {
		d := a.(*array.Dictionary)
		return d.Dictionary().(*array.Binary).Value(d.GetValueIndex(row))
	}
	return a.(*array.Binary).Value(row)
}

// floorDiv and floorMod give Euclidean division, so that pre-epoch timestamps
// decompose into a non-negative nanos remainder as protobuf requires.
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

func floorMod(a, b int64) int64 { return a - floorDiv(a, b)*b }
