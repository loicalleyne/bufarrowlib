package bufarrowlib

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// Well-known protobuf message descriptors used by the type mapper and the
// tree builder to recognise special types that should be represented as
// flat Arrow scalars rather than nested Arrow structs.
var (
	// otelAnyDescriptor is the message descriptor for the OpenTelemetry
	// AnyValue type. Fields of this type are serialised to Arrow Binary
	// columns containing raw protobuf bytes.
	otelAnyDescriptor protoreflect.MessageDescriptor = (&commonv1.AnyValue{}).ProtoReflect().Descriptor()

	// otelAnyName is the fully-qualified name of the OpenTelemetry AnyValue.
	// Dispatch is by name rather than by descriptor identity because
	// descriptors compiled at runtime by protocompile are distinct objects
	// from the linked-in ones.
	otelAnyName = otelAnyDescriptor.FullName()

	// timestampDescriptor is the message descriptor for google.protobuf.Timestamp.
	// Fields of this type are mapped to arrow.Timestamp(Millisecond, "UTC") in
	// the denormalizer. The main tree builder still treats Timestamps as nested
	// structs (seconds/nanos) for backward compatibility.
	timestampDescriptor protoreflect.MessageDescriptor = (&timestamppb.Timestamp{}).ProtoReflect().Descriptor()

	// recursiveWKTs are well-known types that recursively contain themselves
	// and therefore have no finite Arrow struct representation. Struct holds a
	// map of Value; Value's oneof reaches Struct and ListValue; ListValue is a
	// repeated Value. They terminate as their canonical protojson encoding.
	recursiveWKTs = map[protoreflect.FullName]struct{}{
		"google.protobuf.Struct":    {},
		"google.protobuf.Value":     {},
		"google.protobuf.ListValue": {},
	}
)

// isRecursiveWKT reports whether md is a well-known type that cannot be
// expanded into a finite Arrow schema.
func isRecursiveWKT(md protoreflect.MessageDescriptor) bool {
	_, ok := recursiveWKTs[md.FullName()]
	return ok
}

// jsonWKTSetup returns a node setup closure that stores a recursive well-known
// type as its canonical protojson encoding in an Arrow string column.
//
// The closure never returns an error. A valueFn that returns without appending
// would leave the column short of its siblings, and node.WriteMessage discards
// the returned error, so the desynchronisation would be silent. Unset values
// and unmarshallable values both become nulls instead.
func jsonWKTSetup() func(array.Builder) valueFn {
	return func(b array.Builder) valueFn {
		a := b.(*array.StringBuilder)
		return func(v protoreflect.Value, set bool) error {
			if !v.IsValid() || !set || !v.Message().IsValid() {
				a.AppendNull()
				return nil
			}
			bs, err := protojson.Marshal(v.Message().Interface())
			if err != nil {
				// google.protobuf.Value with no oneof member set cannot be
				// marshalled. See docs/artifacts/protojson-wkt.md.
				a.AppendNull()
				return nil
			}
			a.Append(string(bs))
			return nil
		}
	}
}

// jsonWKTEncode returns the decode counterpart of jsonWKTSetup.
func jsonWKTEncode() encodeFn {
	return func(value protoreflect.Value, a arrow.Array, row int) protoreflect.Value {
		if a.IsNull(row) {
			return protoreflect.Value{}
		}
		var s string
		if a.DataType().ID() == arrow.DICTIONARY {
			d := a.(*array.Dictionary)
			s = d.Dictionary().(*array.String).Value(d.GetValueIndex(row))
		} else {
			s = a.(*array.String).Value(row)
		}
		if err := protojson.Unmarshal([]byte(s), value.Message().Interface()); err != nil {
			return protoreflect.Value{}
		}
		return value
	}
}

// binaryWKTSetup returns a node setup closure that stores a message as raw
// protobuf bytes in an Arrow binary column. Used for the OpenTelemetry
// AnyValue, which is recursive via ArrayValue and KvlistValue.
func binaryWKTSetup() func(array.Builder) valueFn {
	return func(b array.Builder) valueFn {
		a := b.(*array.BinaryBuilder)
		return func(v protoreflect.Value, set bool) error {
			if !v.IsValid() {
				a.AppendNull()
				return nil
			}
			bs, err := proto.Marshal(v.Message().Interface())
			if err != nil {
				a.AppendNull()
				return nil
			}
			a.Append(bs)
			return nil
		}
	}
}

// binaryWKTEncode returns the decode counterpart of binaryWKTSetup.
func binaryWKTEncode() encodeFn {
	return func(value protoreflect.Value, a arrow.Array, row int) protoreflect.Value {
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
