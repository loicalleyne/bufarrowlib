package bufarrowlib

import (
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
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

	// timestampDescriptor is the message descriptor for google.protobuf.Timestamp.
	// Fields of this type are mapped to arrow.Timestamp(Millisecond, "UTC") in
	// the denormalizer. The main tree builder still treats Timestamps as nested
	// structs (seconds/nanos) for backward compatibility.
	timestampDescriptor protoreflect.MessageDescriptor = (&timestamppb.Timestamp{}).ProtoReflect().Descriptor()
)
