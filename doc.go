// Package bufarrowlib converts protobuf messages to Apache Arrow record
// batches (and back), providing high-throughput ingestion pipelines and
// optional denormalization for analytics workloads.
//
// # Core types
//
// A [Transcoder] is the central type. It holds the compiled protobuf schema,
// an Arrow record builder for the full message, and an optional denormalizer
// that projects selected scalar paths into a flat Arrow record. Construct one
// with [New] (from a [protoreflect.MessageDescriptor]) or [NewFromFile]
// (from a .proto source file).
//
// A [HyperType] wraps a compiled [buf.build/go/hyperpb.MessageType] and
// enables online profile-guided optimization (PGO). All [Transcoder] instances
// sharing a HyperType contribute profiling data, and a recompile atomically
// upgrades the parser for all of them.
//
// # Concurrency
//
// Transcoder methods are NOT safe for concurrent use. Call [Transcoder.Clone]
// to obtain independent copies for parallel goroutines. HyperType IS safe for
// concurrent use.
//
// # Hot-path guidance
//
//   - Use [Transcoder.AppendRaw] / [Transcoder.AppendDenormRaw] with a
//     [HyperType] for the fastest path from raw proto bytes to Arrow.
//   - Never call [New] or [Clone] inside a message-processing loop; construction
//     is expensive (~300 µs). Pre-create workers and feed messages through
//     channels.
//   - Always call defer rec.Release() on every [arrow.RecordBatch] returned by
//     [Transcoder.NewRecordBatch] or [Transcoder.NewDenormalizerRecordBatch].
//
// # Denormalization
//
// The denormalizer (configured via [WithDenormalizerPlan]) evaluates a set of
// protobuf field paths—potentially through repeated fields—and writes one flat
// Arrow record per cross-joined fan-out row. Configure it from a YAML file
// with [NewTranscoderFromConfigFile] or [ParseDenormConfigFile] +
// [NewTranscoderFromConfig].
package bufarrowlib
