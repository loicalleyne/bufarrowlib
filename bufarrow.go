package bufarrowlib

import (
	"context"
	"errors"
	"fmt"
	"io"

	"buf.build/go/hyperpb"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Transcoder converts protobuf messages to Apache Arrow record batches and
// back. It holds the compiled message schema, an Arrow record builder for
// the full message ("stencil"), and optionally a separate denormalizer that
// projects selected paths into a flat Arrow record suitable for analytics.
//
// A Transcoder is not safe for concurrent use; call [Transcoder.Clone] to
// create independent copies for parallel goroutines.
type Transcoder struct {
	msgDesc       protoreflect.MessageDescriptor
	msg           *message
	msgType       *hyperpb.MessageType
	stencil       proto.Message
	stencilCustom proto.Message
	// customFieldRemap maps original custom field numbers to their renumbered
	// positions in the merged descriptor. Built once in New() and shared
	// (read-only) across clones. Used by AppendRawMerged/AppendDenormRawMerged
	// to rewrite customBytes wire tags before concatenation with baseBytes.
	customFieldRemap map[int32]int32
	denormBuilder    *array.RecordBuilder
	denormPlan       *pbpath.Plan
	denormSchema     *arrow.Schema
	denormGroups     []fanoutGroup
	denormCols       []denormColumn
	opts             *Opt

	// hyperType is the shared HyperType coordinator for PGO-enabled raw-bytes
	// ingestion. When non-nil, AppendRaw/AppendDenormRaw use it to load the
	// current compiled MessageType and record profiling data. Multiple
	// Transcoders (including clones) may share the same HyperType.
	hyperType *HyperType

	// hyperShared is per-Transcoder hyperpb memory reuse state. It is created
	// fresh for each Transcoder/clone and must not be shared.
	hyperShared *hyperpb.Shared

	// unmarshalScratch is a fixed-size option array reused across AppendRaw /
	// AppendDenormRaw / AppendRawMerged / AppendDenormRawMerged calls when PGO
	// profiling is active. Eliminates the per-call append allocation.
	unmarshalScratch [1]hyperpb.UnmarshalOption

	// mergeScratch is a reusable byte buffer for AppendRawMerged and
	// AppendDenormRawMerged. Accumulates baseBytes || remapped(customBytes) in
	// a single allocation that grows to capacity once and is reused thereafter.
	mergeScratch []byte

	// Scratch slices reused across AppendDenorm calls to avoid per-call allocations.
	denormGroupCounts []int
	denormGroupIsNull []bool
	denormBranchIdx   []int
	denormNullCols    []bool
}

// Opt holds the option values collected from [Option] functions and passed
// to [New] or [NewFromFile]. Fields are unexported; use the With* helpers.
type Opt struct {
	customMsgDesc     protoreflect.MessageDescriptor
	customProtoPath   string
	customMsgName     string
	customImportPaths []string
	denormPaths       []pbpath.PlanPathSpec
	hyperType         *HyperType
}

// Option is a functional option applied to [New] and [NewFromFile] to
// configure schema merging, denormalization, or other behaviours.
type Option func(config)

// config is the underlying pointer type threaded through [Option] closures.
type config = *Opt

// WithCustomMessage provides a [protoreflect.MessageDescriptor] whose fields
// will be merged into the base message schema. The merged schema can be
// populated with [Transcoder.AppendWithCustom].
// This option is mutually exclusive with [WithCustomMessageFile].
func WithCustomMessage(msgDesc protoreflect.MessageDescriptor) Option {
	return func(cfg config) {
		cfg.customMsgDesc = msgDesc
	}
}

// WithCustomMessageFile specifies a .proto file and message name whose fields
// will be merged into the base message schema. The .proto file is compiled at
// schema creation time using protocompile. The merged schema can be populated
// with [Transcoder.AppendWithCustom].
// This option is mutually exclusive with [WithCustomMessage].
func WithCustomMessageFile(protoFilePath, messageName string, importPaths []string) Option {
	return func(cfg config) {
		cfg.customProtoPath = protoFilePath
		cfg.customMsgName = messageName
		cfg.customImportPaths = importPaths
	}
}

// WithDenormalizerPlan configures one or more protobuf field paths to project
// into a flat (denormalized) Arrow record. Each path is specified as a
// [pbpath.PlanPathSpec] created via [pbpath.PlanPath], which supports
// per-path options such as [pbpath.Alias] and [pbpath.StrictPath].
//
// Paths that traverse repeated fields with a wildcard [*] or range [start:end]
// produce fan-out rows. Multiple independent fan-out groups are cross-joined;
// empty fan-out groups produce a single null row (left-join semantics).
//
// Each leaf path must terminate at a scalar protobuf field or a recognized
// well-known message type (google.protobuf.Timestamp, otel AnyValue).
// Message-typed terminal nodes are rejected at schema creation time.
func WithDenormalizerPlan(paths ...pbpath.PlanPathSpec) Option {
	return func(cfg config) {
		cfg.denormPaths = append(cfg.denormPaths, paths...)
	}
}

// WithHyperType provides a shared [HyperType] coordinator for PGO-enabled
// raw-bytes ingestion via [Transcoder.AppendRaw] and
// [Transcoder.AppendDenormRaw]. The HyperType's compiled
// [hyperpb.MessageType] is used instead of compiling a new one, and all
// Transcoders sharing the same HyperType contribute profiling data for
// online recompilation.
//
// Create a HyperType with [NewHyperType] and pass it to multiple New/Clone
// calls. Call [HyperType.Recompile] to upgrade the parser with collected
// profile data.
func WithHyperType(ht *HyperType) Option {
	return func(cfg config) {
		cfg.hyperType = ht
	}
}

// NewFromFile returns a new [Transcoder] by compiling a .proto file at runtime.
// protoFilePath is the path to the .proto file, messageName is the top-level
// message to use, and importPaths are the directories to search for imports.
// Options include [WithDenormalizerPlan], [WithCustomMessage], and
// [WithCustomMessageFile].
func NewFromFile(protoFilePath, messageName string, importPaths []string, mem memory.Allocator, opts ...Option) (*Transcoder, error) {
	fd, err := CompileProtoToFileDescriptor(protoFilePath, importPaths)
	if err != nil {
		return nil, fmt.Errorf("bufarrow: failed to compile proto file %s: %w", protoFilePath, err)
	}
	msgDesc, err := GetMessageDescriptorByName(fd, messageName)
	if err != nil {
		return nil, fmt.Errorf("bufarrow: %w", err)
	}
	return New(msgDesc, mem, opts...)
}

// New returns a new [Transcoder] from a pre-resolved message descriptor.
// Options include [WithDenormalizerPlan], [WithCustomMessage], and
// [WithCustomMessageFile]. WithDenormalizerPlan creates a separate flat Arrow
// record for analytics whilst WithCustomMessage/WithCustomMessageFile expand
// the schema of the proto.MessageDescriptor used as input.
func New(msgDesc protoreflect.MessageDescriptor, mem memory.Allocator, opts ...Option) (tc *Transcoder, err error) {
	defer func() {
		e := recover()
		if e != nil {
			switch x := e.(type) {
			case error:
				err = x
			case string:
				err = errors.New(x)
			default:
				panic(x)
			}
		}
	}()

	o := new(Opt)
	for _, f := range opts {
		f(o)
	}

	// Validate mutual exclusivity of custom message options
	if o.customMsgDesc != nil && o.customProtoPath != "" {
		return nil, fmt.Errorf("bufarrow: WithCustomMessage and WithCustomMessageFile are mutually exclusive")
	}

	// Resolve custom message descriptor from .proto file if specified
	if o.customProtoPath != "" {
		fd, err := CompileProtoToFileDescriptor(o.customProtoPath, o.customImportPaths)
		if err != nil {
			return nil, fmt.Errorf("bufarrow: failed to compile custom proto file %s: %w", o.customProtoPath, err)
		}
		o.customMsgDesc, err = GetMessageDescriptorByName(fd, o.customMsgName)
		if err != nil {
			return nil, fmt.Errorf("bufarrow: %w", err)
		}
	}

	var mergedMsgDesc protoreflect.MessageDescriptor
	if o.customMsgDesc != nil {
		mergedMsgDesc, err = MergeMessageDescriptors(msgDesc, o.customMsgDesc, string(msgDesc.Name())+"Custom")
		if err != nil {
			return nil, fmt.Errorf("bufarrow: failed to merge custom message: %w", err)
		}
	}

	// Use merged descriptor if custom fields were provided, otherwise use the base descriptor
	activeMsgDesc := msgDesc
	if mergedMsgDesc != nil {
		activeMsgDesc = mergedMsgDesc
	}

	var msgType *hyperpb.MessageType
	if o.hyperType != nil {
		msgType = o.hyperType.Type()
	} else {
		msgType = hyperpb.CompileMessageDescriptor(activeMsgDesc)
	}
	a := hyperpb.NewMessage(msgType)
	tc = &Transcoder{msgDesc: msgDesc, msgType: msgType, stencil: a, opts: o}
	if o.hyperType != nil {
		tc.hyperType = o.hyperType
		tc.hyperShared = new(hyperpb.Shared)
	}

	var b *message
	if mergedMsgDesc != nil {
		// Build stencilCustom from the merged descriptor for use by AppendWithCustom
		tc.stencilCustom = dynamicpb.NewMessage(mergedMsgDesc)

		// Build customFieldRemap: originalFieldNumber → mergedFieldNumber.
		// MergeMessageDescriptors renumbers custom fields to start above the
		// base message's max field number. AppendRawMerged needs to rewrite
		// customBytes (encoded with original numbers) before concatenation.
		if o.customMsgDesc != nil {
			remap := make(map[int32]int32, o.customMsgDesc.Fields().Len())
			for i := 0; i < o.customMsgDesc.Fields().Len(); i++ {
				orig := o.customMsgDesc.Fields().Get(i)
				merged := mergedMsgDesc.Fields().ByName(orig.Name())
				if merged != nil && merged.Number() != orig.Number() {
					remap[int32(orig.Number())] = int32(merged.Number())
				}
			}
			if len(remap) > 0 {
				tc.customFieldRemap = remap
			}
		}

		b = build(a.ProtoReflect())
		b.build(mem)
	} else {
		b = build(a.ProtoReflect())
		b.build(mem)
	}

	tc.msg = b

	if len(o.denormPaths) > 0 {
		if err := tc.compileDenormPlan(mem); err != nil {
			return nil, err
		}
	}
	return tc, err
}

// Clone returns an identical [Transcoder]. Use in concurrency scenarios as
// Transcoder methods are not concurrency safe.
//
// The compiled denormalizer [Plan] is shared (it is immutable), but the Arrow
// builders, scratch buffers, and leaf scratch are freshly allocated so each
// clone can operate independently.
func (s *Transcoder) Clone(mem memory.Allocator) (tc *Transcoder, err error) {
	defer func() {
		e := recover()
		if e != nil {
			switch x := e.(type) {
			case error:
				err = x
			case string:
				err = errors.New(x)
			default:
				panic(x)
			}
		}
	}()
	a := s.stencil
	b := build(a.ProtoReflect())
	b.build(mem)
	tc = &Transcoder{msgDesc: s.msgDesc, msg: b, msgType: s.msgType, stencil: a, opts: s.opts}
	if s.hyperType != nil {
		tc.hyperType = s.hyperType
		tc.hyperShared = new(hyperpb.Shared)
	}
	if s.stencilCustom != nil {
		tc.stencilCustom = dynamicpb.NewMessage(s.stencilCustom.ProtoReflect().Descriptor())
		tc.customFieldRemap = s.customFieldRemap // immutable map, safe to share across clones
	}
	if s.denormPlan != nil {
		if err := tc.cloneDenorm(s, mem); err != nil {
			return nil, err
		}
	}
	return tc, err
}

// Append appends a protobuf message to the transcoder's Arrow record builder.
// This method is not safe for concurrent use.
func (s *Transcoder) Append(value proto.Message) {
	s.msg.append(value.ProtoReflect())
}

// AppendWithCustom appends a protobuf value merged with custom field values to
// the transcoder's builder. The custom [proto.Message] must conform to the
// message descriptor provided via [WithCustomMessage] or
// [WithCustomMessageFile]. Both messages are marshalled to bytes and
// unmarshalled into the merged stencil for appending.
// This method is not safe for concurrent use.
func (s *Transcoder) AppendWithCustom(value proto.Message, custom proto.Message) error {
	if s.stencilCustom == nil {
		return fmt.Errorf("bufarrow: AppendWithCustom called without custom message configured")
	}
	m, err := s.mergeMessages(value, custom)
	if err != nil {
		return err
	}
	s.msg.append(m.ProtoReflect())
	return nil
}

// mergeMessages marshals both base and custom messages, then unmarshals them
// sequentially into a clone of the merged stencil so their fields combine.
func (s *Transcoder) mergeMessages(value proto.Message, custom proto.Message) (proto.Message, error) {
	v := proto.Clone(s.stencilCustom)
	vb, err := proto.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("bufarrow: failed to marshal base message: %w", err)
	}
	if err := proto.Unmarshal(vb, v); err != nil {
		return nil, fmt.Errorf("bufarrow: failed to unmarshal base message into merged stencil: %w", err)
	}
	cb, err := proto.Marshal(custom)
	if err != nil {
		return nil, fmt.Errorf("bufarrow: failed to marshal custom message: %w", err)
	}
	if err := proto.Unmarshal(cb, v); err != nil {
		return nil, fmt.Errorf("bufarrow: failed to unmarshal custom message into merged stencil: %w", err)
	}
	return v, nil
}

// NewRecordBatch returns the buffered builder contents as an
// [arrow.RecordBatch]. The builder is reset and can be reused.
func (s *Transcoder) NewRecordBatch() arrow.RecordBatch {
	return s.msg.NewRecordBatch()
}

// DenormalizerBuilder returns the denormalizer's Arrow [array.RecordBuilder].
// This is exposed for callers who need to implement custom denormalization
// logic beyond what [Transcoder.AppendDenorm] provides. In most cases
// prefer AppendDenorm for automatic fan-out and cross-join handling.
// Returns nil if no denormalizer plan was configured.
func (s *Transcoder) DenormalizerBuilder() *array.RecordBuilder {
	return s.denormBuilder
}

// DenormalizerSchema returns the Arrow schema of the denormalized record.
// Returns nil if no denormalizer plan was configured.
func (s *Transcoder) DenormalizerSchema() *arrow.Schema {
	return s.denormSchema
}

// NewDenormalizerRecordBatch returns the buffered denormalizer builder contents
// as an [arrow.RecordBatch]. The builder is reset and can be reused.
// Returns nil if no denormalizer plan was configured.
func (s *Transcoder) NewDenormalizerRecordBatch() arrow.RecordBatch {
	if s.denormBuilder == nil {
		return nil
	}
	return s.denormBuilder.NewRecordBatch()
}

// Parquet returns the Parquet [schema.Schema] for the message.
func (s *Transcoder) Parquet() *schema.Schema {
	return s.msg.Parquet()
}

// Schema returns the Arrow [arrow.Schema] for the message.
func (s *Transcoder) Schema() *arrow.Schema {
	return s.msg.schema
}

// FieldNames returns the top-level Arrow field names of the message schema.
func (s *Transcoder) FieldNames() []string {
	fieldNames := make([]string, 0, len(s.msg.schema.Fields()))
	for _, f := range s.msg.schema.Fields() {
		fieldNames = append(fieldNames, f.Name)
	}
	return fieldNames
}

// ReadParquet reads the specified columns from Parquet source r and returns an
// Arrow RecordBatch. The returned RecordBatch must be released by the caller.
func (s *Transcoder) ReadParquet(ctx context.Context, r parquet.ReaderAtSeeker, columns []int) (arrow.RecordBatch, error) {
	return s.msg.Read(ctx, r, columns)
}

// WriteParquet writes the current record to Parquet format on w.
func (s *Transcoder) WriteParquet(w io.Writer) error {
	return s.msg.WriteParquet(w)
}

// WriteParquetRecords writes one or many Arrow records to Parquet on w.
func (s *Transcoder) WriteParquetRecords(w io.Writer, records ...arrow.RecordBatch) error {
	return s.msg.WriteParquetRecords(w, records...)
}

// Proto decodes selected rows from an Arrow RecordBatch back into protobuf
// messages. Pass nil for rows to decode all rows.
func (s *Transcoder) Proto(r arrow.RecordBatch, rows []int) []proto.Message {
	return unmarshal(s.msgType, s.msg.root, r, rows)
}

// Release releases the reference on the underlying Arrow record builder.
func (s *Transcoder) Release() {
	s.msg.builder.Release()
}
