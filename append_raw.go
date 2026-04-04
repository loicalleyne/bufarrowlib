package bufarrowlib

import (
	"fmt"

	"buf.build/go/hyperpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
)

// AppendRaw unmarshals raw protobuf bytes using the [HyperType]'s compiled
// parser and appends the result to the transcoder's Arrow record builder.
//
// This method requires a [HyperType] configured via [WithHyperType]. It uses
// [hyperpb.Shared] for memory reuse and optionally records profiling data for
// online PGO. When the auto-recompile threshold is reached, the parser is
// recompiled inline.
//
// This method is not safe for concurrent use.
func (s *Transcoder) AppendRaw(data []byte) error {
	if s.hyperType == nil {
		return fmt.Errorf("bufarrow: AppendRaw called without HyperType configured; use WithHyperType option")
	}

	mt := s.hyperType.Type()
	msg := s.hyperShared.NewMessage(mt)

	var unmarshalOpts []hyperpb.UnmarshalOption
	if prof := s.hyperType.Profile(); prof != nil && s.hyperType.rate > 0 {
		s.unmarshalScratch[0] = hyperpb.WithRecordProfile(prof, s.hyperType.rate)
		unmarshalOpts = s.unmarshalScratch[:]
	}

	if err := msg.Unmarshal(data, unmarshalOpts...); err != nil {
		s.hyperShared.Free()
		return fmt.Errorf("bufarrow: AppendRaw unmarshal: %w", err)
	}

	s.msg.append(msg.ProtoReflect())
	s.hyperShared.Free()

	if s.hyperType.RecordMessage() {
		s.hyperType.RecompileAsync()
	}
	return nil
}

// AppendDenormRaw unmarshals raw protobuf bytes and appends the denormalized
// result to the transcoder's denormalizer Arrow record builder.
//
// When a [HyperType] is configured (via [WithHyperType]), it uses the compiled
// parser with [hyperpb.Shared] for memory reuse and optional PGO profiling.
// Otherwise, it falls back to [proto.Unmarshal] with a dynamicpb stencil.
//
// A denormalizer plan (via [WithDenormalizerPlan]) must be configured.
//
// This method is not safe for concurrent use.
func (s *Transcoder) AppendDenormRaw(data []byte) error {
	if s.denormPlan == nil {
		return fmt.Errorf("bufarrow: AppendDenormRaw called without denormalizer plan configured")
	}

	if s.hyperType != nil {
		mt := s.hyperType.Type()
		msg := s.hyperShared.NewMessage(mt)

		var unmarshalOpts []hyperpb.UnmarshalOption
		if prof := s.hyperType.Profile(); prof != nil && s.hyperType.rate > 0 {
			s.unmarshalScratch[0] = hyperpb.WithRecordProfile(prof, s.hyperType.rate)
			unmarshalOpts = s.unmarshalScratch[:]
		}

		if err := msg.Unmarshal(data, unmarshalOpts...); err != nil {
			s.hyperShared.Free()
			return fmt.Errorf("bufarrow: AppendDenormRaw unmarshal: %w", err)
		}

		err := s.AppendDenorm(msg)
		s.hyperShared.Free()

		if err != nil {
			return err
		}

		if s.hyperType.RecordMessage() {
			s.hyperType.RecompileAsync()
		}
		return nil
	}

	// Fallback: dynamicpb path (no HyperType)
	v := dynamicpb.NewMessage(s.msgDesc)
	if err := proto.Unmarshal(data, v); err != nil {
		return fmt.Errorf("bufarrow: AppendDenormRaw unmarshal: %w", err)
	}
	return s.AppendDenorm(v)
}

// AppendRawMerged concatenates base and custom serialized protobuf byte slices
// and appends the merged result to the transcoder's Arrow record builder.
//
// This works because [MergeMessageDescriptors] renumbers all custom fields
// above the base message's maximum field number, so the two wire-format byte
// slices have strictly disjoint field tags. Protobuf wire-format concatenation
// (base || custom) produces a valid merged message.
//
// When a [HyperType] is configured (fast path), the concatenated bytes are
// parsed via [hyperpb.Unmarshal]. Otherwise (fallback), [proto.Unmarshal] into
// a clone of [Transcoder.stencilCustom] is used.
//
// Returns an error if no custom message was configured via [WithCustomMessage]
// or [WithCustomMessageFile].
//
// This method is not safe for concurrent use.
func (s *Transcoder) AppendRawMerged(baseBytes, customBytes []byte) error {
	if s.stencilCustom == nil {
		return fmt.Errorf("bufarrow: AppendRawMerged called without custom message configured; use WithCustomMessage or WithCustomMessageFile")
	}

	// Build merged bytes into reusable scratch: baseBytes || remapped(customBytes).
	// rewriteCustomFieldTagsAppend writes directly into mergeScratch, so the entire
	// merge is done with at most one growth of the reusable buffer.
	var mergeErr error
	s.mergeScratch = append(s.mergeScratch[:0], baseBytes...)
	s.mergeScratch, mergeErr = rewriteCustomFieldTagsAppend(s.mergeScratch, customBytes, s.customFieldRemap)
	if mergeErr != nil {
		return fmt.Errorf("bufarrow: AppendRawMerged: %w", mergeErr)
	}
	merged := s.mergeScratch

	if s.hyperType != nil {
		mt := s.hyperType.Type()
		msg := s.hyperShared.NewMessage(mt)

		var unmarshalOpts []hyperpb.UnmarshalOption
		if prof := s.hyperType.Profile(); prof != nil && s.hyperType.rate > 0 {
			s.unmarshalScratch[0] = hyperpb.WithRecordProfile(prof, s.hyperType.rate)
			unmarshalOpts = s.unmarshalScratch[:]
		}

		if err := msg.Unmarshal(merged, unmarshalOpts...); err != nil {
			s.hyperShared.Free()
			return fmt.Errorf("bufarrow: AppendRawMerged unmarshal: %w", err)
		}

		s.msg.append(msg.ProtoReflect())
		s.hyperShared.Free()

		if s.hyperType.RecordMessage() {
			s.hyperType.RecompileAsync()
		}
		return nil
	}

	// Fallback: dynamicpb path
	v := proto.Clone(s.stencilCustom)
	if err := proto.Unmarshal(merged, v); err != nil {
		return fmt.Errorf("bufarrow: AppendRawMerged unmarshal: %w", err)
	}
	s.msg.append(v.ProtoReflect())
	return nil
}

// AppendDenormRawMerged concatenates base and custom serialized protobuf byte
// slices and appends the denormalized result to the transcoder's denormalizer
// Arrow record builder.
//
// This follows the same byte-concatenation strategy as [AppendRawMerged] but
// routes the result through the denormalization engine.
//
// Requires both a custom message (via [WithCustomMessage] or
// [WithCustomMessageFile]) and a denormalizer plan (via
// [WithDenormalizerPlan]).
//
// This method is not safe for concurrent use.
func (s *Transcoder) AppendDenormRawMerged(baseBytes, customBytes []byte) error {
	if s.stencilCustom == nil {
		return fmt.Errorf("bufarrow: AppendDenormRawMerged called without custom message configured; use WithCustomMessage or WithCustomMessageFile")
	}
	if s.denormPlan == nil {
		return fmt.Errorf("bufarrow: AppendDenormRawMerged called without denormalizer plan configured")
	}

	// Build merged bytes into reusable scratch: baseBytes || remapped(customBytes).
	// rewriteCustomFieldTagsAppend writes directly into mergeScratch, so the entire
	// merge is done with at most one growth of the reusable buffer.
	var mergeErr error
	s.mergeScratch = append(s.mergeScratch[:0], baseBytes...)
	s.mergeScratch, mergeErr = rewriteCustomFieldTagsAppend(s.mergeScratch, customBytes, s.customFieldRemap)
	if mergeErr != nil {
		return fmt.Errorf("bufarrow: AppendDenormRawMerged: %w", mergeErr)
	}
	merged := s.mergeScratch

	if s.hyperType != nil {
		mt := s.hyperType.Type()
		msg := s.hyperShared.NewMessage(mt)

		var unmarshalOpts []hyperpb.UnmarshalOption
		if prof := s.hyperType.Profile(); prof != nil && s.hyperType.rate > 0 {
			s.unmarshalScratch[0] = hyperpb.WithRecordProfile(prof, s.hyperType.rate)
			unmarshalOpts = s.unmarshalScratch[:]
		}

		if err := msg.Unmarshal(merged, unmarshalOpts...); err != nil {
			s.hyperShared.Free()
			return fmt.Errorf("bufarrow: AppendDenormRawMerged unmarshal: %w", err)
		}

		err := s.AppendDenorm(msg)
		s.hyperShared.Free()

		if err != nil {
			return err
		}

		if s.hyperType.RecordMessage() {
			s.hyperType.RecompileAsync()
		}
		return nil
	}

	// Fallback: dynamicpb path
	v := proto.Clone(s.stencilCustom)
	if err := proto.Unmarshal(merged, v); err != nil {
		return fmt.Errorf("bufarrow: AppendDenormRawMerged unmarshal: %w", err)
	}
	return s.AppendDenorm(v)
}

// rewriteCustomFieldTags rewrites top-level field numbers in encoded protobuf
// bytes according to remap (originalNumber → mergedNumber). This translates
// customBytes — encoded with the custom descriptor's original field numbers —
// into the renumbered field positions assigned by [MergeMessageDescriptors]
// before concatenation with baseBytes.
//
// Only top-level field tags are rewritten; nested message bytes are passed
// through unchanged since their field numbers live in a separate namespace.
// Proto3 only — groups (wire types 3/4) are not used by any supported schema.
func rewriteCustomFieldTags(data []byte, remap map[int32]int32) ([]byte, error) {
	return rewriteCustomFieldTagsAppend(nil, data, remap)
}

// rewriteCustomFieldTagsAppend appends the remapped bytes of data to dst and
// returns the extended slice. Passing a pre-allocated dst (e.g. a reusable
// scratch buffer) avoids the otherwise-unavoidable allocation when remap is
// non-empty. When remap is empty the original bytes are appended verbatim.
func rewriteCustomFieldTagsAppend(dst, data []byte, remap map[int32]int32) ([]byte, error) {
	if len(remap) == 0 {
		return append(dst, data...), nil
	}
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, fmt.Errorf("malformed tag: %w", protowire.ParseError(n))
		}
		data = data[n:]

		// Measure the raw value bytes without copying.
		m := protowire.ConsumeFieldValue(num, typ, data)
		if m < 0 {
			return nil, fmt.Errorf("malformed value for field %d: %w", num, protowire.ParseError(m))
		}
		rawVal := data[:m]
		data = data[m:]

		// Remap field number if present in the map.
		outNum := num
		if newNum, ok := remap[int32(num)]; ok {
			outNum = protowire.Number(newNum)
		}

		dst = protowire.AppendTag(dst, outNum, typ)
		dst = append(dst, rawVal...)
	}
	return dst, nil
}
