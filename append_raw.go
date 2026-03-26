package bufarrowlib

import (
	"fmt"

	"buf.build/go/hyperpb"
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
		unmarshalOpts = append(unmarshalOpts, hyperpb.WithRecordProfile(prof, s.hyperType.rate))
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

// AppendDenormRaw unmarshals raw protobuf bytes using the [HyperType]'s
// compiled parser and appends the denormalized result to the transcoder's
// denormalizer Arrow record builder.
//
// This method requires both a [HyperType] (via [WithHyperType]) and a
// denormalizer plan (via [WithDenormalizerPlan]). It uses [hyperpb.Shared]
// for memory reuse and optionally records profiling data for online PGO.
//
// This method is not safe for concurrent use.
func (s *Transcoder) AppendDenormRaw(data []byte) error {
	if s.hyperType == nil {
		return fmt.Errorf("bufarrow: AppendDenormRaw called without HyperType configured; use WithHyperType option")
	}
	if s.denormPlan == nil {
		return fmt.Errorf("bufarrow: AppendDenormRaw called without denormalizer plan configured")
	}

	mt := s.hyperType.Type()
	msg := s.hyperShared.NewMessage(mt)

	var unmarshalOpts []hyperpb.UnmarshalOption
	if prof := s.hyperType.Profile(); prof != nil && s.hyperType.rate > 0 {
		unmarshalOpts = append(unmarshalOpts, hyperpb.WithRecordProfile(prof, s.hyperType.rate))
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
