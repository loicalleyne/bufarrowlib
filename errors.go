package bufarrowlib

import (
	"errors"
	"fmt"
)

// Typed constructor error sentinels for errors.Is checks.
var (
	ErrMutuallyExclusiveCustomMessageOptions = errors.New("bufarrow: WithCustomMessage and WithCustomMessageFile are mutually exclusive")
	ErrHyperTypeDescriptorMismatch          = errors.New("bufarrow: WithHyperType descriptor does not match active message descriptor")
	ErrInvalidHyperTypeMismatchPolicy       = errors.New("bufarrow: invalid hyper type mismatch policy")
)

// HyperTypeDescriptorMismatchError reports a descriptor fingerprint mismatch
// between the active message descriptor and the provided HyperType.
type HyperTypeDescriptorMismatchError struct {
	ActiveMessage string
	HyperMessage  string
	ActiveHash    string
	HyperHash     string
}

func (e *HyperTypeDescriptorMismatchError) Error() string {
	return fmt.Sprintf(
		"bufarrow: WithHyperType descriptor mismatch: active=%q (hash=%s), hyper=%q (hash=%s)",
		e.ActiveMessage,
		e.ActiveHash,
		e.HyperMessage,
		e.HyperHash,
	)
}

func (e *HyperTypeDescriptorMismatchError) Unwrap() error {
	return ErrHyperTypeDescriptorMismatch
}
