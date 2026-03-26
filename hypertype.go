package bufarrowlib

import (
	"fmt"
	"sync/atomic"

	"buf.build/go/hyperpb"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// HyperType is a shared coordinator for a compiled [hyperpb.MessageType] that
// supports lock-free sharing across multiple [Transcoder] instances (including
// clones running in separate goroutines). It provides online profile-guided
// optimization (PGO) via [hyperpb.Profile]: all Transcoders using the same
// HyperType contribute profiling data, and a recompile atomically upgrades the
// parser for all of them.
//
// A HyperType is safe for concurrent use. The underlying [hyperpb.MessageType]
// is immutable after compilation; [HyperType.Recompile] atomically swaps it.
type HyperType struct {
	msgType atomic.Pointer[hyperpb.MessageType]
	profile atomic.Pointer[hyperpb.Profile]
	seen    atomic.Int64

	// Immutable after construction.
	threshold int64   // auto-recompile after this many messages; 0 = manual only
	rate      float64 // sampling rate for WithRecordProfile (0, 1]
}

// HyperTypeOption configures a [HyperType] during construction.
type HyperTypeOption func(*hyperTypeConfig)

type hyperTypeConfig struct {
	threshold int64
	rate      float64
}

// WithAutoRecompile enables automatic recompilation after threshold messages
// have been profiled. rate is the sampling fraction passed to
// [hyperpb.WithRecordProfile] (e.g. 0.01 for 1%). A threshold of 0 disables
// auto-recompile (the default); use [HyperType.Recompile] manually instead.
func WithAutoRecompile(threshold int64, rate float64) HyperTypeOption {
	return func(cfg *hyperTypeConfig) {
		cfg.threshold = threshold
		cfg.rate = rate
	}
}

// NewHyperType compiles a [hyperpb.MessageType] from md and returns a shared
// coordinator ready for use by one or more [Transcoder] instances. The
// compiled type can later be recompiled with profiling data for improved
// parse performance.
func NewHyperType(md protoreflect.MessageDescriptor, opts ...HyperTypeOption) *HyperType {
	cfg := &hyperTypeConfig{
		rate: 0.01, // default: 1% sampling
	}
	for _, o := range opts {
		o(cfg)
	}

	mt := hyperpb.CompileMessageDescriptor(md)

	ht := &HyperType{
		threshold: cfg.threshold,
		rate:      cfg.rate,
	}
	ht.msgType.Store(mt)
	ht.profile.Store(mt.NewProfile())
	return ht
}

// Type returns the current compiled [hyperpb.MessageType]. The returned
// pointer is safe to use until the next [HyperType.Recompile] call; callers
// should load it once per batch rather than caching it long-term.
func (h *HyperType) Type() *hyperpb.MessageType {
	return h.msgType.Load()
}

// Profile returns the current [hyperpb.Profile] for recording parse
// statistics. Returns nil if profiling has not been initialized.
func (h *HyperType) Profile() *hyperpb.Profile {
	return h.profile.Load()
}

// SampleRate returns the profiling sample rate.
func (h *HyperType) SampleRate() float64 {
	return h.rate
}

// RecordMessage increments the message counter and returns true if the
// auto-recompile threshold has been reached and the caller should trigger
// [HyperType.Recompile]. Returns false if auto-recompile is disabled
// (threshold == 0) or the threshold has not been reached.
func (h *HyperType) RecordMessage() bool {
	if h.threshold <= 0 {
		return false
	}
	n := h.seen.Add(1)
	return n%h.threshold == 0
}

// Recompile recompiles the underlying [hyperpb.MessageType] using the
// collected profiling data. It atomically swaps the old profile for a fresh
// one (preventing double-recompile races), recompiles, and stores the new
// type. All [Transcoder] instances sharing this HyperType will pick up the
// new type on their next [Transcoder.AppendRaw] or [Transcoder.AppendDenormRaw]
// call.
//
// Recompile is safe for concurrent use but is intentionally synchronous:
// the caller blocks until compilation finishes. For non-blocking recompile,
// wrap the call in a goroutine.
//
// Returns an error if no profile data has been collected.
func (h *HyperType) Recompile() error {
	old := h.profile.Load()
	if old == nil {
		return fmt.Errorf("bufarrow: HyperType.Recompile called with no profile data")
	}

	// CAS-swap the profile to prevent concurrent double-recompile.
	// The loser gets a nil swap and returns early.
	if !h.profile.CompareAndSwap(old, nil) {
		return nil // another goroutine is already recompiling
	}

	mt := h.msgType.Load()
	newType := mt.Recompile(old)
	h.msgType.Store(newType)

	// Install a fresh profile for the new type.
	h.profile.Store(newType.NewProfile())
	return nil
}

// RecompileAsync spawns a goroutine to recompile asynchronously. The returned
// channel is closed when recompilation completes (or is skipped because another
// recompile is in progress). Errors are silently discarded; use [Recompile]
// directly if error handling is needed.
func (h *HyperType) RecompileAsync() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Recompile()
	}()
	return done
}
