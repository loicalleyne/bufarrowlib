//go:build cgo

package main

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
	bufarrowlib "github.com/loicalleyne/bufarrowlib"
)

// poolMode controls which append/flush methods workers call.
type poolMode int

const (
	poolModeRaw         poolMode = iota // AppendRaw + NewRecordBatch
	poolModeDenorm                      // AppendDenormRaw + NewDenormalizerRecordBatch
	poolModeMerged                      // AppendRawMerged + NewRecordBatch
	poolModeDenormMerged                // AppendDenormRawMerged + NewDenormalizerRecordBatch
)

// poolJob is the message envelope sent through the job channel.
type poolJob struct {
	base   []byte // always set
	custom []byte // non-nil only for merged variants
}

// cPool is the internal state behind a BufarrowPool CGo handle.
// Python submits messages to the bounded job channel; Go goroutines parse
// them with independent Transcoder clones; PoolFlush drains and merges.
type cPool struct {
	workers  int
	mode     poolMode
	jobs     chan poolJob       // bounded; producers block when full (backpressure)
	clones   []*bufarrowlib.Transcoder
	allocs   []*mallocator.Mallocator
	mu       sync.Mutex
	lastErr  string
	pending  atomic.Int64 // approximate count of messages in-flight

	// fields managed by the worker lifecycle (protected by mu)
	results  chan arrow.RecordBatch
	done     []chan struct{}
	wg       sync.WaitGroup
	running  bool
}

// newCPool allocates a cPool and starts the worker goroutines.
// base is the prototype Transcoder (already configured with HyperType,
// denorm plan, etc.); workers clones are created here.
func newCPool(base *bufarrowlib.Transcoder, workers, capacity int, mode poolMode) (*cPool, error) {
	if workers <= 0 {
		workers = 1
	}
	if capacity <= 0 {
		capacity = workers * 64
	}

	clones := make([]*bufarrowlib.Transcoder, workers)
	allocs := make([]*mallocator.Mallocator, workers)
	for w := range workers {
		a := mallocator.NewMallocator()
		c, err := base.Clone(a)
		if err != nil {
			// release previously allocated clones
			for j := range w {
				clones[j].Release()
			}
			return nil, fmt.Errorf("bufarrow pool: clone worker %d: %w", w, err)
		}
		clones[w] = c
		allocs[w] = a
	}

	p := &cPool{
		workers: workers,
		mode:    mode,
		jobs:    make(chan poolJob, capacity),
		clones:  clones,
		allocs:  allocs,
	}
	p.startWorkers()
	return p, nil
}

// startWorkers (re-)starts the worker goroutines and resets the results channel.
// Must be called with mu held or during initial construction.
func (p *cPool) startWorkers() {
	p.results = make(chan arrow.RecordBatch, p.workers)
	p.done = make([]chan struct{}, p.workers)
	for w := range p.workers {
		p.done[w] = make(chan struct{})
	}
	for w := range p.workers {
		w := w
		p.wg.Go(func() { p.workerLoop(p.clones[w], p.done[w]) })
	}
	p.running = true
}

// workerLoop is the goroutine body for each worker.
// It consumes jobs from the shared channel until it receives a drain signal.
// When drained, it flushes its Transcoder and sends the record batch to results.
//
// Dispatch rules:
//   - isDenorm is true  → use Denorm append + NewDenormalizerRecordBatch
//   - j.custom != nil   → use the Merged append variant
func (p *cPool) workerLoop(tc *bufarrowlib.Transcoder, done <-chan struct{}) {
	isDenorm := p.mode == poolModeDenorm || p.mode == poolModeDenormMerged

	processJob := func(j poolJob) {
		var err error
		if j.custom != nil {
			if isDenorm {
				err = tc.AppendDenormRawMerged(j.base, j.custom)
			} else {
				err = tc.AppendRawMerged(j.base, j.custom)
			}
		} else {
			if isDenorm {
				err = tc.AppendDenormRaw(j.base)
			} else {
				err = tc.AppendRaw(j.base)
			}
		}
		if err != nil {
			p.setError(err)
		}
		p.pending.Add(-1)
	}

	for {
		select {
		case j := <-p.jobs:
			processJob(j)

		case <-done:
			// Drain any jobs that arrived before p.running was set to false.
			// After running=false, no new submit() can enqueue jobs, so this
			// loop terminates. Messages enqueued after the race window go to
			// the next flush window.
		drain:
			for {
				select {
				case j := <-p.jobs:
					processJob(j)
				default:
					break drain
				}
			}

			var rb arrow.RecordBatch
			if isDenorm {
				rb = tc.NewDenormalizerRecordBatch()
			} else {
				rb = tc.NewRecordBatch()
			}
			p.results <- rb
			return
		}
	}
}

// submit enqueues a job. Blocks when the channel is full (backpressure).
func (p *cPool) submit(base, custom []byte) error {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return fmt.Errorf("bufarrow pool: submit after flush with no restart")
	}
	p.mu.Unlock()
	p.pending.Add(1)
	p.jobs <- poolJob{base: base, custom: custom}
	return nil
}

// flush drains all workers, collects their record batches, merges them into
// one batch, and restarts workers for the next window.
func (p *cPool) flush() (arrow.RecordBatch, error) {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return nil, fmt.Errorf("bufarrow pool: flush on idle pool")
	}

	// Mark as not running to block new submits, capture channels, then drop
	// the lock so worker goroutines can still acquire it (e.g. via setError).
	p.running = false
	done := p.done
	results := p.results
	p.mu.Unlock()

	// Signal all workers to drain and wait for them to finish.
	for _, d := range done {
		close(d)
	}
	p.wg.Wait()
	close(results)

	// Collect per-worker record batches.
	batches := make([]arrow.RecordBatch, 0, p.workers)
	for rb := range results {
		if rb != nil {
			batches = append(batches, rb)
		}
	}

	// Restart workers for the next window.
	p.mu.Lock()
	p.startWorkers()
	p.mu.Unlock()

	if len(batches) == 0 {
		// All workers returned nil — return a 0-row batch using clone schema.
		var sc *arrow.Schema
		if p.mode == poolModeDenorm || p.mode == poolModeDenormMerged {
			sc = p.clones[0].DenormalizerSchema()
		} else {
			sc = p.clones[0].Schema()
		}
		if sc == nil {
			sc = arrow.NewSchema(nil, nil)
		}
		return array.NewRecord(sc, nil, 0), nil
	}
	defer func() {
		for _, rb := range batches {
			rb.Release()
		}
	}()

	merged, err := mergeRecordBatches(batches, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("bufarrow pool: merge: %w", err)
	}
	return merged, nil
}

// release frees all clones and signals workers to stop if still running.
func (p *cPool) release() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	// Capture and disarm before releasing the lock so workers can acquire it.
	p.running = false
	done := p.done
	results := p.results
	p.mu.Unlock()

	for _, d := range done {
		close(d)
	}
	p.wg.Wait()
	close(results)
	for range results {
	} // drain remaining batches

	for _, tc := range p.clones {
		tc.Release()
	}
}

func (p *cPool) setError(err error) {
	p.mu.Lock()
	p.lastErr = err.Error()
	p.mu.Unlock()
}

func (p *cPool) getError() string {
	p.mu.Lock()
	e := p.lastErr
	p.lastErr = ""
	p.mu.Unlock()
	return e
}

// mergeRecordBatches concatenates column-by-column across all batches.
// Cost is O(columns × workers), not O(rows).
func mergeRecordBatches(batches []arrow.RecordBatch, alloc memory.Allocator) (arrow.RecordBatch, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches to merge")
	}
	if len(batches) == 1 {
		batches[0].Retain()
		return batches[0], nil
	}

	sc := batches[0].Schema()
	ncols := sc.NumFields()
	cols := make([]arrow.Array, ncols)

	for i := range ncols {
		arrays := make([]arrow.Array, len(batches))
		for j, rb := range batches {
			arrays[j] = rb.Column(i)
		}
		merged, err := array.Concatenate(arrays, alloc)
		if err != nil {
			// release already-built columns
			for k := range i {
				cols[k].Release()
			}
			return nil, fmt.Errorf("concatenate column %d: %w", i, err)
		}
		cols[i] = merged
	}

	var totalRows int64
	for _, rb := range batches {
		totalRows += rb.NumRows()
	}

	result := array.NewRecord(sc, cols, totalRows)
	for _, c := range cols {
		c.Release()
	}
	return result, nil
}
