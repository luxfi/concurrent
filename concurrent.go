// Package concurrent provides concurrency control utilities inspired by VictoriaMetrics
package concurrent

import (
	"context"
	"runtime"
	"sync"

	"github.com/luxfi/metric"
)

// ConcurrencyLimiter provides token-based concurrency limiting similar to VictoriaMetrics
type ConcurrencyLimiter struct {
	tokens chan struct{}
	name   string
	waitCounter    *metric.OptimizedCounter
	activeGauge    *metric.OptimizedGauge
	totalProcessed *metric.OptimizedCounter
}

// NewConcurrencyLimiter creates a new concurrency limiter with specified max concurrent operations
func NewConcurrencyLimiter(maxConcurrency int, name string, reg *metric.MetricsRegistry) *ConcurrencyLimiter {
	if maxConcurrency <= 0 {
		maxConcurrency = runtime.NumCPU()
	}

	limiter := &ConcurrencyLimiter{
		tokens: make(chan struct{}, maxConcurrency),
		name:   name,
	}

	// Initialize metrics
	if reg != nil {
		limiter.waitCounter = metric.NewOptimizedCounter("concurrent_limiter_wait_total", "Total number of operations that had to wait for concurrency token")
		limiter.activeGauge = metric.NewOptimizedGauge("concurrent_limiter_active_operations", "Number of currently active operations")
		limiter.totalProcessed = metric.NewOptimizedCounter("concurrent_limiter_processed_total", "Total number of operations processed")

		reg.RegisterCounter("concurrent_limiter_wait", limiter.waitCounter)
		reg.RegisterGauge("concurrent_limiter_active", limiter.activeGauge)
		reg.RegisterCounter("concurrent_limiter_processed", limiter.totalProcessed)
	}

	// Fill the channel with tokens initially
	for i := 0; i < maxConcurrency; i++ {
		limiter.tokens <- struct{}{}
	}

	return limiter
}

// Acquire acquires a concurrency token, blocking until one is available
func (cl *ConcurrencyLimiter) Acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-cl.tokens:
		if cl.activeGauge != nil {
			cl.activeGauge.Inc()
		}
		if cl.totalProcessed != nil {
			cl.totalProcessed.Inc()
		}
		return nil
	}
}

// Release releases a concurrency token
func (cl *ConcurrencyLimiter) Release() {
	if cl.activeGauge != nil {
		cl.activeGauge.Dec()
	}
	select {
	case cl.tokens <- struct{}{}:
	default:
		// Should not happen if used correctly
	}
}

// TryAcquire tries to acquire a concurrency token without blocking
func (cl *ConcurrencyLimiter) TryAcquire() bool {
	select {
	case <-cl.tokens:
		if cl.activeGauge != nil {
			cl.activeGauge.Inc()
		}
		if cl.totalProcessed != nil {
			cl.totalProcessed.Inc()
		}
		return true
	default:
		if cl.waitCounter != nil {
			cl.waitCounter.Inc()
		}
		return false
	}
}

// Available returns the number of available concurrency slots
func (cl *ConcurrencyLimiter) Available() int {
	return len(cl.tokens)
}

// Capacity returns the total capacity of the limiter
func (cl *ConcurrencyLimiter) Capacity() int {
	return cap(cl.tokens)
}

// Active returns the number of currently active operations
func (cl *ConcurrencyLimiter) Active() int {
	return cl.Capacity() - cl.Available()
}

// WorkerPool manages a pool of workers for processing tasks
type WorkerPool struct {
	jobs       chan Job
	limiter    *ConcurrencyLimiter
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	workers    int
	jobCounter *metric.OptimizedCounter
	errorCounter *metric.OptimizedCounter
	processingTime *metric.TimingMetric
}

// Job represents a unit of work to be processed by the worker pool
type Job func() error

// NewWorkerPool creates a new worker pool with specified number of workers
func NewWorkerPool(ctx context.Context, workers int, maxQueueSize int, name string, reg *metric.MetricsRegistry) *WorkerPool {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	workerCtx, cancel := context.WithCancel(ctx)
	wp := &WorkerPool{
		jobs:    make(chan Job, maxQueueSize),
		workers: workers,
		ctx:     workerCtx,
		cancel:  cancel,
	}

	wp.limiter = NewConcurrencyLimiter(workers, name+"_workers", reg)

	// Initialize metrics
	if reg != nil {
		wp.jobCounter = metric.NewOptimizedCounter("worker_pool_jobs_processed_total", "Total number of jobs processed by worker pool")
		wp.errorCounter = metric.NewOptimizedCounter("worker_pool_job_errors_total", "Total number of job processing errors")

		histogram := metric.NewOptimizedHistogram("worker_pool_processing_duration_seconds", "Duration of job processing", metric.DefBuckets)
		wp.processingTime = metric.NewTimingMetric(histogram)

		reg.RegisterCounter("worker_pool_jobs_processed", wp.jobCounter)
		reg.RegisterCounter("worker_pool_job_errors", wp.errorCounter)
		reg.RegisterHistogram("worker_pool_processing_duration", histogram)
	}

	// Start workers
	for i := 0; i < workers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}

	return wp
}

// worker runs the worker loop
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for {
		select {
		case <-wp.ctx.Done():
			return
		case job := <-wp.jobs:
			if err := wp.processJob(job); err != nil && wp.errorCounter != nil {
				wp.errorCounter.Inc()
			}
		}
	}
}

// processJob processes a single job with proper error handling
func (wp *WorkerPool) processJob(job Job) error {
	if wp.processingTime != nil {
		wp.processingTime.Reset()
		defer wp.processingTime.Stop()
	}

	if err := wp.limiter.Acquire(wp.ctx); err != nil {
		return err
	}
	defer wp.limiter.Release()

	err := job()
	if wp.jobCounter != nil {
		wp.jobCounter.Inc()
	}

	return err
}

// Submit submits a job to the worker pool
func (wp *WorkerPool) Submit(job Job) error {
	select {
	case <-wp.ctx.Done():
		return wp.ctx.Err()
	case wp.jobs <- job:
		return nil
	}
}

// SubmitAndWait submits a job and waits for it to complete
func (wp *WorkerPool) SubmitAndWait(job Job) error {
	// For immediate execution with concurrency control, just acquire and run
	if err := wp.limiter.Acquire(wp.ctx); err != nil {
		return err
	}
	defer wp.limiter.Release()

	return job()
}

// Stop stops the worker pool and waits for all workers to finish
func (wp *WorkerPool) Stop() {
	wp.cancel()
	close(wp.jobs)
	wp.wg.Wait()
}

// Running returns whether the worker pool is still running
func (wp *WorkerPool) Running() bool {
	select {
	case <-wp.ctx.Done():
		return false
	default:
		return true
	}
}

// QueueSize returns the current number of queued jobs
func (wp *WorkerPool) QueueSize() int {
	return len(wp.jobs)
}

// QueueCapacity returns the maximum number of queued jobs
func (wp *WorkerPool) QueueCapacity() int {
	return cap(wp.jobs)
}