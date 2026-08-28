package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// Task represents input work.
type Task struct {
	ID   int
	Data string
}

// Result represents computed output.
type Result struct {
	TaskID int
	Value  int
	Err    error
}

// Processor is an interface for doing work.
type Processor interface {
	Process(ctx context.Context, t Task) (int, error)
}

// SquareProcessor parses an integer and returns its square.
type SquareProcessor struct{}

func (p SquareProcessor) Process(ctx context.Context, t Task) (int, error) {
	// Check cancellation early.
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	n, err := strconv.Atoi(t.Data)
	if err != nil {
		return 0, fmt.Errorf("task %d: invalid integer %q: %w", t.ID, t.Data, err)
	}
	if n < 0 {
		return 0, errors.New("negative numbers are not allowed")
	}

	// Simulate a bit of work.
	time.Sleep(50 * time.Millisecond)

	// Check cancellation again mid-work.
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	return n * n, nil
}

func worker(ctx context.Context, proc Processor, in <-chan Task, out chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case t, ok := <-in:
			if !ok {
				return
			}
			val, err := proc.Process(ctx, t)
			select {
			case <-ctx.Done():
				return
			case out <- Result{TaskID: t.ID, Value: val, Err: err}:
			}
		}
	}
}

func main() {
	// Cancel everything after a short timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 650*time.Millisecond)
	defer cancel()

	proc := SquareProcessor{}

	// Example inputs.
	tasks := []string{"3", "5", "12", "abc", "-2", "7", "9", "4"}

	taskCh := make(chan Task)
	resultCh := make(chan Result)

	// Start a small pool of workers.
	const workers = 3
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go worker(ctx, proc, taskCh, resultCh, &wg)
	}

	// Feed tasks asynchronously.
	go func() {
		defer close(taskCh)
		for i, s := range tasks {
			select {
			case <-ctx.Done():
				return
			case taskCh <- Task{ID: i + 1, Data: s}:
			}
		}
	}()

	// Close results when workers finish.
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect results.
	fmt.Println("Starting concurrent processing...")
	var okCount, errCount int
	for r := range resultCh {
		if r.Err != nil {
			errCount++
			fmt.Printf("Task %d error: %v\n", r.TaskID, r.Err)
			continue
		}
		okCount++
		fmt.Printf("Task %d result: %d\n", r.TaskID, r.Value)
	}

	// Final summary (note: may stop early due to timeout).
	fmt.Printf("\nSummary: ok=%d errors=%d\n", okCount, errCount)
	if ctx.Err() != nil {
		fmt.Printf("Stopped because: %v\n", ctx.Err())
	}
}
