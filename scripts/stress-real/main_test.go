package main

import (
	"errors"
	"testing"
	"time"
)

func TestSampleDrainReturnsTerminalSampleWithoutExtraFetch(t *testing.T) {
	t.Parallel()

	calls := 0
	fetch := func() (*listTasksResult, error) {
		calls++
		if calls > 1 {
			return nil, errors.New("transient request failure")
		}
		return &listTasksResult{Total: 42}, nil
	}

	settled, rates, err := sampleDrain(time.Now().Add(time.Second), time.Millisecond, fetch)
	if err != nil {
		t.Fatalf("sampleDrain returned an error: %v", err)
	}
	if settled != 42 {
		t.Fatalf("settled = %d; want 42", settled)
	}
	if len(rates) != 0 {
		t.Fatalf("rates = %v; want no rates from one sample", rates)
	}
	if calls != 1 {
		t.Fatalf("fetch called %d times; want 1", calls)
	}
}

func TestSampleDrainErrorsWhenEveryFetchFails(t *testing.T) {
	t.Parallel()

	_, _, err := sampleDrain(time.Now().Add(10*time.Millisecond), time.Millisecond, func() (*listTasksResult, error) {
		return nil, errors.New("service unavailable")
	})
	if err == nil {
		t.Fatal("sampleDrain returned nil error; want an error")
	}
}
