package main

import (
	"testing"
)

func TestLegacyFloatToMinorUnits(t *testing.T) {
	if got := legacyFloatToMinorUnits(12.4); got != 12 {
		t.Fatalf("12.4 converted to %d, want 12", got)
	}
	if got := legacyFloatToMinorUnits(12.6); got != 13 {
		t.Fatalf("12.6 converted to %d, want 13", got)
	}
}

func TestTransientErrorClassification(t *testing.T) {
	if !isTransientError(errorString("Couchbase timeout")) {
		t.Fatal("timeout should be transient")
	}
	if isTransientError(errorString("INSUFFICIENT_FUNDS")) {
		t.Fatal("business error should not be transient")
	}
}

type errorString string

func (e errorString) Error() string { return string(e) }
