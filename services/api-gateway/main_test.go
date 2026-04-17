package main

import "testing"

func TestParsePositiveInt(t *testing.T) {
	if got := parsePositiveInt("10", 1); got != 10 {
		t.Fatalf("parsePositiveInt valid failed: got %d", got)
	}
	if got := parsePositiveInt("0", 7); got != 7 {
		t.Fatalf("parsePositiveInt zero fallback failed: got %d", got)
	}
	if got := parsePositiveInt("", 3); got != 3 {
		t.Fatalf("parsePositiveInt empty fallback failed: got %d", got)
	}
}

func TestHashAPIKeyDeterministic(t *testing.T) {
	h1 := hashAPIKey("salt", "key")
	h2 := hashAPIKey("salt", "key")
	if h1 != h2 {
		t.Fatal("hashAPIKey should be deterministic")
	}

	h3 := hashAPIKey("salt2", "key")
	if h1 == h3 {
		t.Fatal("hashAPIKey should change with salt")
	}
}
