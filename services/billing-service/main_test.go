package main

import "testing"

func TestCalculateCost(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		want     int
	}{
		{name: "data endpoint", endpoint: "/api/v1/data", want: 1},
		{name: "compute endpoint", endpoint: "/api/v1/compute", want: 3},
		{name: "default endpoint", endpoint: "/api/v1/other", want: 2},
		{name: "trimmed endpoint", endpoint: "  /api/v1/data  ", want: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := calculateCost(tc.endpoint)
			if got != tc.want {
				t.Fatalf("calculateCost(%q)=%d want=%d", tc.endpoint, got, tc.want)
			}
		})
	}
}
