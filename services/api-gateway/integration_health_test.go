package main

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"
)

func TestIntegrationHealthEndpoint(t *testing.T) {
	if os.Getenv("INTEGRATION_TEST") != "true" {
		t.Skip("set INTEGRATION_TEST=true to run integration smoke test")
	}

	baseURL := os.Getenv("INTEGRATION_BASE_URL")
	if baseURL == "" {
		baseURL = "http://localhost"
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(baseURL + "/health")
	if err != nil {
		t.Fatalf("health request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d", resp.StatusCode)
	}

	var body map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode health body failed: %v", err)
	}

	if body["status"] != "ok" {
		t.Fatalf("unexpected health status: %#v", body["status"])
	}
}
