package main

import (
	"reflect"
	"testing"
)

func TestScheme(t *testing.T) {
	*https = true
	if got := scheme(); got != "https" {
		t.Errorf("scheme() with https=true = %q; want \"https\"", got)
	}

	*https = false
	if got := scheme(); got != "http" {
		t.Errorf("scheme() with https=false = %q; want \"http\"", got)
	}
}

func TestDoHashDeterministic(t *testing.T) {
	input := "/some/path/for/hashing"
	h1 := doHash(input)
	h2 := doHash(input)
	if h1 != h2 {
		t.Errorf("doHash not deterministic: first %d, second %d", h1, h2)
	}

	if reflect.TypeOf(h1).Kind() != reflect.Uint32 {
		t.Errorf("doHash returned type %v; want uint32", reflect.TypeOf(h1).Kind())
	}
}

func TestChooseDeterministic(t *testing.T) {
	for i := range healthyServers {
		healthyServers[i] = true
	}

	path := "/repeatable-path"
	first := chooseServer(path)
	if first == "" {
		t.Fatal("chooseServer returned empty for healthy pool")
	}

	for i := 0; i < 10; i++ {
		if got := chooseServer(path); got != first {
			t.Errorf("iteration %d: chooseServer = %q; want %q", i, got, first)
		}
	}
}

func TestSkipUnhealthy(t *testing.T) {
	n := len(serversPool)
	if n < 2 {
		t.Skip("need at least 2 servers for skip-unhealthy test")
	}

	for i := range healthyServers {
		healthyServers[i] = true
	}

	path := "/skip-test"
	primary := int(doHash(path) % uint32(n))
	healthyServers[primary] = false

	expected := -1
	for offset := 1; offset < n; offset++ {
		idx := (primary + offset) % n
		if healthyServers[idx] {
			expected = idx
			break
		}
	}
	if expected < 0 {
		t.Skip("only one server healthy; cannot test skip logic")
	}

	if got := chooseServer(path); got != serversPool[expected] {
		t.Errorf(
			"chooseServer skipped unhealthy[%d] and returned %q; want %q (serversPool[%d])",
			primary, got, serversPool[expected], expected,
		)
	}
}

func TestNoHealthy(t *testing.T) {
	for i := range healthyServers {
		healthyServers[i] = false
	}

	if got := chooseServer("/anything"); got != "" {
		t.Errorf("chooseServer with no healthy returned %q; want \"\"", got)
	}
}
