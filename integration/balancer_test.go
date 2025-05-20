package integration

import (
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	// First, verify all servers are healthy
	for i := 0; i < len(serversPool); i++ {
		if !health(serversPool[i]) {
			t.Fatalf("Server %s is not healthy at test start", serversPool[i])
		}
	}

	endpoints := []string{
		"/api/v1/some-data",
		"/api/v1/some-data-check1",
		"/api/v1/some-data-check2",
		"/api/v1/some-data-check3",
	}

	serversUsed := make(map[string]bool)
	requestsCount := 30

	for i := 0; i < requestsCount; i++ {
		for _, endpoint := range endpoints {
			url := fmt.Sprintf("%s%s", baseAddress, endpoint)
			resp, err := client.Get(url)
			if err != nil {
				t.Errorf("Request to %s failed: %v", url, err)
				continue
			}

			if resp.StatusCode != http.StatusOK {
				t.Errorf("Request to %s returned status %d, expected 200", url, resp.StatusCode)
			}

			serverFrom := resp.Header.Get("lb-from")
			if serverFrom == "" {
				t.Error("No lb-from header present in response")
			} else {
				t.Logf("[%s] response from [%s]", endpoint, serverFrom)
				serversUsed[serverFrom] = true
			}

			resp.Body.Close()
		}
	}

	t.Logf("Servers used: %d", len(serversUsed))
	for server := range serversUsed {
		t.Logf("Server used: %s", server)
	}

	expectedServers := []string{"server1:8080", "server2:8080", "server3:8080"}
	for _, server := range expectedServers {
		if !serversUsed[server] {
			t.Errorf("Server %s was not used by balancer", server)
		}
	}
}

func TestConsistentHashingOfBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	endpointServersMap := make(map[string]string)
	endpoints := []string{
		"/api/v1/some-data",
		"/api/v1/some-data-check1",
		"/api/v1/some-data-check2",
		"/api/v1/some-data-check3",
	}

	for _, endpoint := range endpoints {
		url := fmt.Sprintf("%s%s", baseAddress, endpoint)
		resp, err := client.Get(url)
		if err != nil {
			t.Fatalf("Failed to connect to balancer: %v", err)
		}

		serverFrom := resp.Header.Get("lb-from")
		if serverFrom == "" {
			t.Error("No lb-from header present in response")
		} else {
			endpointServersMap[endpoint] = serverFrom
			t.Logf("Endpoint %s mapped to server %s", endpoint, serverFrom)
		}

		resp.Body.Close()
	}

	for _, endpoint := range endpoints {
		url := fmt.Sprintf("%s%s", baseAddress, endpoint)
		resp, err := client.Get(url)
		if err != nil {
			t.Fatalf("Failed to connect to balancer: %v", err)
		}

		serverFrom := resp.Header.Get("lb-from")
		expectedServer := endpointServersMap[endpoint]

		if serverFrom != expectedServer {
			t.Errorf("Expected endpoint %s to be consistently routed to %s, but got %s",
				endpoint, expectedServer, serverFrom)
		}

		resp.Body.Close()
	}
}

func TestURLStickyRouting(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	const (
		testURL         = "/api/v1/some-data?key=test123"
		parallelRequests = 20                            
	)

	results := make(chan string, parallelRequests)
	var wg sync.WaitGroup

	wg.Add(parallelRequests)
	for i := 0; i < parallelRequests; i++ {
		go func(id int) {
			defer wg.Done()

			resp, err := client.Get(baseAddress + testURL)
			if err != nil {
				t.Errorf("Request %d failed: %v", id, err)
				return
			}
			defer resp.Body.Close()

			server := resp.Header.Get("lb-from")
			if server == "" {
				t.Errorf("Request %d: no lb-from header", id)
				return
			}
			results <- server
		}(i)
	}

	wg.Wait()
	close(results)

	uniqueServers := make(map[string]struct{})
	for server := range results {
		uniqueServers[server] = struct{}{}
	}

	if len(uniqueServers) != 1 {
		t.Errorf("Expected all requests to go to same server, got %d different servers: %v", 
			len(uniqueServers), uniqueServers)
	} else {
		t.Logf("All %d requests for '%s' went to server: %s", 
			parallelRequests, testURL, getFirstKey(uniqueServers))
	}
}

func getFirstKey(m map[string]struct{}) string {
	for k := range m {
		return k
	}
	return ""
}

func BenchmarkBalancer(b *testing.B) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		b.Skip("Integration benchmark is not enabled")
	}

	urls := []string{
		fmt.Sprintf("%s/api/v1/some-data", baseAddress),
		fmt.Sprintf("%s/api/v1/some-data-check1", baseAddress),
		fmt.Sprintf("%s/api/v1/some-data-check2", baseAddress),
		fmt.Sprintf("%s/api/v1/some-data-check3", baseAddress),
	}

	serverCounts := make(map[string]int)
	responseTimeTotal := time.Duration(0)
	var responseTimeMu sync.Mutex
	var serverCountsMu sync.Mutex

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		localIndex := 0
		for pb.Next() {
			url := urls[localIndex%len(urls)]
			localIndex++

			start := time.Now()
			resp, err := client.Get(url)
			if err != nil {
				b.Logf("Request error: %v", err)
				continue
			}

			elapsed := time.Since(start)
			responseTimeMu.Lock()
			responseTimeTotal += elapsed
			responseTimeMu.Unlock()

			server := resp.Header.Get("lb-from")
			if server != "" {
				serverCountsMu.Lock()
				serverCounts[server]++
				serverCountsMu.Unlock()
			}

			resp.Body.Close()
		}
	})

	totalRequests := 0
	for _, count := range serverCounts {
		totalRequests += count
	}

	b.Logf("Total requests processed: %d", totalRequests)
	b.Logf("Average response time: %v", responseTimeTotal/time.Duration(totalRequests))

	var servers []string
	for server := range serverCounts {
		servers = append(servers, server)
	}
	sort.Strings(servers)

	for _, server := range servers {
		percentage := float64(serverCounts[server]) * 100 / float64(totalRequests)
		b.Logf("Server %s handled %d requests (%.2f%%)",
			server, serverCounts[server], percentage)
	}
}

// health checks if a server is healthy
func health(server string) bool {
	resp, err := client.Get(fmt.Sprintf("http://%s/health", server))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

var serversPool = []string{
	"server1:8080",
	"server2:8080",
	"server3:8080",
}
