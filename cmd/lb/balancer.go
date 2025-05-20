package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/sifes/architecture-practice-4-template/httptools"
	"github.com/sifes/architecture-practice-4-template/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
	healthyServers = make([]bool, len(serversPool))
	mut             sync.Mutex
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func doHash(input string) uint32 {
	hasher := sha256.New()
	hasher.Write([]byte(input))
	return binary.BigEndian.Uint32(hasher.Sum(nil))
}

func updateHealthyServers() {
	for idx, address := range serversPool {
		srv := address
		index := idx
		go func() {
			for range time.Tick(10 * time.Second) {
				mut.Lock()
				healthyServers[index] = health(srv)
				mut.Unlock()
			}
		}()
	}
}

func chooseServer(uriPath string) string {
	mut.Lock()
	defer mut.Unlock()

	idx := doHash(uriPath) % uint32(len(serversPool))

	startIdx := idx
	for !healthyServers[idx] {
		idx = (idx + 1) % uint32(len(serversPool))
		if idx == startIdx {
			return ""
		}
	}

	return serversPool[idx]
}

func main() {
	flag.Parse()

	updateHealthyServers()

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		server := chooseServer(r.URL.Path)
		if server == "" {
			http.Error(rw, "No healthy servers available", http.StatusServiceUnavailable)
			return

		}

		err := forward(server, rw, r)
		if err != nil {
			log.Printf("Failed to forward request: %s", err)
		}
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}