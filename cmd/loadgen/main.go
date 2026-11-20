// Package main implements cmd/loadgen, a placeholder CLI stub compiled during Phase 1
// and expanded in Phases 6 and 13 into a high-throughput benchmark and rate-limit verifier.
//
// WHY: Having a valid main package in cmd/loadgen keeps `go vet ./...` and `go test ./...`
// clean across the entire module layout from Phase 1 onward.
//
// Expected output example (Phase 1 stub):
//
//	$ go run ./cmd/loadgen
//	loadgen: phase 1 stub (full batch load generator arrives in phase 6)
package main

import "fmt"

func main() {
	fmt.Println("loadgen: phase 1 stub (full batch load generator arrives in phase 6)")
}
