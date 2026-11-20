// Package domain defines core notification entities, enums, state machine transitions,
// multi-channel validation, SMS segment calculation, typed domain errors, and canonical request hashing.
//
// WHY: Isolating domain invariants from HTTP and SQL packages keeps business logic unit-testable
// without Docker or external dependencies and guarantees one authoritative state-machine source.
//
// Expected output example (Phase 2+):
//
//	CanTransition(StatusPending, StatusProcessing) -> true
//	SMSInfo("Şifreniz: 1234") -> {Encoding: "UCS-2", Units: 14, Segments: 1}
package domain
