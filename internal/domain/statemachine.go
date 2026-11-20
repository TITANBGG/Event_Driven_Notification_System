package domain

// transitionMap is the single authoritative state machine matching CLAUDE.md Section 3.
//
// WHY: Centralizing allowed transitions here and mirroring the exact same allowed_from sets in
// PostgreSQL WHERE clauses guarantees cancel-vs-worker races and reconciler recoveries stay consistent.
var transitionMap = map[Status]map[Status]bool{
	StatusScheduled: {
		StatusProcessing: true,
		StatusCancelled:  true,
	},
	StatusPending: {
		StatusProcessing: true,
		StatusCancelled:  true,
	},
	StatusRetrying: {
		StatusProcessing: true,
		StatusCancelled:  true,
	},
	StatusProcessing: {
		StatusSent:     true,
		StatusRetrying: true,
		StatusFailed:   true,
		StatusPending:  true, // reconciler only after VISIBILITY_TIMEOUT
	},
	StatusFailed: {
		StatusPending: true, // manual retry endpoint only
	},
	StatusSent:      {},
	StatusCancelled: {},
}

// CanTransition reports whether transitioning from -> to is permitted by the domain state machine.
//
// Expected output example:
//
//	CanTransition(StatusPending, StatusProcessing) -> true
//	CanTransition(StatusSent, StatusCancelled)   -> false
func CanTransition(from, to Status) bool {
	allowed, ok := transitionMap[from]
	if !ok {
		return false
	}
	return allowed[to]
}

// AllowedFrom returns all source statuses permitted to transition directly into target status to.
// The returned slice follows AllStatuses() order so callers and SQL tests observe deterministic output.
//
// Expected output example:
//
//	AllowedFrom(StatusCancelled) -> []Status{StatusScheduled, StatusPending, StatusRetrying}
func AllowedFrom(to Status) []Status {
	var out []Status
	for _, from := range AllStatuses() {
		if CanTransition(from, to) {
			out = append(out, from)
		}
	}
	return out
}

// IsCancellable reports whether a notification in status s can be cancelled via the API (FR-1.5).
//
// Expected output example:
//
//	IsCancellable(StatusRetrying)   -> true
//	IsCancellable(StatusProcessing) -> false
func IsCancellable(s Status) bool {
	return CanTransition(s, StatusCancelled)
}
