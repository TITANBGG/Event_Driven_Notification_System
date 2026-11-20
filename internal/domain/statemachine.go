package domain

// transitionMap defines every legal status transition. SQL WHERE clauses mirror these exact
// allowed-from sets so cancel-vs-worker races and reconciler recoveries stay atomic.
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
func CanTransition(from, to Status) bool {
	allowed, ok := transitionMap[from]
	if !ok {
		return false
	}
	return allowed[to]
}

// AllowedFrom returns all source statuses permitted to transition directly into target status to.
// The returned slice follows AllStatuses() order so callers and SQL tests observe deterministic output.
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
func IsCancellable(s Status) bool {
	return CanTransition(s, StatusCancelled)
}
