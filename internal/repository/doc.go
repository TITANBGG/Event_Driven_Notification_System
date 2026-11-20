// Package repository provides PostgreSQL persistence backed by sqlc-generated queries
// and maps database errors (pgx.ErrNoRows, unique violations) into typed domain errors.
//
// WHY: Wrapping sqlc behind a thin domain-typed boundary isolates SQL schema details from API
// handlers and worker loops while enabling atomic Claim and fenced updates.
//
// Expected output example (Phase 2+):
//
//	repo.Claim(ctx, id, skew) -> (*domain.Notification{Status: StatusProcessing, Attempts: 1}, true, nil)
package repository
