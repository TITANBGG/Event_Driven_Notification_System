// Package repository provides PostgreSQL persistence over sqlc-generated queries and maps
// database errors (pgx.ErrNoRows, unique violations) into typed domain sentinel errors.
package repository
