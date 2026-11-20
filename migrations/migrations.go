// Package migrations embeds the versioned PostgreSQL schema migration files
// for programmatic execution in integration tests and tooling.
package migrations

import "embed"

// FS holds all embedded *.sql migration files.
//
//go:embed *.sql
var FS embed.FS
