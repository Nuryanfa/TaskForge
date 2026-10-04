// Package migrations exposes TaskForge's versioned SQL migrations as an
// immutable filesystem embedded in every migration binary.
package migrations

import "embed"

// Files contains all versioned SQL migrations in this directory.
//
//go:embed *.sql
var Files embed.FS
