// Package migrations embeds the SQL migrations that the collector applies at
// startup.
//
// They live in the repository root because they are part of the deployment
// contract, not part of one Go package: a DBA can read them without reading
// Go.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
