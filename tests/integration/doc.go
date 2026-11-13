// Package integration holds the tests that talk to real infrastructure.
//
// The tests are skipped unless the environment names a server, so that
// `go test ./...` works on a machine without Docker:
//
//	QUEUELENS_TEST_DATABASE_URL=postgresql://queuelens:queuelens@127.0.0.1:55433/queuelens
//	QUEUELENS_TEST_REDIS_URL=redis://127.0.0.1:56379/0
package integration
