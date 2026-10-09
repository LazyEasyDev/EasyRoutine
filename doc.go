// Package EasyRoutine runs panic-safe local tasks and persistent distributed
// supervisors with queryable current status and lifecycle history.
// Call Initialize with an application context before starting managed work.
// Supply a SQLConfig to enable unique supervisors and SQL queries, or nil for local work.
// Close requests package shutdown; Wait waits for work and cleanup to finish.
package EasyRoutine
