// Package EasyRoutine runs panic-safe local tasks and persistent distributed
// supervisors with queryable current status and lifecycle history.
// Call InitLease with a LeaseProvider, or InitSQLLease with a database and SQL
// dialect, before using StartUniqueSupervisor.
package EasyRoutine
