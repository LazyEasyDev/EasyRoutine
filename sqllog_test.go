package EasyRoutine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

type logTimeoutExecer func(context.Context, string, ...any) (sql.Result, error)

func TestSupervisorQueriesPropagateSQLErrors(t *testing.T) {
	statusColumns := []string{"name", "owner", "status", "success_count", "failure_count", "log", "expires_at", "updated_at"}
	statusRow := []driver.Value{"reports", "worker", "running", int64(1), int64(0), "", int64(200), int64(100)}
	logColumns := []string{"id", "name", "owner", "action", "status", "log", "created_at"}
	logRow := []driver.Value{"log-1", "reports", "worker", "acquire", "running", "", int64(100)}
	discovery := sqlQueryStep{columns: []string{"routine_id"}, rows: [][]driver.Value{{routineID("reports")}}}
	for _, target := range []struct {
		name      string
		statuses  bool
		names     []string
		prefix    []sqlQueryStep
		columns   []string
		validRow  []driver.Value
		queryText string
		scanText  string
		nextText  string
	}{
		{name: "filtered statuses", statuses: true, names: []string{"reports"}, columns: statusColumns, validRow: statusRow,
			queryText: "query supervisor statuses:", scanText: "scan supervisor status:", nextText: "iterate supervisor statuses:"},
		{name: "all statuses", statuses: true, columns: statusColumns, validRow: statusRow,
			queryText: "query supervisor statuses:", scanText: "scan supervisor status:", nextText: "iterate supervisor statuses:"},
		{name: "filtered logs", names: []string{"reports"}, columns: logColumns, validRow: logRow,
			queryText: "query supervisor logs:", scanText: "scan supervisor log:", nextText: "iterate supervisor logs:"},
		{name: "log discovery", columns: discovery.columns, validRow: discovery.rows[0],
			queryText: "query supervisor log routine IDs:", scanText: "scan supervisor log routine ID:", nextText: "iterate supervisor log routine IDs:"},
		{name: "all logs after discovery", prefix: []sqlQueryStep{discovery}, columns: logColumns, validRow: logRow,
			queryText: "query supervisor logs:", scanText: "scan supervisor log:", nextText: "iterate supervisor logs:"},
	} {
		for _, failure := range []string{"query", "scan", "iteration"} {
			t.Run(target.name+"/"+failure, func(t *testing.T) {
				resetDefaultCoordinator(t)
				cause := errors.New("injected SQL failure")
				step := sqlQueryStep{columns: target.columns}
				var wantPrefix string
				switch failure {
				case "query":
					step.queryErr = cause
					wantPrefix = target.queryText
				case "scan":
					invalidRow := append([]driver.Value(nil), target.validRow...)
					invalidRow[0] = nil
					step.rows = [][]driver.Value{target.validRow, invalidRow}
					wantPrefix = target.scanText
				case "iteration":
					step.rows = [][]driver.Value{target.validRow}
					step.nextErr = cause
					wantPrefix = target.nextText
				}
				steps := append([]sqlQueryStep(nil), target.prefix...)
				executor := &scriptedSQLExecer{querySteps: append(steps, step)}
				db := sql.OpenDB(scriptedSQLConnector{executor: executor})
				t.Cleanup(func() { _ = db.Close() })
				backend, err := newSQLLease(db, SQLPostgreSQL)
				if err != nil {
					t.Fatal(err)
				}
				if err := initLease(backend); err != nil {
					t.Fatal(err)
				}
				if target.statuses {
					var statuses SupervisorStatuses
					statuses, err = GetSupervisorStatuses(context.Background(), target.names...)
					if statuses != nil {
						t.Fatalf("returned partial statuses on failure: %#v", statuses)
					}
				} else {
					var logs SupervisorHistory
					logs, err = GetSupervisorLogs(context.Background(), target.names...)
					if logs != nil {
						t.Fatalf("returned partial logs on failure: %#v", logs)
					}
				}
				if err == nil || !strings.HasPrefix(err.Error(), wantPrefix) {
					t.Fatalf("error = %v, want prefix %q", err, wantPrefix)
				}
				if failure != "scan" && !errors.Is(err, cause) {
					t.Fatalf("error did not preserve injected cause: %v", err)
				}
				if failure == "scan" && !strings.Contains(err.Error(), "converting NULL to string is unsupported") {
					t.Fatalf("scan error did not preserve conversion failure: %v", err)
				}
				if len(executor.queryCalls) != len(executor.querySteps) {
					t.Fatalf("query calls = %d, want %d", len(executor.queryCalls), len(executor.querySteps))
				}
				if inUse := db.Stats().InUse; inUse != 0 {
					t.Fatalf("connections still in use after failure = %d", inUse)
				}
			})
		}
	}
}

func (execute logTimeoutExecer) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return execute(ctx, query, args...)
}

func TestSQLLogSharedTimeout(t *testing.T) {
	for _, action := range []LeaseAction{LeaseAcquire, LeaseRelease} {
		for _, scenario := range []struct {
			name           string
			insertDuration time.Duration
			parentTimeout  time.Duration
			cancelParent   bool
			wantDuration   time.Duration
			wantPrune      bool
		}{
			{name: "blocked insert", wantDuration: 10 * time.Second},
			{name: "prune shares insertion budget", insertDuration: 6 * time.Second, wantDuration: 10 * time.Second, wantPrune: true},
			{name: "earlier parent deadline", insertDuration: time.Second, parentTimeout: 3 * time.Second, wantDuration: 3 * time.Second, wantPrune: true},
			{name: "parent cancellation", cancelParent: true},
		} {
			t.Run(string(action)+"/"+scenario.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					if scenario.parentTimeout > 0 {
						var deadlineCancel context.CancelFunc
						ctx, deadlineCancel = context.WithTimeout(ctx, scenario.parentTimeout)
						defer deadlineCancel()
					}
					var backend *sqlLease
					var insertContext context.Context
					pruneCalled := false
					executor := logTimeoutExecer(func(callContext context.Context, query string, _ ...any) (sql.Result, error) {
						switch query {
						case backend.logs.insert:
							insertContext = callContext
							if scenario.cancelParent {
								cancel()
							}
							if scenario.insertDuration > 0 {
								time.Sleep(scenario.insertDuration)
								return driver.RowsAffected(1), nil
							}
							<-callContext.Done()
							return nil, callContext.Err()
						case backend.logs.prune:
							pruneCalled = true
							if callContext != insertContext {
								t.Fatal("insertion and pruning used different contexts")
							}
							<-callContext.Done()
							return nil, callContext.Err()
						default:
							return driver.RowsAffected(1), nil
						}
					})
					var err error
					backend, err = newSQLLease(executor, SQLPostgreSQL)
					if err != nil {
						t.Fatal(err)
					}
					started := time.Now()
					applied := backend.Action(ctx, action, leaseState{
						Name: "reports", Owner: "worker", TTL: leaseTTL, Status: RoutineNotStarted,
					})
					if !applied {
						t.Fatal("logging timeout changed successful ownership result")
					}
					if elapsed := time.Since(started); elapsed != scenario.wantDuration {
						t.Fatalf("elapsed = %v, want %v", elapsed, scenario.wantDuration)
					}
					if pruneCalled != scenario.wantPrune {
						t.Fatalf("prune called = %t, want %t", pruneCalled, scenario.wantPrune)
					}
				})
			})
		}
	}
}
