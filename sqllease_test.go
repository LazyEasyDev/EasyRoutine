package EasyRoutine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
)

var _ leaseProvider = (*sqlLease)(nil)

type sqlExecStep struct {
	rows       int64
	execErr    error
	rowsErr    error
	panicValue any
}

type sqlExecCall struct {
	query string
	args  []any
}

type sqlStateError struct {
	state string
}

func (e sqlStateError) Error() string    { return "SQLSTATE " + e.state }
func (e sqlStateError) SQLState() string { return e.state }

type sqlQueryStep struct {
	columns  []string
	rows     [][]driver.Value
	queryErr error
}

type scriptedSQLExecer struct {
	steps      []sqlExecStep
	calls      []sqlExecCall
	querySteps []sqlQueryStep
	queryCalls []sqlExecCall
}

type cancelingSQLExecer struct {
	cancel context.CancelFunc
	calls  int
}

func (s *cancelingSQLExecer) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	s.calls++
	s.cancel()
	return nil, errors.New("connection interrupted")
}

func (s *scriptedSQLExecer) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	s.calls = append(s.calls, sqlExecCall{query: query, args: append([]any(nil), args...)})
	if len(s.calls) > len(s.steps) {
		return nil, errors.New("unexpected SQL execution")
	}

	step := s.steps[len(s.calls)-1]
	if step.panicValue != nil {
		panic(step.panicValue)
	}
	if step.execErr != nil {
		return nil, step.execErr
	}
	return scriptedSQLResult{rows: step.rows, err: step.rowsErr}, nil
}

type scriptedSQLResult struct {
	rows int64
	err  error
}

func (scriptedSQLResult) LastInsertId() (int64, error) {
	return 0, errors.New("not supported")
}

func (r scriptedSQLResult) RowsAffected() (int64, error) {
	return r.rows, r.err
}

type scriptedSQLConnector struct {
	executor *scriptedSQLExecer
}

func (c scriptedSQLConnector) Connect(context.Context) (driver.Conn, error) {
	return scriptedSQLConn{executor: c.executor}, nil
}

func (c scriptedSQLConnector) Driver() driver.Driver {
	return scriptedSQLDriver{executor: c.executor}
}

type scriptedSQLDriver struct {
	executor *scriptedSQLExecer
}

func (d scriptedSQLDriver) Open(string) (driver.Conn, error) {
	return scriptedSQLConn{executor: d.executor}, nil
}

type scriptedSQLConn struct {
	executor *scriptedSQLExecer
}

func (scriptedSQLConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not supported")
}

func (scriptedSQLConn) Close() error {
	return nil
}

func (scriptedSQLConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not supported")
}

func (c scriptedSQLConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	values := make([]any, len(args))
	for index, arg := range args {
		values[index] = arg.Value
	}
	return c.executor.ExecContext(ctx, query, values...)
}

func (c scriptedSQLConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	values := make([]any, len(args))
	for index, arg := range args {
		values[index] = arg.Value
	}
	c.executor.queryCalls = append(c.executor.queryCalls, sqlExecCall{query: query, args: values})
	if len(c.executor.queryCalls) > len(c.executor.querySteps) {
		return nil, errors.New("unexpected SQL query")
	}

	step := c.executor.querySteps[len(c.executor.queryCalls)-1]
	if step.queryErr != nil {
		return nil, step.queryErr
	}
	return &scriptedSQLRows{columns: step.columns, rows: step.rows}, nil
}

type scriptedSQLRows struct {
	columns []string
	rows    [][]driver.Value
	index   int
}

func TestRoutineIDUsesSHA256(t *testing.T) {
	const want = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got := routineID("abc"); got != want {
		t.Fatalf("routine ID = %q, want %q", got, want)
	}
	if routineID("Job") == routineID("job") {
		t.Fatal("routine ID does not preserve exact name identity")
	}
}

func (r *scriptedSQLRows) Columns() []string { return r.columns }
func (*scriptedSQLRows) Close() error        { return nil }

func (r *scriptedSQLRows) Next(values []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	copy(values, r.rows[r.index])
	r.index++
	return nil
}

func TestSQLLeaseSupportsKnownDialects(t *testing.T) {
	tests := []struct {
		dialect         SQLDialect
		leaseClock      string
		logClock        string
		primaryKey      string
		leaseTimeColumn string
		logTimeColumn   string
		releaseGuard    string
	}{
		{dialect: SQLPostgreSQL, leaseClock: postgreSQLCurrentSeconds, logClock: postgreSQLCurrentSeconds, primaryKey: `"routine_id" VARCHAR(64) PRIMARY KEY`, leaseTimeColumn: `"expires_at" BIGINT NOT NULL`, logTimeColumn: `"created_at" BIGINT NOT NULL`, releaseGuard: `WHERE "routine_id" = $5 AND "owner" = $6`},
		{dialect: SQLMySQL, leaseClock: mySQLCurrentSeconds, logClock: mySQLCurrentSeconds, primaryKey: "`routine_id` VARCHAR(64) NOT NULL PRIMARY KEY", leaseTimeColumn: "`expires_at` BIGINT NOT NULL", logTimeColumn: "`created_at` BIGINT NOT NULL", releaseGuard: "WHERE `routine_id` = ? AND `owner` = ?"},
		{dialect: SQLMariaDB, leaseClock: mySQLCurrentSeconds, logClock: mySQLCurrentSeconds, primaryKey: "`routine_id` VARCHAR(64) NOT NULL PRIMARY KEY", leaseTimeColumn: "`expires_at` BIGINT NOT NULL", logTimeColumn: "`created_at` BIGINT NOT NULL", releaseGuard: "WHERE `routine_id` = ? AND `owner` = ?"},
		{dialect: SQLTiDB, leaseClock: mySQLCurrentSeconds, logClock: mySQLCurrentSeconds, primaryKey: "`routine_id` VARCHAR(64) NOT NULL PRIMARY KEY", leaseTimeColumn: "`expires_at` BIGINT NOT NULL", logTimeColumn: "`created_at` BIGINT NOT NULL", releaseGuard: "WHERE `routine_id` = ? AND `owner` = ?"},
		{dialect: SQLSQLite, leaseClock: sqliteCurrentSeconds, logClock: sqliteCurrentSeconds, primaryKey: `"routine_id" TEXT PRIMARY KEY`, leaseTimeColumn: `"expires_at" INTEGER NOT NULL`, logTimeColumn: `"created_at" INTEGER NOT NULL`, releaseGuard: `WHERE "routine_id" = ? AND "owner" = ?`},
		{dialect: SQLServer, leaseClock: sqlServerCurrentSeconds, logClock: sqlServerCurrentSeconds, primaryKey: "[routine_id] VARCHAR(64) NOT NULL PRIMARY KEY", leaseTimeColumn: "[expires_at] BIGINT NOT NULL", logTimeColumn: "[created_at] BIGINT NOT NULL", releaseGuard: "WHERE [routine_id] = @p5 AND [owner] = @p6"},
		{dialect: SQLGaussDB, leaseClock: postgreSQLCurrentSeconds, logClock: postgreSQLCurrentSeconds, primaryKey: `"routine_id" VARCHAR(64) PRIMARY KEY`, leaseTimeColumn: `"expires_at" BIGINT NOT NULL`, logTimeColumn: `"created_at" BIGINT NOT NULL`, releaseGuard: `WHERE "routine_id" = $5 AND "owner" = $6`},
		{dialect: SQLOracle, leaseClock: oracleCurrentSeconds, logClock: oracleCurrentSeconds, primaryKey: `PRIMARY KEY ("routine_id")`, leaseTimeColumn: `"expires_at" NUMBER(19) NOT NULL`, logTimeColumn: `"created_at" NUMBER(19) NOT NULL`, releaseGuard: `WHERE "routine_id" = :5 AND "owner" = :6`},
	}

	for _, test := range tests {
		t.Run(string(test.dialect), func(t *testing.T) {
			executor := &scriptedSQLExecer{steps: []sqlExecStep{{}, {}, {}}}
			backend, err := newSQLLease(executor, test.dialect)
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.ensureSchema(context.Background()); err != nil {
				t.Fatal(err)
			}
			wantSchemaCalls := 2
			if backend.logs.createIndex != "" {
				wantSchemaCalls++
			}
			if len(executor.calls) != wantSchemaCalls || !strings.Contains(executor.calls[0].query, "unique_routine") || !strings.Contains(executor.calls[1].query, "unique_routine_log") {
				t.Fatalf("schema calls = %#v, want lease and log table creation", executor.calls)
			}
			if !strings.Contains(backend.statements.create, "success_count") || !strings.Contains(backend.statements.create, "updated_at") {
				t.Fatalf("lease schema %q does not store current state", backend.statements.create)
			}
			if !strings.Contains(backend.statements.create, test.primaryKey) {
				t.Fatalf("lease schema %q does not define routine ID primary key %q", backend.statements.create, test.primaryKey)
			}
			if !strings.Contains(backend.statements.create, test.leaseTimeColumn) || !strings.Contains(backend.statements.create, strings.Replace(test.leaseTimeColumn, "expires_at", "updated_at", 1)) {
				t.Fatalf("lease schema %q does not define integer Unix-second columns", backend.statements.create)
			}
			if !strings.Contains(backend.logs.create, test.logTimeColumn) {
				t.Fatalf("log schema %q does not define an integer Unix-second column", backend.logs.create)
			}
			if !strings.Contains(backend.logs.selectRoutineIDs, backend.logs.routineIDColumn) {
				t.Fatalf("log routine ID query %q does not select %q", backend.logs.selectRoutineIDs, backend.logs.routineIDColumn)
			}
			if !strings.HasPrefix(backend.statements.release, "UPDATE") {
				t.Fatalf("release statement %q does not retain current state", backend.statements.release)
			}
			if !strings.Contains(backend.statements.release, test.releaseGuard) {
				t.Fatalf("release statement %q is not owner guarded by %q", backend.statements.release, test.releaseGuard)
			}
			if !strings.Contains(backend.logs.create+backend.logs.createIndex, "unique_routine_log_routine_id") {
				t.Fatalf("log schema does not define a routine ID index")
			}
			for _, statement := range []string{backend.logs.create, backend.logs.insert, backend.logs.prune} {
				if !strings.Contains(statement, "routine_id") {
					t.Fatalf("history statement %q does not use the internal routine ID", statement)
				}
			}
			for _, statement := range []string{backend.logs.create, backend.logs.insert, backend.logs.selectBase} {
				if strings.Contains(statement, "success_count") || strings.Contains(statement, "failure_count") {
					t.Fatalf("history statement %q contains current-state counters", statement)
				}
			}
			for _, statement := range []string{backend.statements.acquireExisting, backend.statements.acquireNew, backend.statements.renew, backend.statements.release} {
				if !strings.Contains(statement, "routine_id") {
					t.Fatalf("lease statement %q does not use the internal routine ID", statement)
				}
				if !strings.Contains(statement, test.leaseClock) {
					t.Fatalf("lease statement %q does not contain database clock %q", statement, test.leaseClock)
				}
				for _, column := range []string{"status", "success_count", "failure_count", "log", "updated_at"} {
					if !strings.Contains(statement, column) {
						t.Fatalf("lease statement %q does not persist %q", statement, column)
					}
				}
			}
			if !strings.Contains(backend.statements.selectBase, "expires_at") || !strings.Contains(backend.statements.selectBase, "updated_at") {
				t.Fatalf("status query %q does not select lease timestamps", backend.statements.selectBase)
			}
			for _, query := range []string{backend.statements.selectBase, backend.logs.selectRoutineIDs, backend.logs.selectBase} {
				if strings.Contains(strings.ToUpper(query), "ORDER BY") {
					t.Fatalf("retrieval query %q orders rows in SQL", query)
				}
			}
			if !strings.Contains(backend.logs.insert, test.logClock) {
				t.Fatalf("log insert %q does not contain database clock %q", backend.logs.insert, test.logClock)
			}
			if !strings.Contains(backend.logs.prune, "> 25") {
				t.Fatalf("log prune %q does not retain 25 records", backend.logs.prune)
			}
			if strings.Contains(strings.ToLower(backend.logs.prune), "status") {
				t.Fatalf("log prune %q retains records per status instead of per name", backend.logs.prune)
			}
		})
	}
}

func TestTTLSecondsRoundsUp(t *testing.T) {
	tests := []struct {
		ttl   time.Duration
		want  int64
		valid bool
	}{
		{ttl: -time.Nanosecond},
		{ttl: 0},
		{ttl: time.Nanosecond, want: 1, valid: true},
		{ttl: time.Second, want: 1, valid: true},
		{ttl: time.Second + time.Nanosecond, want: 2, valid: true},
	}

	for _, test := range tests {
		got, valid := ttlSeconds(test.ttl)
		if got != test.want || valid != test.valid {
			t.Errorf("ttlSeconds(%s) = (%d, %t), want (%d, %t)", test.ttl, got, valid, test.want, test.valid)
		}
	}
}

func TestMySQLRenewConfirmsZeroChangeMatchedRow(t *testing.T) {
	executor := &scriptedSQLExecer{
		steps: []sqlExecStep{{rows: 0}},
		querySteps: []sqlQueryStep{{
			columns: []string{"owned"},
			rows:    [][]driver.Value{{int64(1)}},
		}},
	}
	db := sql.OpenDB(scriptedSQLConnector{executor: executor})
	t.Cleanup(func() { _ = db.Close() })
	backend, err := newSQLLease(db, SQLMySQL)
	if err != nil {
		t.Fatal(err)
	}
	state := leaseState{Name: "reports", Owner: "worker-1", TTL: time.Second, Status: RoutineRunning}
	if !backend.Action(context.Background(), LeaseRenew, state) {
		t.Fatal("zero-change MySQL renewal did not confirm matched ownership")
	}
	if len(executor.queryCalls) != 1 {
		t.Fatalf("ownership confirmation queries = %d, want 1", len(executor.queryCalls))
	}
	assertSQLCall(t, executor.queryCalls, 0, mySQLLeaseStatements.confirmRenew, routineID("reports"), "worker-1")
}

func TestMySQLRenewRejectsZeroChangeWithoutOwnership(t *testing.T) {
	executor := &scriptedSQLExecer{
		steps:      []sqlExecStep{{rows: 0}},
		querySteps: []sqlQueryStep{{columns: []string{"owned"}}},
	}
	db := sql.OpenDB(scriptedSQLConnector{executor: executor})
	t.Cleanup(func() { _ = db.Close() })
	backend, err := newSQLLease(db, SQLMySQL)
	if err != nil {
		t.Fatal(err)
	}
	state := leaseState{Name: "reports", Owner: "worker-1", TTL: time.Second, Status: RoutineRunning}
	if backend.Action(context.Background(), LeaseRenew, state) {
		t.Fatal("zero-change MySQL renewal confirmed absent ownership")
	}
}

func TestInitSQLLeaseValidatesConfiguration(t *testing.T) {
	if err := InitSQLLease(context.Background(), nil, SQLPostgreSQL); err == nil {
		t.Fatal("expected missing database error")
	}

	db := sql.OpenDB(scriptedSQLConnector{executor: &scriptedSQLExecer{}})
	t.Cleanup(func() { _ = db.Close() })
	if err := InitSQLLease(context.Background(), db, SQLDialect("clickhouse")); err == nil {
		t.Fatal("expected unsupported dialect error")
	}
}

func TestInitSQLLeaseEnsuresSchemaAndRegistersProvider(t *testing.T) {
	resetDefaultCoordinator(t)
	executor := &scriptedSQLExecer{steps: []sqlExecStep{{}, {}, {}}}
	db := sql.OpenDB(scriptedSQLConnector{executor: executor})
	t.Cleanup(func() { _ = db.Close() })

	if err := InitSQLLease(context.Background(), db, SQLPostgreSQL); err != nil {
		t.Fatal(err)
	}
	if len(executor.calls) != 3 || executor.calls[0].query != postgreSQLLeaseStatements.create || executor.calls[1].query != postgreSQLLogStatements.create || executor.calls[2].query != postgreSQLLogStatements.createIndex {
		t.Fatalf("schema calls = %#v, want PostgreSQL lease table, log table, and routine ID index creation", executor.calls)
	}
	if _, ok := defaultCoordinator.backend.(*sqlLease); !ok {
		t.Fatalf("backend type = %T, want *sqlLease", defaultCoordinator.backend)
	}
}

func TestInitSQLLeaseRejectsRepeatedInitializationBeforeSchema(t *testing.T) {
	resetDefaultCoordinator(t)
	firstExecutor := &scriptedSQLExecer{steps: []sqlExecStep{{}, {}, {}}}
	firstDB := sql.OpenDB(scriptedSQLConnector{executor: firstExecutor})
	t.Cleanup(func() { _ = firstDB.Close() })
	if err := InitSQLLease(context.Background(), firstDB, SQLPostgreSQL); err != nil {
		t.Fatal(err)
	}

	secondExecutor := &scriptedSQLExecer{}
	secondDB := sql.OpenDB(scriptedSQLConnector{executor: secondExecutor})
	t.Cleanup(func() { _ = secondDB.Close() })
	err := InitSQLLease(context.Background(), secondDB, SQLPostgreSQL)
	if err == nil || !strings.Contains(err.Error(), "already initialized") {
		t.Fatalf("repeated initialization error = %v, want already initialized", err)
	}
	if len(secondExecutor.calls) != 0 {
		t.Fatalf("second database received %d SQL calls, want 0", len(secondExecutor.calls))
	}
}

func TestInitSQLLeaseDoesNotRegisterAfterSchemaFailure(t *testing.T) {
	resetDefaultCoordinator(t)
	executor := &scriptedSQLExecer{steps: []sqlExecStep{{execErr: errors.New("permission denied")}}}
	db := sql.OpenDB(scriptedSQLConnector{executor: executor})
	t.Cleanup(func() { _ = db.Close() })

	if err := InitSQLLease(context.Background(), db, SQLPostgreSQL); err == nil {
		t.Fatal("expected schema error")
	}
	if defaultCoordinator != nil {
		t.Fatal("lease provider was registered after schema failure")
	}
}

func TestInitSQLLeaseDoesNotRegisterAfterLogSchemaFailure(t *testing.T) {
	resetDefaultCoordinator(t)
	executor := &scriptedSQLExecer{steps: []sqlExecStep{{}, {execErr: errors.New("permission denied")}}}
	db := sql.OpenDB(scriptedSQLConnector{executor: executor})
	t.Cleanup(func() { _ = db.Close() })

	if err := InitSQLLease(context.Background(), db, SQLPostgreSQL); err == nil {
		t.Fatal("expected log schema error")
	}
	if defaultCoordinator != nil {
		t.Fatal("lease provider was registered after log schema failure")
	}
}

func TestSQLLeaseRetriesConcurrentSchemaConflicts(t *testing.T) {
	executor := &scriptedSQLExecer{steps: []sqlExecStep{
		{execErr: sqlStateError{state: "23505"}}, {},
		{execErr: sqlStateError{state: "42P07"}}, {},
		{execErr: sqlStateError{state: "42710"}}, {},
	}}
	backend, err := newSQLLease(executor, SQLPostgreSQL)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.ensureSchema(context.Background()); err != nil {
		t.Fatalf("schema initialization did not recover from concurrent DDL: %v", err)
	}
	if len(executor.calls) != 6 {
		t.Fatalf("schema calls = %d, want 6 after one retry per object", len(executor.calls))
	}
}

func TestSQLLeaseAcquiresExpiredLeaseWithUpdate(t *testing.T) {
	executor := &scriptedSQLExecer{steps: []sqlExecStep{{rows: 1}}}
	backend, err := newSQLLease(executor, SQLPostgreSQL)
	if err != nil {
		t.Fatal(err)
	}

	state := leaseState{Name: "reports", Owner: "worker-1", TTL: time.Second + time.Nanosecond, Status: RoutineNotStarted}
	if !backend.acquireState(context.Background(), state, 2) {
		t.Fatal("expected lease acquisition")
	}
	assertSQLCall(t, executor.calls, 0, postgreSQLLeaseStatements.acquireExisting,
		"reports", "worker-1", int64(2), string(RoutineNotStarted), int64(0), int64(0), "", routineID("reports"))
	if len(executor.calls) != 1 {
		t.Fatalf("SQL calls = %d, want 1", len(executor.calls))
	}
}

func TestSQLLeaseAcquiresMissingLeaseWithInsert(t *testing.T) {
	executor := &scriptedSQLExecer{steps: []sqlExecStep{{rows: 0}, {rows: 1}}}
	backend, err := newSQLLease(executor, SQLMySQL)
	if err != nil {
		t.Fatal(err)
	}

	state := leaseState{Name: "reports", Owner: "worker-1", TTL: time.Second, Status: RoutineNotStarted}
	if !backend.acquireState(context.Background(), state, 1) {
		t.Fatal("expected lease acquisition")
	}
	assertSQLCall(t, executor.calls, 0, mySQLLeaseStatements.acquireExisting,
		"reports", "worker-1", int64(1), string(RoutineNotStarted), int64(0), int64(0), "", routineID("reports"))
	assertSQLCall(t, executor.calls, 1, mySQLLeaseStatements.acquireNew,
		routineID("reports"), "reports", "worker-1", int64(1), string(RoutineNotStarted), int64(0), int64(0), "")
}

func TestSQLLeaseReturnsFalseWhenInsertLosesRace(t *testing.T) {
	executor := &scriptedSQLExecer{steps: []sqlExecStep{{rows: 0}, {execErr: errors.New("duplicate key")}}}
	backend, err := newSQLLease(executor, SQLSQLite)
	if err != nil {
		t.Fatal(err)
	}

	state := leaseState{Name: "reports", Owner: "worker-1", TTL: time.Second, Status: RoutineNotStarted}
	if backend.acquireState(context.Background(), state, 1) {
		t.Fatal("acquired lease after insert lost its race")
	}
}

func TestSQLLeaseDoesNotInsertAfterUpdateFailure(t *testing.T) {
	executor := &scriptedSQLExecer{steps: []sqlExecStep{{execErr: errors.New("connection lost")}}}
	backend, err := newSQLLease(executor, SQLPostgreSQL)
	if err != nil {
		t.Fatal(err)
	}

	state := leaseState{Name: "reports", Owner: "worker-1", TTL: time.Second, Status: RoutineNotStarted}
	if backend.acquireState(context.Background(), state, 1) {
		t.Fatal("acquired lease after update failure")
	}
	if len(executor.calls) != 1 {
		t.Fatalf("SQL calls = %d, want 1", len(executor.calls))
	}
}

func TestSQLLeaseRenewsAndReleasesForOwner(t *testing.T) {
	executor := &scriptedSQLExecer{steps: []sqlExecStep{{rows: 1}, {rows: 1}}}
	backend, err := newSQLLease(executor, SQLServer)
	if err != nil {
		t.Fatal(err)
	}

	renewState := leaseState{Name: "reports", Owner: "worker-1", TTL: time.Second + time.Nanosecond, Status: RoutineRunning}
	if !backend.renewState(context.Background(), renewState, 2) {
		t.Fatal("expected lease renewal")
	}
	releaseState := leaseState{Name: "reports", Owner: "worker-1", Status: RoutineDone}
	if !backend.releaseState(context.Background(), releaseState) {
		t.Fatal("expected lease release")
	}
	assertSQLCall(t, executor.calls, 0, sqlServerLeaseStatements.renew,
		int64(2), string(RoutineRunning), int64(0), int64(0), "", routineID("reports"), "worker-1")
	assertSQLCall(t, executor.calls, 1, sqlServerLeaseStatements.release,
		string(RoutineDone), int64(0), int64(0), "", routineID("reports"), "worker-1")
}

func TestSQLLeaseActionRetriesRenewalAfterExecutionError(t *testing.T) {
	executor := &scriptedSQLExecer{steps: []sqlExecStep{
		{execErr: errors.New("connection interrupted")},
		{rows: 1},
	}}
	backend, err := newSQLLease(executor, SQLPostgreSQL)
	if err != nil {
		t.Fatal(err)
	}
	if backend.renewRetryDelay != 10*time.Second {
		t.Fatalf("renew retry delay = %s, want 10s", backend.renewRetryDelay)
	}
	backend.renewRetryDelay = 0
	state := leaseState{
		Name:   "reports",
		Owner:  "worker-1",
		TTL:    time.Second,
		Status: RoutineRunning,
	}

	if !backend.Action(context.Background(), LeaseRenew, state) {
		t.Fatal("renewal retry did not succeed")
	}
	wantArgs := []any{int64(1), string(RoutineRunning), int64(0), int64(0), "", routineID("reports"), "worker-1"}
	assertSQLCall(t, executor.calls, 0, postgreSQLLeaseStatements.renew, wantArgs...)
	assertSQLCall(t, executor.calls, 1, postgreSQLLeaseStatements.renew, wantArgs...)
	if len(executor.calls) != 2 {
		t.Fatalf("SQL calls = %d, want only two renewal attempts", len(executor.calls))
	}
}

func TestSQLLeaseActionReturnsFalseAfterRenewalRetryError(t *testing.T) {
	executor := &scriptedSQLExecer{steps: []sqlExecStep{
		{execErr: errors.New("connection interrupted")},
		{execErr: errors.New("connection still unavailable")},
	}}
	backend, err := newSQLLease(executor, SQLPostgreSQL)
	if err != nil {
		t.Fatal(err)
	}
	backend.renewRetryDelay = 0
	state := leaseState{
		Name:   "reports",
		Owner:  "worker-1",
		TTL:    time.Second,
		Status: RoutineRunning,
	}

	if backend.Action(context.Background(), LeaseRenew, state) {
		t.Fatal("renewal succeeded after two execution errors")
	}
	if len(executor.calls) != 2 {
		t.Fatalf("SQL calls = %d, want only two renewal attempts", len(executor.calls))
	}
}

func TestSQLLeaseDoesNotRetryRenewalAfterContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	executor := &cancelingSQLExecer{cancel: cancel}
	backend, err := newSQLLease(executor, SQLPostgreSQL)
	if err != nil {
		t.Fatal(err)
	}
	state := leaseState{
		Name:   "reports",
		Owner:  "worker-1",
		TTL:    time.Second,
		Status: RoutineRunning,
	}

	if backend.Action(ctx, LeaseRenew, state) {
		t.Fatal("renewal succeeded after its context was canceled")
	}
	if executor.calls != 1 {
		t.Fatalf("SQL calls = %d, want no retry after cancellation", executor.calls)
	}
}

func TestSQLLeaseActionDoesNotRecordRenewal(t *testing.T) {
	executor := &scriptedSQLExecer{steps: []sqlExecStep{{rows: 1}}}
	backend, err := newSQLLease(executor, SQLPostgreSQL)
	if err != nil {
		t.Fatal(err)
	}
	state := leaseState{
		Name:         "reports",
		Owner:        "worker-1",
		TTL:          time.Second,
		Status:       RoutineRunning,
		SuccessCount: 4,
		FailureCount: 2,
		Log:          "processing",
	}

	if !backend.Action(context.Background(), LeaseRenew, state) {
		t.Fatal("expected lease action to succeed")
	}
	assertSQLCall(t, executor.calls, 0, postgreSQLLeaseStatements.renew,
		int64(1), string(RoutineRunning), int64(4), int64(2), "processing", routineID("reports"), "worker-1")
	if len(executor.calls) != 1 {
		t.Fatalf("SQL calls = %d, want only renewal update", len(executor.calls))
	}
}

func TestSQLLeaseActionValidatesTTLOnce(t *testing.T) {
	executor := &scriptedSQLExecer{steps: []sqlExecStep{{rows: 1}}}
	backend, err := newSQLLease(executor, SQLPostgreSQL)
	if err != nil {
		t.Fatal(err)
	}
	validationCalls := 0
	backend.statements.ttlArgument = func(ttl time.Duration) (int64, bool) {
		validationCalls++
		return ttlSeconds(ttl)
	}
	state := leaseState{
		Name:   "reports",
		Owner:  "worker-1",
		TTL:    time.Second,
		Status: RoutineRunning,
	}

	if !backend.Action(context.Background(), LeaseRenew, state) {
		t.Fatal("expected renewal to succeed")
	}
	if validationCalls != 1 {
		t.Fatalf("TTL validation calls = %d, want 1", validationCalls)
	}
}

func TestSQLLeaseActionDispatchesAcquireAndRelease(t *testing.T) {
	tests := []struct {
		action    LeaseAction
		status    RoutineStatus
		wantQuery string
		wantArgs  []any
	}{
		{
			action:    LeaseAcquire,
			status:    RoutineNotStarted,
			wantQuery: postgreSQLLeaseStatements.acquireExisting,
			wantArgs:  []any{"reports", "worker-1", int64(1), string(RoutineNotStarted), int64(0), int64(0), "", routineID("reports")},
		},
		{
			action:    LeaseRelease,
			status:    RoutinePanic,
			wantQuery: postgreSQLLeaseStatements.release,
			wantArgs:  []any{string(RoutinePanic), int64(0), int64(0), "", routineID("reports"), "worker-1"},
		},
	}

	for _, test := range tests {
		t.Run(string(test.action), func(t *testing.T) {
			executor := &scriptedSQLExecer{steps: []sqlExecStep{{rows: 1}, {rows: 1}, {rows: 1}}}
			backend, err := newSQLLease(executor, SQLPostgreSQL)
			if err != nil {
				t.Fatal(err)
			}
			state := leaseState{
				Name:   "reports",
				Owner:  "worker-1",
				TTL:    time.Second,
				Status: test.status,
			}

			if !backend.Action(context.Background(), test.action, state) {
				t.Fatalf("%s action failed", test.action)
			}
			assertSQLCall(t, executor.calls, 0, test.wantQuery, test.wantArgs...)
			if len(executor.calls) != 3 {
				t.Fatalf("SQL calls = %d, want action, log insert, and prune", len(executor.calls))
			}
		})
	}
}

func TestSQLLeaseActionRecordsIdempotentRelease(t *testing.T) {
	executor := &scriptedSQLExecer{steps: []sqlExecStep{{rows: 0}, {rows: 1}, {rows: 1}}}
	backend, err := newSQLLease(executor, SQLPostgreSQL)
	if err != nil {
		t.Fatal(err)
	}
	state := leaseState{
		Name:         "reports",
		Owner:        "worker-1",
		Status:       RoutinePanic,
		FailureCount: 1,
		Log:          "boom",
	}

	if !backend.Action(context.Background(), LeaseRelease, state) {
		t.Fatal("release should succeed when ownership is already absent")
	}
	if len(executor.calls) != 3 {
		t.Fatalf("SQL calls = %d, want release, log insert, and prune", len(executor.calls))
	}
	assertSQLCall(t, executor.calls, 0, postgreSQLLeaseStatements.release,
		string(RoutinePanic), int64(0), int64(1), "boom", routineID("reports"), "worker-1")
	insert := executor.calls[1]
	if insert.query != postgreSQLLogStatements.insert {
		t.Fatalf("insert query = %q, want %q", insert.query, postgreSQLLogStatements.insert)
	}
	wantInsertTail := []any{routineID("reports"), "reports", "worker-1", string(LeaseRelease), string(RoutinePanic), "boom"}
	if len(insert.args) != 7 || !reflect.DeepEqual(insert.args[1:], wantInsertTail) {
		t.Fatalf("insert arguments = %#v, want ID followed by %#v", insert.args, wantInsertTail)
	}
	assertSQLCall(t, executor.calls, 2, postgreSQLLogStatements.prune, routineID("reports"))
}

func TestSQLLeaseActionKeepsOwnershipResultWhenLoggingFails(t *testing.T) {
	executor := &scriptedSQLExecer{steps: []sqlExecStep{{rows: 1}, {execErr: errors.New("log unavailable")}}}
	backend, err := newSQLLease(executor, SQLSQLite)
	if err != nil {
		t.Fatal(err)
	}
	state := leaseState{
		Name:   "reports",
		Owner:  "worker-1",
		TTL:    time.Second,
		Status: RoutineNotStarted,
	}

	if !backend.Action(context.Background(), LeaseAcquire, state) {
		t.Fatal("log failure changed a successful ownership result")
	}
	if len(executor.calls) != 2 {
		t.Fatalf("SQL calls = %d, want acquire and failed log insert", len(executor.calls))
	}
}

func TestSQLLeaseActionKeepsOwnershipResultWhenLoggingPanics(t *testing.T) {
	executor := &scriptedSQLExecer{steps: []sqlExecStep{{rows: 1}, {panicValue: "log unavailable"}}}
	backend, err := newSQLLease(executor, SQLSQLite)
	if err != nil {
		t.Fatal(err)
	}
	state := leaseState{
		Name:   "reports",
		Owner:  "worker-1",
		TTL:    time.Second,
		Status: RoutineNotStarted,
	}

	if !backend.Action(context.Background(), LeaseAcquire, state) {
		t.Fatal("log panic changed a successful ownership result")
	}
	if len(executor.calls) != 2 {
		t.Fatalf("SQL calls = %d, want acquire and panicking log insert", len(executor.calls))
	}
}

func TestSQLLeaseActionDoesNotLogFailedOwnershipOperation(t *testing.T) {
	executor := &scriptedSQLExecer{steps: []sqlExecStep{{rows: 0}}}
	backend, err := newSQLLease(executor, SQLSQLite)
	if err != nil {
		t.Fatal(err)
	}
	state := leaseState{
		Name:   "reports",
		Owner:  "worker-1",
		TTL:    time.Second,
		Status: RoutineRunning,
	}

	if backend.Action(context.Background(), LeaseRenew, state) {
		t.Fatal("unexpected successful ownership result")
	}
	if len(executor.calls) != 1 {
		t.Fatalf("SQL calls = %d, want only failed renewal", len(executor.calls))
	}
}

func TestSQLLeaseGetLogsFiltersNamesAndGroupsHistory(t *testing.T) {
	columns := []string{"id", "name", "owner", "action", "status", "log", "created_at"}
	newest := time.Date(2026, time.September, 10, 12, 0, 0, 0, time.UTC).Unix()
	older := newest - int64(time.Hour/time.Second)
	executor := &scriptedSQLExecer{querySteps: []sqlQueryStep{
		{
			columns: columns,
			rows: [][]driver.Value{
				{"log-2", "name2", "worker-2", "release", "panic", "boom", newest},
				{"log-1", "name1", "worker-1", "renew", "running", nil, fmt.Sprint(older)},
			},
		},
		{columns: []string{"routine_id"}, rows: [][]driver.Value{{routineID("name1")}}},
		{columns: columns},
	}}
	db := sql.OpenDB(scriptedSQLConnector{executor: executor})
	t.Cleanup(func() { _ = db.Close() })
	backend, err := newSQLLease(db, SQLPostgreSQL)
	if err != nil {
		t.Fatal(err)
	}

	logs, err := backend.GetLogs(context.Background(), "name1", "name2")
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 2 {
		t.Fatalf("history = %#v, want 2 names", logs)
	}
	name2 := logs["name2"]
	if len(name2) != 1 || name2[0].ID != "log-2" || name2[0].Action != LeaseRelease || name2[0].Status != RoutinePanic || name2[0].Log != "boom" {
		t.Fatalf("name2 history = %#v, want panic release", name2)
	}
	if name2[0].CreatedAt != newest {
		t.Fatalf("created time = %d, want %d", name2[0].CreatedAt, newest)
	}
	name1 := logs["name1"]
	if len(name1) != 1 || name1[0].Log != "" {
		t.Fatalf("name1 history = %#v, want empty nullable log", name1)
	}
	if name1[0].CreatedAt != older {
		t.Fatalf("older created time = %d, want %d", name1[0].CreatedAt, older)
	}
	filteredQuery := postgreSQLLogStatements.selectBase + ` WHERE "routine_id" IN ($1, $2)`
	assertSQLCall(t, executor.queryCalls, 0, filteredQuery, routineID("name1"), routineID("name2"))

	if _, err := backend.GetLogs(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertSQLCall(t, executor.queryCalls, 1, postgreSQLLogStatements.selectRoutineIDs)
	allLogsQuery := postgreSQLLogStatements.selectBase + ` WHERE "routine_id" IN ($1)`
	assertSQLCall(t, executor.queryCalls, 2, allLogsQuery, routineID("name1"))
}

func TestSQLLeaseGetStatusesFiltersNames(t *testing.T) {
	columns := []string{"name", "owner", "status", "success_count", "failure_count", "log", "expires_at", "updated_at"}
	expiresAt := time.Date(2026, time.September, 10, 12, 5, 0, 0, time.UTC).Unix()
	updatedAt := expiresAt - int64(time.Minute/time.Second)
	executor := &scriptedSQLExecer{querySteps: []sqlQueryStep{{
		columns: columns,
		rows: [][]driver.Value{{
			"reports", "worker-1", "running", int64(3), int64(1), nil, expiresAt, fmt.Sprint(updatedAt),
		}},
	}}}
	db := sql.OpenDB(scriptedSQLConnector{executor: executor})
	t.Cleanup(func() { _ = db.Close() })
	backend, err := newSQLLease(db, SQLPostgreSQL)
	if err != nil {
		t.Fatal(err)
	}

	statuses, err := backend.GetStatuses(context.Background(), "reports")
	if err != nil {
		t.Fatal(err)
	}
	if len(statuses) != 1 {
		t.Fatalf("statuses = %#v, want one current status", statuses)
	}
	status := statuses["reports"]
	if status.Name != "reports" || status.Owner != "worker-1" || status.Status != RoutineRunning || status.SuccessCount != 3 || status.FailureCount != 1 || status.Log != "" {
		t.Fatalf("status = %#v, want current running state", status)
	}
	if status.ExpiresAt != expiresAt {
		t.Fatalf("expiry = %d, want %d", status.ExpiresAt, expiresAt)
	}
	if status.UpdatedAt != updatedAt {
		t.Fatalf("updated time = %d, want %d", status.UpdatedAt, updatedAt)
	}
	query := postgreSQLLeaseStatements.selectBase + ` WHERE "routine_id" IN ($1)`
	assertSQLCall(t, executor.queryCalls, 0, query, routineID("reports"))
}

func TestSQLLeaseQueriesReturnEmptyMaps(t *testing.T) {
	executor := &scriptedSQLExecer{querySteps: []sqlQueryStep{
		{columns: []string{"routine_id"}},
		{columns: []string{"name", "owner", "status", "success_count", "failure_count", "log", "expires_at", "updated_at"}},
	}}
	db := sql.OpenDB(scriptedSQLConnector{executor: executor})
	t.Cleanup(func() { _ = db.Close() })
	backend, err := newSQLLease(db, SQLPostgreSQL)
	if err != nil {
		t.Fatal(err)
	}

	history, err := backend.GetLogs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if history == nil || len(history) != 0 {
		t.Fatalf("history = %#v, want non-nil empty map", history)
	}

	statuses, err := backend.GetStatuses(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if statuses == nil || len(statuses) != 0 {
		t.Fatalf("statuses = %#v, want non-nil empty map", statuses)
	}
}

func TestSQLLeaseGetLogsDeduplicatesAndBatchesNames(t *testing.T) {
	columns := []string{"id", "name", "owner", "action", "status", "log", "created_at"}
	latest := time.Date(2026, time.September, 10, 12, 0, 0, 0, time.UTC)
	executor := &scriptedSQLExecer{querySteps: []sqlQueryStep{
		{
			columns: columns,
			rows: [][]driver.Value{
				{"log-a2", "name-a", "worker-1", "release", "done", nil, latest.Unix()},
				{"log-z", "name-z", "worker-1", "release", "done", nil, latest.Add(-2 * time.Hour).Unix()},
			},
		},
		{
			columns: columns,
			rows: [][]driver.Value{
				{"log-a1", "name-a", "worker-2", "acquire", "not_started", nil, latest.Add(-time.Hour).Unix()},
				{"log-b", "name-b", "worker-2", "release", "done", nil, latest.Unix()},
			},
		},
	}}
	db := sql.OpenDB(scriptedSQLConnector{executor: executor})
	t.Cleanup(func() { _ = db.Close() })
	backend, err := newSQLLease(db, SQLPostgreSQL)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, sqlNameFilterBatchSize+1)
	for index := range names {
		names[index] = fmt.Sprintf("routine-%03d", index)
	}
	names = append(names, names[0])

	logs, err := backend.GetLogs(context.Background(), names...)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 3 {
		t.Fatalf("history = %#v, want 3 names", logs)
	}
	if got := []string{logs["name-a"][0].ID, logs["name-a"][1].ID}; !reflect.DeepEqual(got, []string{"log-a1", "log-a2"}) {
		t.Fatalf("name-a log IDs = %v, want oldest-first history", got)
	}
	assertBatchedNameFilterCalls(t, executor.queryCalls, names)
}

func TestSQLLeaseGetLogsDiscoversAndBatchesAllRoutineIDs(t *testing.T) {
	columns := []string{"id", "name", "owner", "action", "status", "log", "created_at"}
	routineIDs := make([]string, sqlNameFilterBatchSize+1)
	routineIDRows := make([][]driver.Value, len(routineIDs))
	for index := range routineIDs {
		routineIDs[index] = fmt.Sprintf("routine-id-%03d", index)
		routineIDRows[index] = []driver.Value{routineIDs[index]}
	}
	executor := &scriptedSQLExecer{querySteps: []sqlQueryStep{
		{columns: []string{"routine_id"}, rows: routineIDRows},
		{columns: columns, rows: [][]driver.Value{{"log-a", "name-a", "worker-1", "acquire", "running", nil, int64(1)}}},
		{columns: columns, rows: [][]driver.Value{{"log-b", "name-b", "worker-2", "release", "done", nil, int64(2)}}},
	}}
	db := sql.OpenDB(scriptedSQLConnector{executor: executor})
	t.Cleanup(func() { _ = db.Close() })
	backend, err := newSQLLease(db, SQLPostgreSQL)
	if err != nil {
		t.Fatal(err)
	}

	logs, err := backend.GetLogs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if logs["name-a"][0].ID != "log-a" || logs["name-b"][0].ID != "log-b" {
		t.Fatalf("grouped history = %#v, want records keyed by name", logs)
	}
	assertSQLCall(t, executor.queryCalls, 0, postgreSQLLogStatements.selectRoutineIDs)
	assertBatchedRoutineIDFilterCalls(t, executor.queryCalls[1:], routineIDs)
}

func TestSQLLeaseGetStatusesDeduplicatesAndBatchesNames(t *testing.T) {
	columns := []string{"name", "owner", "status", "success_count", "failure_count", "log", "expires_at", "updated_at"}
	now := time.Date(2026, time.September, 10, 12, 0, 0, 0, time.UTC)
	executor := &scriptedSQLExecer{querySteps: []sqlQueryStep{
		{
			columns: columns,
			rows: [][]driver.Value{
				{"zeta", "worker-1", "running", int64(1), int64(0), nil, now.Unix(), now.Unix()},
			},
		},
		{
			columns: columns,
			rows: [][]driver.Value{
				{"alpha", "worker-2", "running", int64(1), int64(0), nil, now.Unix(), now.Unix()},
			},
		},
	}}
	db := sql.OpenDB(scriptedSQLConnector{executor: executor})
	t.Cleanup(func() { _ = db.Close() })
	backend, err := newSQLLease(db, SQLPostgreSQL)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, sqlNameFilterBatchSize+1)
	for index := range names {
		names[index] = fmt.Sprintf("routine-%03d", index)
	}
	names = append(names, names[0])

	statuses, err := backend.GetStatuses(context.Background(), names...)
	if err != nil {
		t.Fatal(err)
	}
	if len(statuses) != 2 {
		t.Fatalf("statuses = %#v, want 2 records", statuses)
	}
	if statuses["alpha"].Name != "alpha" || statuses["zeta"].Name != "zeta" {
		t.Fatalf("statuses = %#v, want states keyed by name", statuses)
	}
	assertBatchedNameFilterCalls(t, executor.queryCalls, names)
}

func assertBatchedNameFilterCalls(t *testing.T, calls []sqlExecCall, names []string) {
	t.Helper()
	routineIDs := uniqueRoutineIDs(names)
	assertBatchedRoutineIDFilterCalls(t, calls, routineIDs)
}

func assertBatchedRoutineIDFilterCalls(t *testing.T, calls []sqlExecCall, routineIDs []string) {
	t.Helper()
	if len(calls) != 2 {
		t.Fatalf("query calls = %d, want 2", len(calls))
	}
	if len(calls[0].args) != sqlNameFilterBatchSize || len(calls[1].args) != 1 {
		t.Fatalf("query argument counts = %d and %d, want %d and 1", len(calls[0].args), len(calls[1].args), sqlNameFilterBatchSize)
	}
	if calls[0].args[0] != routineIDs[0] || calls[1].args[0] != routineIDs[sqlNameFilterBatchSize] {
		t.Fatalf("query batches did not preserve routine ID order")
	}
	if strings.Contains(calls[1].query, "$2") {
		t.Fatalf("second query did not reset bind variables: %q", calls[1].query)
	}
}

func TestSQLLeaseRejectsInvalidAction(t *testing.T) {
	executor := &scriptedSQLExecer{}
	backend, err := newSQLLease(executor, SQLPostgreSQL)
	if err != nil {
		t.Fatal(err)
	}
	valid := leaseState{Name: "reports", Owner: "worker-1", TTL: time.Second, Status: RoutineRunning}
	if backend.Action(context.Background(), LeaseAction("unknown"), valid) {
		t.Fatal("unknown lease action succeeded")
	}
	invalidStatus := valid
	invalidStatus.Status = RoutineStatus("unknown")
	if backend.Action(context.Background(), LeaseRenew, invalidStatus) {
		t.Fatal("unknown routine status succeeded")
	}
	invalidName := valid
	invalidName.Name = strings.Repeat("a", 256)
	if backend.Action(context.Background(), LeaseRenew, invalidName) {
		t.Fatal("oversized routine name succeeded")
	}
	if len(executor.calls) != 0 {
		t.Fatalf("SQL calls = %d, want 0", len(executor.calls))
	}
}

func TestSQLLeaseUsesOracleStatementsAndBindings(t *testing.T) {
	executor := &scriptedSQLExecer{steps: []sqlExecStep{{rows: 0}, {rows: 1}, {rows: 1}, {rows: 1}}}
	backend, err := newSQLLease(executor, SQLOracle)
	if err != nil {
		t.Fatal(err)
	}

	acquireState := leaseState{Name: "reports", Owner: "worker-1", TTL: time.Second + time.Nanosecond, Status: RoutineNotStarted}
	if !backend.acquireState(context.Background(), acquireState, 2) {
		t.Fatal("expected Oracle lease acquisition")
	}
	renewState := leaseState{Name: "reports", Owner: "worker-1", TTL: time.Second, Status: RoutineRunning}
	if !backend.renewState(context.Background(), renewState, 1) {
		t.Fatal("expected Oracle lease renewal")
	}
	releaseState := leaseState{Name: "reports", Owner: "worker-1", Status: RoutineDone}
	if !backend.releaseState(context.Background(), releaseState) {
		t.Fatal("expected Oracle lease release")
	}

	assertSQLCall(t, executor.calls, 0, oracleLeaseStatements.acquireExisting,
		"reports", "worker-1", int64(2), string(RoutineNotStarted), int64(0), int64(0), "", routineID("reports"))
	assertSQLCall(t, executor.calls, 1, oracleLeaseStatements.acquireNew,
		routineID("reports"), "reports", "worker-1", int64(2), string(RoutineNotStarted), int64(0), int64(0), "")
	assertSQLCall(t, executor.calls, 2, oracleLeaseStatements.renew,
		int64(1), string(RoutineRunning), int64(0), int64(0), "", routineID("reports"), "worker-1")
	assertSQLCall(t, executor.calls, 3, oracleLeaseStatements.release,
		string(RoutineDone), int64(0), int64(0), "", routineID("reports"), "worker-1")
}

func TestOracleSchemaCreationVerifiesExistingObjectIsTable(t *testing.T) {
	if !strings.Contains(oracleLeaseStatements.create, "SQLCODE != -955") {
		t.Fatal("Oracle schema creation does not handle an existing table")
	}
	if !strings.Contains(oracleLeaseStatements.create, "FROM USER_TABLES") {
		t.Fatal("Oracle schema creation does not verify the existing object is a table")
	}
}

func TestOracleSchemaCreationVerifiesExistingIndexDefinition(t *testing.T) {
	for _, required := range []string{"USER_IND_COLUMNS", "TABLE_NAME = 'unique_routine_log'", "COLUMN_NAME = 'routine_id'", "COLUMN_POSITION = 1"} {
		if !strings.Contains(oracleLogStatements.createIndex, required) {
			t.Fatalf("Oracle index creation does not verify %q", required)
		}
	}
}

func TestSQLLeaseRejectsInvalidOperations(t *testing.T) {
	executor := &scriptedSQLExecer{}
	backend, err := newSQLLease(executor, SQLServer)
	if err != nil {
		t.Fatal(err)
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if backend.Action(canceledCtx, LeaseAcquire, leaseState{Name: "reports", Owner: "worker-1", TTL: time.Second, Status: RoutineNotStarted}) {
		t.Fatal("acquired lease with canceled context")
	}
	if backend.Action(context.Background(), LeaseAcquire, leaseState{Name: "", Owner: "worker-1", TTL: time.Second, Status: RoutineNotStarted}) {
		t.Fatal("acquired unnamed lease")
	}
	if backend.Action(context.Background(), LeaseRenew, leaseState{Name: "reports", Owner: "worker-1", Status: RoutineRunning}) {
		t.Fatal("renewed lease with zero TTL")
	}
	if backend.Action(context.Background(), LeaseRelease, leaseState{Name: "reports", Status: RoutineDone}) {
		t.Fatal("released lease without owner")
	}
	if len(executor.calls) != 0 {
		t.Fatalf("SQL calls = %d, want 0", len(executor.calls))
	}
}

func TestZeroSQLLeaseFailsCleanly(t *testing.T) {
	var backend *sqlLease
	if err := backend.ensureSchema(context.Background()); err == nil {
		t.Fatal("expected uninitialized lease error")
	}
	if backend.Action(context.Background(), LeaseAcquire, leaseState{Name: "reports", Owner: "worker-1", TTL: time.Second, Status: RoutineNotStarted}) {
		t.Fatal("zero lease acquired ownership")
	}
	if backend.Action(context.Background(), LeaseRenew, leaseState{Name: "reports", Owner: "worker-1", TTL: time.Second, Status: RoutineRunning}) {
		t.Fatal("zero lease renewed ownership")
	}
	if backend.Action(context.Background(), LeaseRelease, leaseState{Name: "reports", Owner: "worker-1", Status: RoutineDone}) {
		t.Fatal("zero lease released ownership")
	}
}

func assertSQLCall(t *testing.T, calls []sqlExecCall, index int, query string, args ...any) {
	t.Helper()
	if len(calls) <= index {
		t.Fatalf("SQL calls = %d, want call %d", len(calls), index+1)
	}
	if calls[index].query != query {
		t.Fatalf("query = %q, want %q", calls[index].query, query)
	}
	if len(calls[index].args) != len(args) || len(args) > 0 && !reflect.DeepEqual(calls[index].args, args) {
		t.Fatalf("arguments = %#v, want %#v", calls[index].args, args)
	}
}
