//go:build integration

package EasyRoutine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/sijms/go-ora/v2"
	_ "modernc.org/sqlite"
)

const (
	realDialectEnvironment = "EASYROUTINE_TEST_DIALECT"
	realDSNEnvironment     = "EASYROUTINE_TEST_DSN"
	realWorkerEnvironment  = "EASYROUTINE_TEST_WORKER"
)

type realDatabaseConfig struct {
	dialect SQLDialect
	driver  string
	dsn     string
}

func TestRealSQLBackend(t *testing.T) {
	config := realDatabaseConfigFromEnvironment(t)
	db := openRealDatabase(t, config)
	dropRealSchema(t, db, config.dialect)
	t.Cleanup(func() { dropRealSchema(t, db, config.dialect) })

	t.Run("ConcurrentSchemaAcrossProcesses", func(t *testing.T) {
		runRealWorkers(t, 8, map[string]string{
			realWorkerEnvironment: "schema",
		})
	})
	t.Run("PublicAPIInitialization", func(t *testing.T) {
		runRealWorkers(t, 1, map[string]string{
			realWorkerEnvironment: "public-api",
		})
	})

	backend, err := newSQLLease(db, config.dialect)
	if err != nil {
		t.Fatalf("create SQL lease: %v", err)
	}
	if err := backend.ensureSchema(t.Context()); err != nil {
		t.Fatalf("verify SQL schema: %v", err)
	}

	t.Run("OwnershipAndQueries", func(t *testing.T) {
		testRealOwnershipAndQueries(t, backend)
	})
	t.Run("ExpiredLeaseTakeover", func(t *testing.T) {
		testRealExpiredLeaseTakeover(t, backend)
	})
	t.Run("UnicodeAndLargeValues", func(t *testing.T) {
		testRealUnicodeAndLargeValues(t, backend)
	})
	t.Run("MassiveContention", func(t *testing.T) {
		testRealMassiveContention(t, backend)
	})
	t.Run("ConcurrentHistoryPruning", func(t *testing.T) {
		testRealConcurrentHistoryPruning(t, backend)
	})
	t.Run("BulkFilteredQueries", func(t *testing.T) {
		testRealBulkFilteredQueries(t, backend)
	})
	t.Run("SupervisorRepeatsAndRenews", func(t *testing.T) {
		testRealSupervisorRepeatsAndRenews(t, backend)
	})
	t.Run("SupervisorPanicState", func(t *testing.T) {
		testRealSupervisorPanicState(t, backend)
	})
	t.Run("SupervisorCancelsOnLeaseLoss", func(t *testing.T) {
		testRealSupervisorCancelsOnLeaseLoss(t, backend)
	})
	t.Run("SupervisorHandoff", func(t *testing.T) {
		testRealSupervisorHandoff(t, backend)
	})
	t.Run("ParallelDistinctSupervisors", func(t *testing.T) {
		testRealParallelDistinctSupervisors(t, backend)
	})
	t.Run("ContentionAcrossProcesses", func(t *testing.T) {
		lockPath := t.TempDir() + "/active-worker"
		runRealWorkers(t, realProcessWorkerCount(), map[string]string{
			realWorkerEnvironment:        "lease",
			"EASYROUTINE_TEST_LOCK_PATH": lockPath,
		})
	})
}

func TestRealSQLProcessWorker(t *testing.T) {
	workerMode := os.Getenv(realWorkerEnvironment)
	if workerMode == "" {
		t.Skip("integration worker helper")
	}

	config := realDatabaseConfigFromEnvironment(t)
	db := openRealDatabase(t, config)
	backend, err := newSQLLease(db, config.dialect)
	if err != nil {
		t.Fatalf("create worker SQL lease: %v", err)
	}

	switch workerMode {
	case "schema":
		if err := backend.ensureSchema(t.Context()); err != nil {
			t.Fatalf("initialize schema: %v", err)
		}
	case "public-api":
		if err := InitSQLLease(t.Context(), db, config.dialect); err != nil {
			t.Fatalf("initialize exported SQL lease API: %v", err)
		}
		if _, err := GetStatuses(t.Context()); err != nil {
			t.Fatalf("query exported SQL lease API: %v", err)
		}
	case "lease":
		runRealLeaseWorker(t, backend)
	default:
		t.Fatalf("unknown integration worker mode %q", workerMode)
	}
}

func realDatabaseConfigFromEnvironment(t *testing.T) realDatabaseConfig {
	t.Helper()
	dialect := SQLDialect(os.Getenv(realDialectEnvironment))
	dsn := os.Getenv(realDSNEnvironment)
	if dialect == "" || dsn == "" {
		t.Skipf("set %s and %s to run real database tests", realDialectEnvironment, realDSNEnvironment)
	}
	if _, err := statementsForSQLDialect(dialect); err != nil {
		t.Fatalf("invalid real database dialect: %v", err)
	}

	driver := ""
	switch dialect {
	case SQLPostgreSQL, SQLGaussDB:
		driver = "pgx"
	case SQLMySQL, SQLMariaDB, SQLTiDB:
		driver = "mysql"
	case SQLSQLite:
		driver = "sqlite"
	case SQLServer:
		driver = "sqlserver"
	case SQLOracle:
		driver = "oracle"
	}
	return realDatabaseConfig{dialect: dialect, driver: driver, dsn: dsn}
}

func openRealDatabase(t *testing.T, config realDatabaseConfig) *sql.DB {
	t.Helper()
	db, err := sql.Open(config.driver, config.dsn)
	if err != nil {
		t.Fatalf("open %s database: %v", config.dialect, err)
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	pingCtx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		t.Fatalf("ping %s database: %v", config.dialect, err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close %s database: %v", config.dialect, err)
		}
	})
	return db
}

func dropRealSchema(t *testing.T, db *sql.DB, dialect SQLDialect) {
	t.Helper()
	statements := []string{
		`DROP TABLE IF EXISTS "unique_routine_log"`,
		`DROP TABLE IF EXISTS "unique_routine"`,
	}
	switch dialect {
	case SQLMySQL, SQLMariaDB, SQLTiDB:
		statements = []string{
			"DROP TABLE IF EXISTS `unique_routine_log`",
			"DROP TABLE IF EXISTS `unique_routine`",
		}
	case SQLServer:
		statements = []string{
			`IF OBJECT_ID(N'unique_routine_log', N'U') IS NOT NULL DROP TABLE [unique_routine_log]`,
			`IF OBJECT_ID(N'unique_routine', N'U') IS NOT NULL DROP TABLE [unique_routine]`,
		}
	case SQLOracle:
		statements = []string{
			`BEGIN EXECUTE IMMEDIATE 'DROP TABLE "unique_routine_log" PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;`,
			`BEGIN EXECUTE IMMEDIATE 'DROP TABLE "unique_routine" PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;`,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("drop real integration schema for %s: %v", dialect, err)
		}
	}
}

func testRealOwnershipAndQueries(t *testing.T, backend *sqlLease) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	name := integrationRoutineName(t, "ownership")
	first := realLeaseState(name, "owner-a")
	second := realLeaseState(name, "owner-b")

	if !backend.Action(ctx, LeaseAcquire, first) {
		t.Fatal("first owner did not acquire a new lease")
	}
	if backend.Action(ctx, LeaseAcquire, second) {
		t.Fatal("second owner acquired a live lease")
	}
	if backend.Action(ctx, LeaseRenew, second) {
		t.Fatal("non-owner renewed a lease")
	}
	if !backend.Action(ctx, LeaseRenew, first) {
		t.Fatal("current owner did not renew its lease")
	}
	if !backend.Action(ctx, LeaseRelease, second) {
		t.Fatal("idempotent non-owner release failed")
	}

	statuses, err := backend.GetStatuses(ctx, name, name, "missing-real-routine")
	if err != nil {
		t.Fatalf("query filtered statuses: %v", err)
	}
	if len(statuses) != 1 || statuses[name].Owner != first.Owner {
		t.Fatalf("unexpected state after non-owner release: %#v", statuses)
	}
	if !backend.Action(ctx, LeaseRelease, first) {
		t.Fatal("current owner did not release its lease")
	}
	if !backend.Action(ctx, LeaseAcquire, second) {
		t.Fatal("second owner did not acquire a released lease")
	}
	if !backend.Action(ctx, LeaseRelease, second) {
		t.Fatal("second owner did not release its lease")
	}

	history, err := backend.GetLogs(ctx, name)
	if err != nil {
		t.Fatalf("query filtered history: %v", err)
	}
	if len(history[name]) < 5 {
		t.Fatalf("expected lifecycle history, got %d records", len(history[name]))
	}
	for index := 1; index < len(history[name]); index++ {
		if history[name][index-1].CreatedAt > history[name][index].CreatedAt {
			t.Fatalf("history is not oldest-first: %#v", history[name])
		}
	}
	allStatuses, err := backend.GetStatuses(ctx)
	if err != nil {
		t.Fatalf("query unfiltered statuses: %v", err)
	}
	if allStatuses[name].Name != name || allStatuses[name].Owner != second.Owner {
		t.Fatalf("unfiltered statuses omitted or changed %q: %#v", name, allStatuses[name])
	}
	allHistory, err := backend.GetLogs(ctx)
	if err != nil {
		t.Fatalf("query unfiltered history: %v", err)
	}
	if len(allHistory[name]) != len(history[name]) {
		t.Fatalf("unfiltered history for %q has %d records, want %d", name, len(allHistory[name]), len(history[name]))
	}
}

func testRealExpiredLeaseTakeover(t *testing.T, backend *sqlLease) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	name := integrationRoutineName(t, "expired-takeover")
	stale := realLeaseState(name, "expired-owner")
	stale.TTL = time.Second
	replacement := realLeaseState(name, "replacement-owner")
	if !backend.Action(ctx, LeaseAcquire, stale) {
		t.Fatal("initial owner did not acquire expiring lease")
	}

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for !backend.Action(ctx, LeaseAcquire, replacement) {
		select {
		case <-ctx.Done():
			t.Fatal("replacement owner did not acquire expired lease")
		case <-ticker.C:
		}
	}
	if backend.Action(ctx, LeaseRenew, stale) {
		t.Fatal("stale owner renewed after expired-lease takeover")
	}
	if !backend.Action(ctx, LeaseRelease, stale) {
		t.Fatal("stale owner release was not idempotent")
	}
	statuses, err := backend.GetStatuses(ctx, name)
	if err != nil {
		t.Fatalf("query replacement owner: %v", err)
	}
	if statuses[name].Owner != replacement.Owner {
		t.Fatalf("stale release changed replacement owner: %#v", statuses[name])
	}
	if !backend.Action(ctx, LeaseRelease, replacement) {
		t.Fatal("replacement owner did not release lease")
	}
}

func testRealUnicodeAndLargeValues(t *testing.T, backend *sqlLease) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	name := strings.Repeat("界", 85)
	state := realLeaseState(name, "unicode-owner-世界")
	state.Log = "[integration]\n" + strings.Repeat("日志🙂", 16*1024)
	state.SuccessCount = math.MaxInt64
	state.FailureCount = math.MaxInt64 - 1
	if len(name) != maxRoutineNameBytes {
		t.Fatalf("invalid test name length %d", len(name))
	}
	if !backend.Action(ctx, LeaseAcquire, state) {
		t.Fatal("could not acquire lease with a 255-byte UTF-8 name and large log")
	}
	if !backend.Action(ctx, LeaseRelease, state) {
		t.Fatal("could not release lease with a 255-byte UTF-8 name and large log")
	}
	statuses, err := backend.GetStatuses(ctx, name)
	if err != nil {
		t.Fatalf("query Unicode status: %v", err)
	}
	if statuses[name].Name != name || statuses[name].Log != state.Log || statuses[name].SuccessCount != state.SuccessCount || statuses[name].FailureCount != state.FailureCount {
		t.Fatal("Unicode name, large log, or 64-bit counters did not round-trip")
	}
}

func testRealMassiveContention(t *testing.T, backend *sqlLease) {
	contenders, rounds := realContentionSize()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	name := integrationRoutineName(t, "massive-contention")

	for round := 0; round < rounds; round++ {
		start := make(chan struct{})
		var winnerCount atomic.Int64
		var winnerMu sync.Mutex
		winner := realLeaseState(name, "unset")
		var wait sync.WaitGroup
		wait.Add(contenders)
		for index := 0; index < contenders; index++ {
			state := realLeaseState(name, fmt.Sprintf("round-%d-owner-%d", round, index))
			go func() {
				defer wait.Done()
				<-start
				if backend.Action(ctx, LeaseAcquire, state) {
					winnerCount.Add(1)
					winnerMu.Lock()
					winner = state
					winnerMu.Unlock()
				}
			}()
		}
		close(start)
		wait.Wait()
		if got := winnerCount.Load(); got != 1 {
			t.Fatalf("round %d had %d acquisition winners among %d contenders", round, got, contenders)
		}
		if !backend.Action(ctx, LeaseRelease, winner) {
			t.Fatalf("release contention winner in round %d", round)
		}
	}
}

func testRealConcurrentHistoryPruning(t *testing.T, backend *sqlLease) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	name := integrationRoutineName(t, "history-pruning")
	owner := realLeaseState(name, "real-history-owner")
	if !backend.Action(ctx, LeaseAcquire, owner) {
		t.Fatal("acquire history pruning lease")
	}

	var wait sync.WaitGroup
	for index := 0; index < 80; index++ {
		wait.Add(1)
		state := realLeaseState(name, fmt.Sprintf("stale-owner-%d", index))
		go func() {
			defer wait.Done()
			if !backend.Action(ctx, LeaseRelease, state) {
				t.Errorf("idempotent stale release failed for %s", state.Owner)
			}
		}()
	}
	wait.Wait()
	if !backend.Action(ctx, LeaseRelease, owner) {
		t.Fatal("release history pruning owner")
	}
	history, err := backend.GetLogs(ctx, name)
	if err != nil {
		t.Fatalf("query pruned history: %v", err)
	}
	if got := len(history[name]); got != retainedLogsPerRoutineID {
		t.Fatalf("retained %d history records, want %d", got, retainedLogsPerRoutineID)
	}
}

func testRealBulkFilteredQueries(t *testing.T, backend *sqlLease) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	const count = sqlNameFilterBatchSize + 5
	base := integrationRoutineName(t, "bulk")
	names := make([]string, count)
	for index := range names {
		names[index] = fmt.Sprintf("%s-%04d", base, index)
		state := realLeaseState(names[index], fmt.Sprintf("bulk-owner-%04d", index))
		if !backend.Action(ctx, LeaseAcquire, state) {
			t.Fatalf("acquire bulk lease %d", index)
		}
	}
	filters := append(append([]string{}, names...), names[0], names[count-1])
	statuses, err := backend.GetStatuses(ctx, filters...)
	if err != nil {
		t.Fatalf("query %d filtered statuses: %v", count, err)
	}
	if len(statuses) != count {
		t.Fatalf("queried %d statuses, want %d", len(statuses), count)
	}
	history, err := backend.GetLogs(ctx, filters...)
	if err != nil {
		t.Fatalf("query %d filtered histories: %v", count, err)
	}
	if len(history) != count {
		t.Fatalf("queried history for %d routines, want %d", len(history), count)
	}
	for index, name := range names {
		wantOwner := fmt.Sprintf("bulk-owner-%04d", index)
		if status, exists := statuses[name]; !exists || status.Owner != wantOwner {
			t.Fatalf("missing or incorrect bulk status %q: %#v", name, status)
		}
		if len(history[name]) == 0 || history[name][0].Name != name {
			t.Fatalf("missing or incorrect bulk history %q: %#v", name, history[name])
		}
	}
}

func testRealSupervisorRepeatsAndRenews(t *testing.T, backend *sqlLease) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	coordinator := realTestCoordinator(backend)
	name := integrationRoutineName(t, "supervisor-renewal")
	started := make(chan struct{})
	var startsOnce sync.Once
	var attempts atomic.Int64
	handle := coordinator.startUniqueSupervisor(ctx, name, uniqueTask{
		Run: func(taskContext context.Context) {
			attempts.Add(1)
			startsOnce.Do(func() { close(started) })
			<-taskContext.Done()
		},
	}, func(Panic) {})
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("supervisor did not start")
	}

	deadline := time.Now().Add(7 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(300 * time.Millisecond)
		if backend.Action(ctx, LeaseAcquire, realLeaseState(name, "competing-owner")) {
			t.Fatal("competing owner acquired while the supervisor should be renewing")
		}
	}
	handle.Stop()
	waitForRealHandle(t, handle, 5*time.Second)
	if attempts.Load() != 1 {
		t.Fatalf("long-running supervisor started %d attempts, want 1", attempts.Load())
	}
}

func testRealSupervisorPanicState(t *testing.T, backend *sqlLease) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	coordinator := realTestCoordinator(backend)
	name := integrationRoutineName(t, "supervisor-panic")
	owner := "panic-owner"
	state := newSupervisorState()
	if !backend.Action(ctx, LeaseAcquire, state.snapshot(name, owner, coordinator.timing.ttl)) {
		t.Fatal("panic supervisor did not acquire lease")
	}
	stop, recovered := coordinator.runAsOwner(ctx, name, owner, uniqueTask{
		Run: func(context.Context) { panic("integration panic") },
	}, state)
	if stop || recovered == nil || recovered.Value != "integration panic" {
		t.Fatalf("unexpected panic result: stop=%t recovered=%#v", stop, recovered)
	}
	if !coordinator.release(name, owner, state) {
		t.Fatal("panic supervisor did not release lease")
	}
	statuses, err := backend.GetStatuses(ctx, name)
	if err != nil {
		t.Fatalf("query panic status: %v", err)
	}
	status := statuses[name]
	if status.Status != RoutinePanic || status.FailureCount != 1 || !strings.Contains(status.Log, "integration panic") {
		t.Fatalf("unexpected panic status: %#v", status)
	}
}

func testRealSupervisorCancelsOnLeaseLoss(t *testing.T, backend *sqlLease) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	coordinator := realTestCoordinator(backend)
	name := integrationRoutineName(t, "supervisor-lease-loss")
	started := make(chan struct{})
	canceled := make(chan struct{})
	handle := coordinator.startUniqueSupervisor(ctx, name, uniqueTask{
		Run: func(taskContext context.Context) {
			close(started)
			<-taskContext.Done()
			close(canceled)
		},
	}, func(Panic) {})
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("lease-loss supervisor did not start")
	}
	statuses, err := backend.GetStatuses(ctx, name)
	if err != nil {
		t.Fatalf("query lease-loss owner: %v", err)
	}
	owner := statuses[name].Owner
	if owner == "" {
		t.Fatal("lease-loss supervisor owner was not persisted")
	}
	if !backend.Action(ctx, LeaseRelease, realLeaseState(name, owner)) {
		t.Fatal("force release supervisor lease")
	}
	select {
	case <-canceled:
		cancel()
	case <-ctx.Done():
		t.Fatal("task was not canceled after ownership was lost")
	}
	waitForRealHandle(t, handle, 5*time.Second)
}

func testRealSupervisorHandoff(t *testing.T, backend *sqlLease) {
	workerCount := realSupervisorWorkerCount()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	coordinator := realTestCoordinator(backend)
	name := integrationRoutineName(t, "supervisor-handoff")
	var active atomic.Int64
	var maximumActive atomic.Int64
	var executed atomic.Int64
	handles := make([]*Handle, 0, workerCount)
	cancels := make([]context.CancelFunc, 0, workerCount)
	for index := 0; index < workerCount; index++ {
		workerContext, workerCancel := context.WithCancel(ctx)
		cancels = append(cancels, workerCancel)
		handle := coordinator.startUniqueSupervisor(workerContext, name, uniqueTask{
			Run: func(context.Context) {
				current := active.Add(1)
				for {
					maximum := maximumActive.Load()
					if current <= maximum || maximumActive.CompareAndSwap(maximum, current) {
						break
					}
				}
				time.Sleep(5 * time.Millisecond)
				active.Add(-1)
				executed.Add(1)
				workerCancel()
			},
		}, func(Panic) {})
		handles = append(handles, handle)
	}
	for _, handle := range handles {
		waitForRealHandle(t, handle, 90*time.Second)
	}
	for _, workerCancel := range cancels {
		workerCancel()
	}
	if got := executed.Load(); got != int64(workerCount) {
		t.Fatalf("executed %d supervisor owners, want %d", got, workerCount)
	}
	if got := maximumActive.Load(); got != 1 {
		t.Fatalf("observed %d simultaneously active supervisors, want 1", got)
	}
}

func testRealParallelDistinctSupervisors(t *testing.T, backend *sqlLease) {
	nameCount := realParallelNameCount()
	replicasPerName := realParallelReplicaCount()
	if nameCount < 4 {
		t.Fatal("parallel distinct-name test requires at least four names")
	}
	if replicasPerName < 2 {
		t.Fatal("parallel distinct-name test requires at least two replicas per name")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	coordinator := realParallelCoordinator(backend)
	base := integrationRoutineName(t, "parallel-distinct")
	names := make([]string, nameCount)
	activeByName := make([]atomic.Int64, nameCount)
	startsByName := make([]atomic.Int64, nameCount)
	handles := make([]*Handle, 0, nameCount*replicasPerName)
	finishWork := make(chan struct{})
	allStarted := make(chan struct{})
	var allStartedOnce sync.Once
	var firstStarts atomic.Int64
	var totalActive atomic.Int64
	var maximumActive atomic.Int64
	var overlapDetected atomic.Bool

	for nameIndex := 0; nameIndex < nameCount; nameIndex++ {
		names[nameIndex] = realParallelRoutineName(base, nameIndex)
		for range replicasPerName {
			index := nameIndex
			handle := coordinator.startUniqueSupervisor(ctx, names[index], uniqueTask{
				Run: func(taskContext context.Context) {
					starts := startsByName[index].Add(1)
					activeForName := activeByName[index].Add(1)
					activeTotal := totalActive.Add(1)
					updateAtomicMaximum(&maximumActive, activeTotal)
					if starts != 1 || activeForName != 1 {
						overlapDetected.Store(true)
					}
					if starts == 1 && firstStarts.Add(1) == int64(nameCount) {
						allStartedOnce.Do(func() { close(allStarted) })
					}
					select {
					case <-finishWork:
					case <-taskContext.Done():
					}
					totalActive.Add(-1)
					activeByName[index].Add(-1)
				},
				RepeatAfter: time.Hour,
			}, func(Panic) {})
			handles = append(handles, handle)
		}
	}

	select {
	case <-allStarted:
	case <-ctx.Done():
		t.Fatalf("only %d of %d distinct routines started: %v", firstStarts.Load(), nameCount, ctx.Err())
	}
	if overlapDetected.Load() {
		t.Fatal("multiple supervisors ran simultaneously for at least one distinct routine name")
	}
	if got := totalActive.Load(); got != int64(nameCount) {
		t.Fatalf("simultaneously active distinct routines = %d, want %d", got, nameCount)
	}
	if got := maximumActive.Load(); got != int64(nameCount) {
		t.Fatalf("maximum parallel distinct routines = %d, want %d", got, nameCount)
	}

	runningStatuses := waitForRealStatuses(t, ctx, backend, names, "running", func(statuses SupervisorStatuses) bool {
		if len(statuses) != nameCount {
			return false
		}
		for _, name := range names {
			status := statuses[name]
			if status.Name != name || status.Status != RoutineRunning || status.Owner == "" || status.SuccessCount != 0 || status.FailureCount != 0 || status.Log != serverLog("") || status.UpdatedAt <= 0 || status.ExpiresAt <= status.UpdatedAt {
				return false
			}
		}
		return true
	})
	owners := make(map[string]string, nameCount)
	seenOwners := make(map[string]string, nameCount)
	for _, name := range names {
		owner := runningStatuses[name].Owner
		if previousName, exists := seenOwners[owner]; exists {
			t.Fatalf("owner %q was shared by distinct names %q and %q", owner, previousName, name)
		}
		owners[name] = owner
		seenOwners[owner] = name
	}

	close(finishWork)
	// A normal task return retains its lease during RepeatAfter, so standby
	// replicas cannot take over while the final done snapshots are observed.
	waitForRealStatuses(t, ctx, backend, names, "done", func(statuses SupervisorStatuses) bool {
		if len(statuses) != nameCount {
			return false
		}
		for _, name := range names {
			status := statuses[name]
			if status.Status != RoutineDone || status.SuccessCount != 1 || status.FailureCount != 0 || status.Owner != owners[name] {
				return false
			}
		}
		return true
	})
	cancel()
	waitForRealHandles(t, handles, 30*time.Second)

	queryCtx, queryCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer queryCancel()
	statuses, err := backend.GetStatuses(queryCtx, names...)
	if err != nil {
		t.Fatalf("query final parallel statuses: %v", err)
	}
	history, err := backend.GetLogs(queryCtx, names...)
	if err != nil {
		t.Fatalf("query parallel histories: %v", err)
	}
	if len(statuses) != nameCount || len(history) != nameCount {
		t.Fatalf("parallel result sizes: statuses=%d history=%d want=%d", len(statuses), len(history), nameCount)
	}
	for index, name := range names {
		if starts := startsByName[index].Load(); starts != 1 {
			t.Fatalf("routine %q started %d times, want 1", name, starts)
		}
		status := statuses[name]
		if status.Name != name || status.Owner != owners[name] || status.Status != RoutineDone || status.SuccessCount != 1 || status.FailureCount != 0 || status.Log != serverLog("") || !realTimestampsClose(status.ExpiresAt, status.UpdatedAt) {
			t.Fatalf("incorrect final parallel status for %q: %#v", name, status)
		}
		if len(history[name]) != 2 {
			t.Fatalf("parallel history for %q has %d records, want acquire and release", name, len(history[name]))
		}
		var acquired, released bool
		for _, log := range history[name] {
			if log.ID == "" || log.Name != name || log.Owner != owners[name] || log.CreatedAt <= 0 {
				t.Fatalf("crossed parallel history for %q: %#v", name, log)
			}
			switch log.Action {
			case LeaseAcquire:
				if log.Status != RoutineNotStarted {
					t.Fatalf("parallel acquire history for %q has status %q", name, log.Status)
				}
				acquired = true
			case LeaseRelease:
				if log.Status != RoutineDone {
					t.Fatalf("parallel release history for %q has status %q", name, log.Status)
				}
				released = true
			default:
				t.Fatalf("unexpected parallel history action for %q: %#v", name, log)
			}
		}
		if !acquired || !released {
			t.Fatalf("parallel history for %q lacks acquire or release: %#v", name, history[name])
		}
	}
}

func waitForRealStatuses(t *testing.T, ctx context.Context, backend *sqlLease, names []string, description string, ready func(SupervisorStatuses) bool) SupervisorStatuses {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastStatuses SupervisorStatuses
	var lastErr error
	for {
		lastStatuses, lastErr = backend.GetStatuses(ctx, names...)
		if lastErr == nil && ready(lastStatuses) {
			return lastStatuses
		}
		select {
		case <-ctx.Done():
			t.Fatalf("parallel statuses did not become %s: %v; last error=%v statuses=%#v", description, ctx.Err(), lastErr, lastStatuses)
		case <-ticker.C:
		}
	}
}

func waitForRealHandles(t *testing.T, handles []*Handle, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for index, handle := range handles {
		select {
		case <-handle.Done():
		case <-ctx.Done():
			for _, remaining := range handles[index:] {
				remaining.Stop()
			}
			t.Fatalf("%d parallel supervisor handles did not finish within %s", len(handles)-index, timeout)
		}
	}
}

func updateAtomicMaximum(maximum *atomic.Int64, candidate int64) {
	for {
		current := maximum.Load()
		if candidate <= current || maximum.CompareAndSwap(current, candidate) {
			return
		}
	}
}

func realTimestampsClose(left, right int64) bool {
	if left <= 0 || right <= 0 {
		return false
	}
	difference := left - right
	return difference >= -1 && difference <= 1
}

func realParallelRoutineName(base string, index int) string {
	switch index {
	case 0:
		return base + "-identity"
	case 1:
		return base + "-IDENTITY"
	case 2:
		return base + "-caf\u00e9"
	case 3:
		return base + "-cafe\u0301"
	}
	switch index % 4 {
	case 0:
		return fmt.Sprintf("%s-lower-%03d", base, index)
	case 1:
		return fmt.Sprintf("%s-UPPER-%03d", base, index)
	case 2:
		return fmt.Sprintf("%s-世界-%03d", base, index)
	default:
		return fmt.Sprintf("%s.dot_%03d", base, index)
	}
}

func runRealLeaseWorker(t *testing.T, backend *sqlLease) {
	lockPath := os.Getenv("EASYROUTINE_TEST_LOCK_PATH")
	if lockPath == "" {
		t.Fatal("worker lock path is required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	coordinator := realTestCoordinator(backend)
	var workerError atomic.Value
	var ran atomic.Bool
	handle := coordinator.startUniqueSupervisor(ctx, "integration-process-contention", uniqueTask{
		Run: func(context.Context) {
			ran.Store(true)
			lock, err := os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
			if err != nil {
				workerError.Store(fmt.Errorf("overlapping process task: %w", err))
				cancel()
				return
			}
			time.Sleep(100 * time.Millisecond)
			if closeErr := lock.Close(); closeErr != nil {
				workerError.Store(fmt.Errorf("close process lock: %w", closeErr))
			}
			if removeErr := os.Remove(lockPath); removeErr != nil {
				workerError.Store(fmt.Errorf("remove process lock: %w", removeErr))
			}
			cancel()
		},
	}, func(Panic) {})
	waitForRealHandle(t, handle, 2*time.Minute)
	if !ran.Load() {
		t.Fatal("process worker never held the lease")
	}
	if value := workerError.Load(); value != nil {
		t.Fatal(value)
	}
}

func runRealWorkers(t *testing.T, count int, extraEnvironment map[string]string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("locate integration test executable: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	type workerResult struct {
		index  int
		output string
		err    error
	}
	results := make(chan workerResult, count)
	start := make(chan struct{})
	for index := 0; index < count; index++ {
		go func(workerIndex int) {
			<-start
			command := exec.CommandContext(ctx, executable, "-test.run=^TestRealSQLProcessWorker$", "-test.timeout=150s")
			commandEnvironment := append([]string{}, os.Environ()...)
			for key, value := range extraEnvironment {
				commandEnvironment = append(commandEnvironment, key+"="+value)
			}
			command.Env = commandEnvironment
			output, commandErr := command.CombinedOutput()
			results <- workerResult{index: workerIndex, output: string(output), err: commandErr}
		}(index)
	}
	close(start)
	for range count {
		result := <-results
		if result.err != nil {
			t.Errorf("integration worker %d failed: %v\n%s", result.index, result.err, result.output)
		}
	}
	if ctx.Err() != nil {
		t.Fatalf("integration workers did not finish: %v", ctx.Err())
	}
}

func realLeaseState(name, owner string) leaseState {
	return leaseState{
		Name:   name,
		Owner:  owner,
		TTL:    5 * time.Second,
		Status: RoutineRunning,
		Log:    "[integration]",
	}
}

func realTestCoordinator(backend *sqlLease) *coordinator {
	return &coordinator{
		backend: backend,
		timing: leaseTiming{
			ttl:       5 * time.Second,
			heartbeat: 250 * time.Millisecond,
			release:   3 * time.Second,
		},
	}
}

func realParallelCoordinator(backend *sqlLease) *coordinator {
	return &coordinator{
		backend: backend,
		timing: leaseTiming{
			ttl:       20 * time.Second,
			heartbeat: 2 * time.Second,
			release:   5 * time.Second,
		},
	}
}

func integrationRoutineName(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("integration-%s-%d", suffix, time.Now().UnixNano())
}

func waitForRealHandle(t *testing.T, handle *Handle, timeout time.Duration) {
	t.Helper()
	select {
	case <-handle.Done():
	case <-time.After(timeout):
		handle.Stop()
		t.Fatalf("supervisor did not finish within %s", timeout)
	}
}

func realContentionSize() (int, int) {
	contenders := realPositiveEnvironmentInt("EASYROUTINE_TEST_CONTENDERS", 128)
	rounds := realPositiveEnvironmentInt("EASYROUTINE_TEST_CONTENTION_ROUNDS", 6)
	return contenders, rounds
}

func realSupervisorWorkerCount() int {
	return realPositiveEnvironmentInt("EASYROUTINE_TEST_SUPERVISORS", 32)
}

func realProcessWorkerCount() int {
	return realPositiveEnvironmentInt("EASYROUTINE_TEST_PROCESSES", 12)
}

func realParallelNameCount() int {
	return realPositiveEnvironmentInt("EASYROUTINE_TEST_PARALLEL_NAMES", 32)
}

func realParallelReplicaCount() int {
	return realPositiveEnvironmentInt("EASYROUTINE_TEST_PARALLEL_REPLICAS", 3)
}

func realPositiveEnvironmentInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		panic(errors.New(name + " must be a positive integer"))
	}
	return parsed
}
