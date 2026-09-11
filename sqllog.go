package EasyRoutine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const retainedLogsPerRoutineID = 25

// Keep each filter below Oracle's IN-list and older SQLite variable limits.
const sqlNameFilterBatchSize = 900

type sqlQueryContext func(context.Context, string, ...any) (*sql.Rows, error)

type sqlLogStatements struct {
	create           string
	createIndex      string
	insert           string
	prune            string
	selectRoutineIDs string
	selectBase       string
	routineIDColumn  string
	bindVariable     func(int) string
}

// Action atomically applies ownership and current state. Acquire and release
// actions also record a best-effort history snapshot.
func (s *sqlLease) Action(ctx context.Context, action LeaseAction, state leaseState) bool {
	ttlArgument, valid := s.validateAction(ctx, action, state)
	if !valid {
		return false
	}

	var applied bool
	switch action {
	case LeaseAcquire:
		applied = s.acquireState(ctx, state, ttlArgument)
	case LeaseRenew:
		applied = s.renewState(ctx, state, ttlArgument)
	case LeaseRelease:
		applied = s.releaseState(ctx, state)
	}
	if applied && action != LeaseRenew {
		s.recordLog(ctx, action, state)
	}
	return applied
}

// GetLogs returns retained records grouped by exact routine name. Each history
// is ordered oldest first. With no names it returns logs for every task.
func (s *sqlLease) GetLogs(ctx context.Context, names ...string) (SupervisorHistory, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || s.db == nil || s.query == nil {
		return nil, errors.New("SQL lease is not initialized")
	}
	routineIDs := uniqueRoutineIDs(names)
	if len(names) == 0 {
		var err error
		routineIDs, err = s.queryLogRoutineIDs(ctx)
		if err != nil {
			return nil, err
		}
	}
	history := make(SupervisorHistory)
	if len(routineIDs) == 0 {
		return history, nil
	}

	for start := 0; start < len(routineIDs); start += sqlNameFilterBatchSize {
		end := min(start+sqlNameFilterBatchSize, len(routineIDs))
		query, args := filteredSQLQuery(
			s.logs.selectBase,
			s.logs.routineIDColumn,
			s.logs.bindVariable,
			routineIDs[start:end],
		)
		batch, err := s.queryLogs(ctx, query, args)
		if err != nil {
			return nil, err
		}
		for name, logs := range batch {
			history[name] = append(history[name], logs...)
		}
	}
	for name := range history {
		logs := history[name]
		sort.SliceStable(logs, func(left, right int) bool {
			return logs[left].CreatedAt < logs[right].CreatedAt
		})
	}
	return history, nil
}

func (s *sqlLease) queryLogRoutineIDs(ctx context.Context) ([]string, error) {
	rows, err := s.query(ctx, s.logs.selectRoutineIDs)
	if err != nil {
		return nil, fmt.Errorf("query supervisor log routine IDs: %w", err)
	}
	defer rows.Close()

	routineIDs := make([]string, 0)
	for rows.Next() {
		var routineID string
		if err := rows.Scan(&routineID); err != nil {
			return nil, fmt.Errorf("scan supervisor log routine ID: %w", err)
		}
		routineIDs = append(routineIDs, routineID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate supervisor log routine IDs: %w", err)
	}
	return routineIDs, nil
}

func (s *sqlLease) queryLogs(ctx context.Context, query string, args []any) (SupervisorHistory, error) {
	rows, err := s.query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query supervisor logs: %w", err)
	}
	defer rows.Close()

	history := make(SupervisorHistory)
	for rows.Next() {
		var (
			log     SupervisorLog
			action  string
			status  string
			logText sql.NullString
		)
		if err := rows.Scan(
			&log.ID,
			&log.Name,
			&log.Owner,
			&action,
			&status,
			&logText,
			&log.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan supervisor log: %w", err)
		}
		log.Action = LeaseAction(action)
		log.Status = RoutineStatus(status)
		log.Log = logText.String
		history[log.Name] = append(history[log.Name], log)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate supervisor logs: %w", err)
	}
	return history, nil
}

// GetStatuses returns one current state per exact routine name. With no names it
// returns every current state; otherwise it filters by the supplied names.
func (s *sqlLease) GetStatuses(ctx context.Context, names ...string) (SupervisorStatuses, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || s.db == nil || s.query == nil {
		return nil, errors.New("SQL lease is not initialized")
	}
	routineIDs := uniqueRoutineIDs(names)

	if len(routineIDs) == 0 {
		return s.queryStatuses(ctx, s.statements.selectBase, nil)
	}

	statuses := make(SupervisorStatuses, len(routineIDs))
	for start := 0; start < len(routineIDs); start += sqlNameFilterBatchSize {
		end := min(start+sqlNameFilterBatchSize, len(routineIDs))
		query, args := filteredSQLQuery(
			s.statements.selectBase,
			s.statements.routineIDColumn,
			s.statements.bindVariable,
			routineIDs[start:end],
		)
		batch, err := s.queryStatuses(ctx, query, args)
		if err != nil {
			return nil, err
		}
		for name, status := range batch {
			statuses[name] = status
		}
	}
	return statuses, nil
}

func (s *sqlLease) queryStatuses(ctx context.Context, query string, args []any) (SupervisorStatuses, error) {
	rows, err := s.query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query supervisor statuses: %w", err)
	}
	defer rows.Close()

	statuses := make(SupervisorStatuses)
	for rows.Next() {
		var (
			status     SupervisorStatus
			statusName string
			logText    sql.NullString
		)
		if err := rows.Scan(
			&status.Name,
			&status.Owner,
			&statusName,
			&status.SuccessCount,
			&status.FailureCount,
			&logText,
			&status.ExpiresAt,
			&status.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan supervisor status: %w", err)
		}
		status.Status = RoutineStatus(statusName)
		status.Log = logText.String
		statuses[status.Name] = status
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate supervisor statuses: %w", err)
	}
	return statuses, nil
}

func uniqueRoutineIDs(names []string) []string {
	if len(names) == 0 {
		return nil
	}

	routineIDs := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		id := routineID(name)
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		routineIDs = append(routineIDs, id)
	}
	return routineIDs
}

func filteredSQLQuery(selectBase, routineIDColumn string, bindVariable func(int) string, routineIDs []string) (string, []any) {
	variables := make([]string, len(routineIDs))
	args := make([]any, len(routineIDs))
	for index, id := range routineIDs {
		variables[index] = bindVariable(index + 1)
		args[index] = id
	}
	query := selectBase + " WHERE " + routineIDColumn + " IN (" + strings.Join(variables, ", ") + ")"
	return query, args
}

func (s *sqlLease) validateAction(ctx context.Context, action LeaseAction, state leaseState) (int64, bool) {
	if ctx == nil || ctx.Err() != nil || s == nil || s.db == nil || state.Owner == "" {
		return 0, false
	}
	if validateRoutineName(state.Name) != nil {
		return 0, false
	}
	if state.SuccessCount < 0 || state.FailureCount < 0 || !validRoutineStatus(state.Status) {
		return 0, false
	}
	switch action {
	case LeaseAcquire, LeaseRenew:
		if s.statements.ttlArgument == nil {
			return 0, false
		}
		return s.statements.ttlArgument(state.TTL)
	case LeaseRelease:
		return 0, true
	default:
		return 0, false
	}
}

func validRoutineStatus(status RoutineStatus) bool {
	switch status {
	case RoutineNotStarted, RoutineRunning, RoutineDone, RoutinePanic:
		return true
	default:
		return false
	}
}

func (s *sqlLease) recordLog(ctx context.Context, action LeaseAction, state leaseState) {
	defer func() {
		_ = recover()
	}()

	_, err := s.db.ExecContext(
		ctx,
		s.logs.insert,
		rand.Text(),
		routineID(state.Name),
		state.Name,
		state.Owner,
		string(action),
		string(state.Status),
		state.Log,
	)
	if err != nil {
		return
	}
	_, _ = s.db.ExecContext(ctx, s.logs.prune, routineID(state.Name))
}

func statementsForSQLLogDialect(dialect SQLDialect) (sqlLogStatements, error) {
	switch dialect {
	case SQLPostgreSQL, SQLGaussDB:
		return postgreSQLLogStatements, nil
	case SQLMySQL, SQLMariaDB, SQLTiDB:
		return mySQLLogStatements, nil
	case SQLSQLite:
		return sqliteLogStatements, nil
	case SQLServer:
		return sqlServerLogStatements, nil
	case SQLOracle:
		return oracleLogStatements, nil
	default:
		return sqlLogStatements{}, fmt.Errorf("unsupported SQL dialect %q", dialect)
	}
}

func dollarVariable(index int) string    { return fmt.Sprintf("$%d", index) }
func questionVariable(int) string        { return "?" }
func sqlServerVariable(index int) string { return fmt.Sprintf("@p%d", index) }
func oracleVariable(index int) string    { return fmt.Sprintf(":%d", index) }

var postgreSQLLogStatements = sqlLogStatements{
	create: `CREATE TABLE IF NOT EXISTS "unique_routine_log" (
    "id" VARCHAR(64) PRIMARY KEY,
	"routine_id" VARCHAR(64) NOT NULL,
    "name" TEXT NOT NULL,
    "owner" TEXT NOT NULL,
    "action" VARCHAR(16) NOT NULL,
    "status" VARCHAR(32) NOT NULL,
    "log" TEXT NOT NULL,
	"created_at" BIGINT NOT NULL
)`,
	createIndex: `CREATE INDEX IF NOT EXISTS "unique_routine_log_routine_id" ON "unique_routine_log" ("routine_id")`,
	insert: `INSERT INTO "unique_routine_log" ("id", "routine_id", "name", "owner", "action", "status", "log", "created_at")
VALUES ($1, $2, $3, $4, $5, $6, $7, ` + postgreSQLCurrentSeconds + `)`,
	prune: fmt.Sprintf(`DELETE FROM "unique_routine_log"
WHERE "id" IN (
    SELECT "id" FROM (
        SELECT "id", ROW_NUMBER() OVER (ORDER BY "created_at" DESC, "id" DESC) AS "row_number"
        FROM "unique_routine_log"
		WHERE "routine_id" = $1
    ) AS "ranked"
		WHERE "row_number" > %d
	)`, retainedLogsPerRoutineID),
	selectRoutineIDs: `SELECT DISTINCT "routine_id" FROM "unique_routine_log"`,
	selectBase:       `SELECT "id", "name", "owner", "action", "status", "log", "created_at" FROM "unique_routine_log"`,
	routineIDColumn:  `"routine_id"`,
	bindVariable:     dollarVariable,
}

var mySQLLogStatements = sqlLogStatements{
	create: `CREATE TABLE IF NOT EXISTS ` + "`unique_routine_log`" + ` (
    ` + "`id`" + ` VARCHAR(64) NOT NULL PRIMARY KEY,
	` + "`routine_id`" + ` VARCHAR(64) NOT NULL,
    ` + "`name`" + ` VARCHAR(255) NOT NULL,
    ` + "`owner`" + ` VARCHAR(128) NOT NULL,
    ` + "`action`" + ` VARCHAR(16) NOT NULL,
    ` + "`status`" + ` VARCHAR(32) NOT NULL,
    ` + "`log`" + ` LONGTEXT NOT NULL,
	` + "`created_at`" + ` BIGINT NOT NULL,
	INDEX ` + "`unique_routine_log_routine_id`" + ` (` + "`routine_id`" + `)
)`,
	insert: `INSERT INTO ` + "`unique_routine_log`" + ` (` + "`id`" + `, ` + "`routine_id`" + `, ` + "`name`" + `, ` + "`owner`" + `, ` + "`action`" + `, ` + "`status`" + `, ` + "`log`" + `, ` + "`created_at`" + `)
VALUES (?, ?, ?, ?, ?, ?, ?, ` + mySQLCurrentSeconds + `)`,
	prune: fmt.Sprintf(`DELETE FROM `+"`unique_routine_log`"+`
WHERE `+"`id`"+` IN (
    SELECT `+"`id`"+` FROM (
        SELECT `+"`id`"+`, ROW_NUMBER() OVER (ORDER BY `+"`created_at`"+` DESC, `+"`id`"+` DESC) AS `+"`row_number`"+`
        FROM `+"`unique_routine_log`"+`
		WHERE `+"`routine_id`"+` = ?
    ) AS `+"`ranked`"+`
		WHERE `+"`row_number`"+` > %d
	)`, retainedLogsPerRoutineID),
	selectRoutineIDs: `SELECT DISTINCT ` + "`routine_id`" + ` FROM ` + "`unique_routine_log`",
	selectBase:       `SELECT ` + "`id`" + `, ` + "`name`" + `, ` + "`owner`" + `, ` + "`action`" + `, ` + "`status`" + `, ` + "`log`" + `, ` + "`created_at`" + ` FROM ` + "`unique_routine_log`",
	routineIDColumn:  "`routine_id`",
	bindVariable:     questionVariable,
}

var sqliteLogStatements = sqlLogStatements{
	create: `CREATE TABLE IF NOT EXISTS "unique_routine_log" (
    "id" TEXT PRIMARY KEY,
	"routine_id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "owner" TEXT NOT NULL,
    "action" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "log" TEXT NOT NULL,
    "created_at" INTEGER NOT NULL
) WITHOUT ROWID`,
	createIndex: `CREATE INDEX IF NOT EXISTS "unique_routine_log_routine_id" ON "unique_routine_log" ("routine_id")`,
	insert: `INSERT INTO "unique_routine_log" ("id", "routine_id", "name", "owner", "action", "status", "log", "created_at")
VALUES (?, ?, ?, ?, ?, ?, ?, ` + sqliteCurrentSeconds + `)`,
	prune: fmt.Sprintf(`DELETE FROM "unique_routine_log"
WHERE "id" IN (
    SELECT "id" FROM (
        SELECT "id", ROW_NUMBER() OVER (ORDER BY "created_at" DESC, "id" DESC) AS "row_number"
        FROM "unique_routine_log"
		WHERE "routine_id" = ?
    ) AS "ranked"
		WHERE "row_number" > %d
	)`, retainedLogsPerRoutineID),
	selectRoutineIDs: `SELECT DISTINCT "routine_id" FROM "unique_routine_log"`,
	selectBase:       `SELECT "id", "name", "owner", "action", "status", "log", "created_at" FROM "unique_routine_log"`,
	routineIDColumn:  `"routine_id"`,
	bindVariable:     questionVariable,
}

var sqlServerLogStatements = sqlLogStatements{
	create: `BEGIN TRY
    IF OBJECT_ID(N'unique_routine_log', N'U') IS NULL
    BEGIN
        CREATE TABLE [unique_routine_log] (
            [id] NVARCHAR(64) NOT NULL PRIMARY KEY,
			[routine_id] VARCHAR(64) NOT NULL,
            [name] NVARCHAR(255) NOT NULL,
            [owner] NVARCHAR(128) NOT NULL,
            [action] NVARCHAR(16) NOT NULL,
            [status] NVARCHAR(32) NOT NULL,
            [log] NVARCHAR(MAX) NOT NULL,
			[created_at] BIGINT NOT NULL
        )
    END
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() <> 2714
        THROW;
END CATCH`,
	createIndex: `BEGIN TRY
	IF NOT EXISTS (
		SELECT 1 FROM sys.indexes
		WHERE [name] = N'unique_routine_log_routine_id'
		  AND [object_id] = OBJECT_ID(N'unique_routine_log')
	)
	BEGIN
		CREATE INDEX [unique_routine_log_routine_id] ON [unique_routine_log] ([routine_id])
	END
END TRY
BEGIN CATCH
	IF ERROR_NUMBER() <> 1913
		THROW;
END CATCH`,
	insert: `INSERT INTO [unique_routine_log] ([id], [routine_id], [name], [owner], [action], [status], [log], [created_at])
VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7, ` + sqlServerCurrentSeconds + `)`,
	prune: fmt.Sprintf(`DELETE FROM [unique_routine_log]
WHERE [id] IN (
    SELECT [id] FROM (
        SELECT [id], ROW_NUMBER() OVER (ORDER BY [created_at] DESC, [id] DESC) AS [row_number]
        FROM [unique_routine_log]
		WHERE [routine_id] = @p1
    ) AS [ranked]
		WHERE [row_number] > %d
	)`, retainedLogsPerRoutineID),
	selectRoutineIDs: `SELECT DISTINCT [routine_id] FROM [unique_routine_log]`,
	selectBase:       `SELECT [id], [name], [owner], [action], [status], [log], [created_at] FROM [unique_routine_log]`,
	routineIDColumn:  `[routine_id]`,
	bindVariable:     sqlServerVariable,
}

var oracleLogStatements = sqlLogStatements{
	create: `DECLARE
	table_count PLS_INTEGER;
BEGIN
	EXECUTE IMMEDIATE 'CREATE TABLE "unique_routine_log" ("id" VARCHAR2(64) NOT NULL, "routine_id" VARCHAR2(64) NOT NULL, "name" VARCHAR2(255) NOT NULL, "owner" VARCHAR2(128) NOT NULL, "action" VARCHAR2(16) NOT NULL, "status" VARCHAR2(32) NOT NULL, "log" CLOB, "created_at" NUMBER(19) NOT NULL, CONSTRAINT "unique_routine_log_pk" PRIMARY KEY ("id"))';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -955 THEN
            RAISE;
        END IF;
		SELECT COUNT(*) INTO table_count FROM USER_TABLES WHERE TABLE_NAME = 'unique_routine_log';
		IF table_count = 0 THEN
			RAISE;
		END IF;
END;`,
	createIndex: `DECLARE
	index_count PLS_INTEGER;
BEGIN
	BEGIN
		EXECUTE IMMEDIATE 'CREATE INDEX "unique_routine_log_routine_id" ON "unique_routine_log" ("routine_id")';
	EXCEPTION
		WHEN OTHERS THEN
			IF SQLCODE != -955 THEN
				RAISE;
			END IF;
			SELECT COUNT(*) INTO index_count FROM USER_INDEXES WHERE INDEX_NAME = 'unique_routine_log_routine_id';
			IF index_count = 0 THEN
				RAISE;
			END IF;
	END;
END;`,
	insert: `INSERT INTO "unique_routine_log" ("id", "routine_id", "name", "owner", "action", "status", "log", "created_at")
VALUES (:1, :2, :3, :4, :5, :6, :7, ` + oracleCurrentSeconds + `)`,
	prune: fmt.Sprintf(`DELETE FROM "unique_routine_log"
WHERE "id" IN (
    SELECT "id" FROM (
        SELECT "id", ROW_NUMBER() OVER (ORDER BY "created_at" DESC, "id" DESC) AS "row_number"
        FROM "unique_routine_log"
		WHERE "routine_id" = :1
    )
		WHERE "row_number" > %d
	)`, retainedLogsPerRoutineID),
	selectRoutineIDs: `SELECT DISTINCT "routine_id" FROM "unique_routine_log"`,
	selectBase:       `SELECT "id", "name", "owner", "action", "status", "log", "created_at" FROM "unique_routine_log"`,
	routineIDColumn:  `"routine_id"`,
	bindVariable:     oracleVariable,
}
