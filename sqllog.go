package EasyRoutine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const retainedLogsPerRoutineID = 25

// Keep each filter below Oracle's IN-list and older SQLite variable limits.
const sqlNameFilterBatchSize = 900

type sqlQueryContext func(context.Context, string, ...any) (*sql.Rows, error)

type sqlLogStatements struct {
	create          string
	createIndex     string
	insert          string
	prune           string
	selectBase      string
	routineIDColumn string
	orderBy         string
	bindVariable    func(int) string
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

// GetLogs returns retained records newest first. With no names it returns logs
// for every task; otherwise it returns only records matching the supplied names.
func (s *sqlLease) GetLogs(ctx context.Context, names ...string) ([]SupervisorLog, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || s.db == nil || s.query == nil {
		return nil, errors.New("SQL lease is not initialized")
	}
	routineIDs, err := uniqueRoutineIDs(names)
	if err != nil {
		return nil, err
	}

	if len(routineIDs) == 0 {
		return s.queryLogs(ctx, s.logs.selectBase+s.logs.orderBy, nil)
	}

	logs := make([]SupervisorLog, 0)
	for start := 0; start < len(routineIDs); start += sqlNameFilterBatchSize {
		end := min(start+sqlNameFilterBatchSize, len(routineIDs))
		query, args := filteredSQLQuery(
			s.logs.selectBase,
			s.logs.routineIDColumn,
			s.logs.orderBy,
			s.logs.bindVariable,
			routineIDs[start:end],
		)
		batch, err := s.queryLogs(ctx, query, args)
		if err != nil {
			return nil, err
		}
		logs = append(logs, batch...)
	}
	if len(routineIDs) > sqlNameFilterBatchSize {
		sort.Slice(logs, func(left, right int) bool {
			if logs[left].CreatedAt.Equal(logs[right].CreatedAt) {
				return logs[left].ID > logs[right].ID
			}
			return logs[left].CreatedAt.After(logs[right].CreatedAt)
		})
	}
	return logs, nil
}

func (s *sqlLease) queryLogs(ctx context.Context, query string, args []any) ([]SupervisorLog, error) {
	rows, err := s.query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query supervisor logs: %w", err)
	}
	defer rows.Close()

	logs := make([]SupervisorLog, 0)
	for rows.Next() {
		var (
			log       SupervisorLog
			action    string
			status    string
			logText   sql.NullString
			createdAt any
		)
		if err := rows.Scan(
			&log.ID,
			&log.Name,
			&log.Owner,
			&action,
			&status,
			&logText,
			&createdAt,
		); err != nil {
			return nil, fmt.Errorf("scan supervisor log: %w", err)
		}
		log.Action = LeaseAction(action)
		log.Status = RoutineStatus(status)
		log.Log = logText.String
		log.CreatedAt, err = parseSQLLogTime(createdAt)
		if err != nil {
			return nil, fmt.Errorf("scan supervisor log timestamp: %w", err)
		}
		logs = append(logs, log)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate supervisor logs: %w", err)
	}
	return logs, nil
}

// GetStatuses returns one current state per task ordered by name. With no names
// it returns every current state; otherwise it filters by the supplied names.
func (s *sqlLease) GetStatuses(ctx context.Context, names ...string) ([]SupervisorStatus, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || s.db == nil || s.query == nil {
		return nil, errors.New("SQL lease is not initialized")
	}
	routineIDs, err := uniqueRoutineIDs(names)
	if err != nil {
		return nil, err
	}

	if len(routineIDs) == 0 {
		return s.queryStatuses(ctx, s.statements.selectBase+s.statements.orderBy, nil)
	}

	statuses := make([]SupervisorStatus, 0, len(routineIDs))
	for start := 0; start < len(routineIDs); start += sqlNameFilterBatchSize {
		end := min(start+sqlNameFilterBatchSize, len(routineIDs))
		query, args := filteredSQLQuery(
			s.statements.selectBase,
			s.statements.routineIDColumn,
			s.statements.orderBy,
			s.statements.bindVariable,
			routineIDs[start:end],
		)
		batch, err := s.queryStatuses(ctx, query, args)
		if err != nil {
			return nil, err
		}
		statuses = append(statuses, batch...)
	}
	if len(routineIDs) > sqlNameFilterBatchSize {
		sort.Slice(statuses, func(left, right int) bool {
			return statuses[left].Name < statuses[right].Name
		})
	}
	return statuses, nil
}

func (s *sqlLease) queryStatuses(ctx context.Context, query string, args []any) ([]SupervisorStatus, error) {
	rows, err := s.query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query supervisor statuses: %w", err)
	}
	defer rows.Close()

	statuses := make([]SupervisorStatus, 0)
	for rows.Next() {
		var (
			status     SupervisorStatus
			statusName string
			logText    sql.NullString
			expiresAt  any
			updatedAt  any
		)
		if err := rows.Scan(
			&status.Name,
			&status.Owner,
			&statusName,
			&status.SuccessCount,
			&status.FailureCount,
			&logText,
			&expiresAt,
			&updatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan supervisor status: %w", err)
		}
		status.Status = RoutineStatus(statusName)
		status.Log = logText.String
		status.ExpiresAt, err = parseSQLLogTime(expiresAt)
		if err != nil {
			return nil, fmt.Errorf("scan supervisor status expiry: %w", err)
		}
		status.UpdatedAt, err = parseSQLLogTime(updatedAt)
		if err != nil {
			return nil, fmt.Errorf("scan supervisor status update time: %w", err)
		}
		statuses = append(statuses, status)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate supervisor statuses: %w", err)
	}
	return statuses, nil
}

func uniqueRoutineIDs(names []string) ([]string, error) {
	if len(names) == 0 {
		return nil, nil
	}

	routineIDs := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if err := validateRoutineName(name); err != nil {
			return nil, err
		}
		id := routineID(name)
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		routineIDs = append(routineIDs, id)
	}
	return routineIDs, nil
}

func filteredSQLQuery(selectBase, routineIDColumn, orderBy string, bindVariable func(int) string, routineIDs []string) (string, []any) {
	variables := make([]string, len(routineIDs))
	args := make([]any, len(routineIDs))
	for index, id := range routineIDs {
		variables[index] = bindVariable(index + 1)
		args[index] = id
	}
	query := selectBase + " WHERE " + routineIDColumn + " IN (" + strings.Join(variables, ", ") + ")" + orderBy
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

func parseSQLLogTime(value any) (time.Time, error) {
	switch typed := value.(type) {
	case time.Time:
		return typed.UTC(), nil
	case int64:
		return time.UnixMicro(typed).UTC(), nil
	case []byte:
		return parseSQLLogTimeString(string(typed))
	case string:
		return parseSQLLogTimeString(typed)
	default:
		return time.Time{}, fmt.Errorf("unsupported timestamp type %T", value)
	}
}

func parseSQLLogTimeString(value string) (time.Time, error) {
	withZone := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999 -07:00",
	}
	for _, layout := range withZone {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), nil
		}
	}
	withoutZone := []string{
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	}
	for _, layout := range withoutZone {
		if parsed, err := time.ParseInLocation(layout, value, time.UTC); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp %q", value)
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
    "created_at" TIMESTAMPTZ NOT NULL
)`,
	createIndex: `CREATE INDEX IF NOT EXISTS "unique_routine_log_routine_id" ON "unique_routine_log" ("routine_id")`,
	insert: `INSERT INTO "unique_routine_log" ("id", "routine_id", "name", "owner", "action", "status", "log", "created_at")
VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)`,
	prune: fmt.Sprintf(`DELETE FROM "unique_routine_log"
WHERE "id" IN (
    SELECT "id" FROM (
        SELECT "id", ROW_NUMBER() OVER (ORDER BY "created_at" DESC, "id" DESC) AS "row_number"
        FROM "unique_routine_log"
		WHERE "routine_id" = $1
    ) AS "ranked"
		WHERE "row_number" > %d
	)`, retainedLogsPerRoutineID),
	selectBase:      `SELECT "id", "name", "owner", "action", "status", "log", "created_at" FROM "unique_routine_log"`,
	routineIDColumn: `"routine_id"`,
	orderBy:         ` ORDER BY "created_at" DESC, "id" DESC`,
	bindVariable:    dollarVariable,
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
    ` + "`created_at`" + ` DATETIME(6) NOT NULL,
	INDEX ` + "`unique_routine_log_routine_id`" + ` (` + "`routine_id`" + `)
)`,
	insert: `INSERT INTO ` + "`unique_routine_log`" + ` (` + "`id`" + `, ` + "`routine_id`" + `, ` + "`name`" + `, ` + "`owner`" + `, ` + "`action`" + `, ` + "`status`" + `, ` + "`log`" + `, ` + "`created_at`" + `)
VALUES (?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(6))`,
	prune: fmt.Sprintf(`DELETE FROM `+"`unique_routine_log`"+`
WHERE `+"`id`"+` IN (
    SELECT `+"`id`"+` FROM (
        SELECT `+"`id`"+`, ROW_NUMBER() OVER (ORDER BY `+"`created_at`"+` DESC, `+"`id`"+` DESC) AS `+"`row_number`"+`
        FROM `+"`unique_routine_log`"+`
		WHERE `+"`routine_id`"+` = ?
    ) AS `+"`ranked`"+`
		WHERE `+"`row_number`"+` > %d
	)`, retainedLogsPerRoutineID),
	selectBase:      `SELECT ` + "`id`" + `, ` + "`name`" + `, ` + "`owner`" + `, ` + "`action`" + `, ` + "`status`" + `, ` + "`log`" + `, ` + "`created_at`" + ` FROM ` + "`unique_routine_log`",
	routineIDColumn: "`routine_id`",
	orderBy:         ` ORDER BY ` + "`created_at`" + ` DESC, ` + "`id`" + ` DESC`,
	bindVariable:    questionVariable,
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
VALUES (?, ?, ?, ?, ?, ?, ?, ` + sqliteCurrentMicroseconds + `)`,
	prune: fmt.Sprintf(`DELETE FROM "unique_routine_log"
WHERE "id" IN (
    SELECT "id" FROM (
        SELECT "id", ROW_NUMBER() OVER (ORDER BY "created_at" DESC, "id" DESC) AS "row_number"
        FROM "unique_routine_log"
		WHERE "routine_id" = ?
    ) AS "ranked"
		WHERE "row_number" > %d
	)`, retainedLogsPerRoutineID),
	selectBase:      `SELECT "id", "name", "owner", "action", "status", "log", "created_at" FROM "unique_routine_log"`,
	routineIDColumn: `"routine_id"`,
	orderBy:         ` ORDER BY "created_at" DESC, "id" DESC`,
	bindVariable:    questionVariable,
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
            [created_at] DATETIME2 NOT NULL
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
VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7, SYSUTCDATETIME())`,
	prune: fmt.Sprintf(`DELETE FROM [unique_routine_log]
WHERE [id] IN (
    SELECT [id] FROM (
        SELECT [id], ROW_NUMBER() OVER (ORDER BY [created_at] DESC, [id] DESC) AS [row_number]
        FROM [unique_routine_log]
		WHERE [routine_id] = @p1
    ) AS [ranked]
		WHERE [row_number] > %d
	)`, retainedLogsPerRoutineID),
	selectBase:      `SELECT [id], [name], [owner], [action], [status], [log], [created_at] FROM [unique_routine_log]`,
	routineIDColumn: `[routine_id]`,
	orderBy:         ` ORDER BY [created_at] DESC, [id] DESC`,
	bindVariable:    sqlServerVariable,
}

var oracleLogStatements = sqlLogStatements{
	create: `DECLARE
	table_count PLS_INTEGER;
BEGIN
	EXECUTE IMMEDIATE 'CREATE TABLE "unique_routine_log" ("id" VARCHAR2(64) NOT NULL, "routine_id" VARCHAR2(64) NOT NULL, "name" VARCHAR2(255) NOT NULL, "owner" VARCHAR2(128) NOT NULL, "action" VARCHAR2(16) NOT NULL, "status" VARCHAR2(32) NOT NULL, "log" CLOB, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL, CONSTRAINT "unique_routine_log_pk" PRIMARY KEY ("id"))';
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
VALUES (:1, :2, :3, :4, :5, :6, :7, SYSTIMESTAMP)`,
	prune: fmt.Sprintf(`DELETE FROM "unique_routine_log"
WHERE "id" IN (
    SELECT "id" FROM (
        SELECT "id", ROW_NUMBER() OVER (ORDER BY "created_at" DESC, "id" DESC) AS "row_number"
        FROM "unique_routine_log"
		WHERE "routine_id" = :1
    )
		WHERE "row_number" > %d
	)`, retainedLogsPerRoutineID),
	selectBase:      `SELECT "id", "name", "owner", "action", "status", "log", "created_at" FROM "unique_routine_log"`,
	routineIDColumn: `"routine_id"`,
	orderBy:         ` ORDER BY "created_at" DESC, "id" DESC`,
	bindVariable:    oracleVariable,
}
