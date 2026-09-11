package EasyRoutine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const retainedLogsPerName = 25

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

// Action atomically applies ownership and current state, then records a
// best-effort history snapshot. A log failure does not change the result.
func (s *SQLLease) Action(ctx context.Context, action LeaseAction, state LeaseState) bool {
	if !s.validAction(ctx, action, state) {
		return false
	}

	var applied bool
	switch action {
	case LeaseAcquire:
		applied = s.acquireState(ctx, state)
	case LeaseRenew:
		applied = s.renewState(ctx, state)
	case LeaseRelease:
		applied = s.releaseState(ctx, state)
	}
	if applied {
		s.recordLog(ctx, action, state)
	}
	return applied
}

// GetLogs returns retained records newest first. With no names it returns logs
// for every task; otherwise it returns only records matching the supplied names.
func (s *SQLLease) GetLogs(ctx context.Context, names ...string) ([]SupervisorLog, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || s.db == nil || s.query == nil {
		return nil, errors.New("SQL lease is not initialized")
	}
	for _, name := range names {
		if err := validateRoutineName(name); err != nil {
			return nil, err
		}
	}

	query := s.logs.selectBase
	args := make([]any, len(names))
	if len(names) > 0 {
		variables := make([]string, len(names))
		for index, name := range names {
			variables[index] = s.logs.bindVariable(index + 1)
			args[index] = routineID(name)
		}
		query += " WHERE " + s.logs.routineIDColumn + " IN (" + strings.Join(variables, ", ") + ")"
	}
	query += s.logs.orderBy

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
func (s *SQLLease) GetStatuses(ctx context.Context, names ...string) ([]SupervisorStatus, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || s.db == nil || s.query == nil {
		return nil, errors.New("SQL lease is not initialized")
	}
	for _, name := range names {
		if err := validateRoutineName(name); err != nil {
			return nil, err
		}
	}

	query := s.statements.selectBase
	args := make([]any, len(names))
	if len(names) > 0 {
		variables := make([]string, len(names))
		for index, name := range names {
			variables[index] = s.statements.bindVariable(index + 1)
			args[index] = routineID(name)
		}
		query += " WHERE " + s.statements.routineIDColumn + " IN (" + strings.Join(variables, ", ") + ")"
	}
	query += s.statements.orderBy

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

func (s *SQLLease) validAction(ctx context.Context, action LeaseAction, state LeaseState) bool {
	if ctx == nil || ctx.Err() != nil || s == nil || s.db == nil || state.Owner == "" {
		return false
	}
	if validateRoutineName(state.Name) != nil {
		return false
	}
	if state.SuccessCount < 0 || state.FailureCount < 0 || !validRoutineStatus(state.Status) {
		return false
	}
	switch action {
	case LeaseAcquire, LeaseRenew:
		_, ok := s.validLeaseArguments(ctx, state.Name, state.Owner, state.TTL)
		return ok
	case LeaseRelease:
		return true
	default:
		return false
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

func (s *SQLLease) recordLog(ctx context.Context, action LeaseAction, state LeaseState) {
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
	)`, retainedLogsPerName),
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
	)`, retainedLogsPerName),
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
	)`, retainedLogsPerName),
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
	)`, retainedLogsPerName),
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
	)`, retainedLogsPerName),
	selectBase:      `SELECT "id", "name", "owner", "action", "status", "log", "created_at" FROM "unique_routine_log"`,
	routineIDColumn: `"routine_id"`,
	orderBy:         ` ORDER BY "created_at" DESC, "id" DESC`,
	bindVariable:    oracleVariable,
}
