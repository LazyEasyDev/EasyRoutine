package EasyRoutine

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"time"
)

const retainedLogsPerRoutineID = 25
const sqlLogTimeout = 10 * time.Second

// Keep each filter below Oracle's IN-list and older SQLite variable limits.
const sqlNameFilterBatchSize = 900

type sqlLogStatements struct {
	create      string
	createIndex string
	insert      string
	prune       string
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
		return ttlSeconds(state.TTL)
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
	ctx, cancel := context.WithTimeout(ctx, sqlLogTimeout)
	defer cancel()

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
	create: `CREATE TABLE IF NOT EXISTS "easy_routine_unique_log" (
    "id" VARCHAR(64) PRIMARY KEY,
	"routine_id" VARCHAR(64) NOT NULL,
    "name" TEXT NOT NULL,
    "owner" TEXT NOT NULL,
    "action" VARCHAR(16) NOT NULL,
    "status" VARCHAR(32) NOT NULL,
    "log" TEXT NOT NULL,
	"created_at" BIGINT NOT NULL
)`,
	createIndex: `CREATE INDEX IF NOT EXISTS "easy_routine_unique_log_rid" ON "easy_routine_unique_log" ("routine_id")`,
	insert: `INSERT INTO "easy_routine_unique_log" ("id", "routine_id", "name", "owner", "action", "status", "log", "created_at")
VALUES ($1, $2, $3, $4, $5, $6, $7, ` + postgreSQLCurrentSeconds + `)`,
	prune: fmt.Sprintf(`DELETE FROM "easy_routine_unique_log"
WHERE "id" IN (
    SELECT "id" FROM (
        SELECT "id", ROW_NUMBER() OVER (ORDER BY "created_at" DESC, "id" DESC) AS "row_number"
        FROM "easy_routine_unique_log"
		WHERE "routine_id" = $1
    ) AS "ranked"
		WHERE "row_number" > %d
	)`, retainedLogsPerRoutineID),
}

var mySQLLogStatements = sqlLogStatements{
	create: `CREATE TABLE IF NOT EXISTS ` + "`easy_routine_unique_log`" + ` (
    ` + "`id`" + ` VARCHAR(64) NOT NULL PRIMARY KEY,
	` + "`routine_id`" + ` VARCHAR(64) NOT NULL,
    ` + "`name`" + ` VARCHAR(255) NOT NULL,
    ` + "`owner`" + ` VARCHAR(128) NOT NULL,
    ` + "`action`" + ` VARCHAR(16) NOT NULL,
    ` + "`status`" + ` VARCHAR(32) NOT NULL,
    ` + "`log`" + ` LONGTEXT NOT NULL,
	` + "`created_at`" + ` BIGINT NOT NULL,
	INDEX ` + "`easy_routine_unique_log_rid`" + ` (` + "`routine_id`" + `)
)`,
	insert: `INSERT INTO ` + "`easy_routine_unique_log`" + ` (` + "`id`" + `, ` + "`routine_id`" + `, ` + "`name`" + `, ` + "`owner`" + `, ` + "`action`" + `, ` + "`status`" + `, ` + "`log`" + `, ` + "`created_at`" + `)
VALUES (?, ?, ?, ?, ?, ?, ?, ` + mySQLCurrentSeconds + `)`,
	prune: fmt.Sprintf(`DELETE FROM `+"`easy_routine_unique_log`"+`
WHERE `+"`id`"+` IN (
    SELECT `+"`id`"+` FROM (
        SELECT `+"`id`"+`, ROW_NUMBER() OVER (ORDER BY `+"`created_at`"+` DESC, `+"`id`"+` DESC) AS `+"`row_number`"+`
        FROM `+"`easy_routine_unique_log`"+`
		WHERE `+"`routine_id`"+` = ?
    ) AS `+"`ranked`"+`
		WHERE `+"`row_number`"+` > %d
	)`, retainedLogsPerRoutineID),
}

var sqliteLogStatements = sqlLogStatements{
	create: `CREATE TABLE IF NOT EXISTS "easy_routine_unique_log" (
    "id" TEXT PRIMARY KEY,
	"routine_id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "owner" TEXT NOT NULL,
    "action" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "log" TEXT NOT NULL,
    "created_at" INTEGER NOT NULL
) WITHOUT ROWID`,
	createIndex: `CREATE INDEX IF NOT EXISTS "easy_routine_unique_log_rid" ON "easy_routine_unique_log" ("routine_id")`,
	insert: `INSERT INTO "easy_routine_unique_log" ("id", "routine_id", "name", "owner", "action", "status", "log", "created_at")
VALUES (?, ?, ?, ?, ?, ?, ?, ` + sqliteCurrentSeconds + `)`,
	prune: fmt.Sprintf(`DELETE FROM "easy_routine_unique_log"
WHERE "id" IN (
    SELECT "id" FROM (
        SELECT "id", ROW_NUMBER() OVER (ORDER BY "created_at" DESC, "id" DESC) AS "row_number"
        FROM "easy_routine_unique_log"
		WHERE "routine_id" = ?
    ) AS "ranked"
		WHERE "row_number" > %d
	)`, retainedLogsPerRoutineID),
}

var sqlServerLogStatements = sqlLogStatements{
	create: `BEGIN TRY
	IF OBJECT_ID(N'easy_routine_unique_log', N'U') IS NULL
    BEGIN
		CREATE TABLE [easy_routine_unique_log] (
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
		WHERE [name] = N'easy_routine_unique_log_rid'
		  AND [object_id] = OBJECT_ID(N'easy_routine_unique_log')
	)
	BEGIN
		CREATE INDEX [easy_routine_unique_log_rid] ON [easy_routine_unique_log] ([routine_id])
	END
END TRY
BEGIN CATCH
	IF ERROR_NUMBER() <> 1913
		THROW;
END CATCH`,
	insert: `INSERT INTO [easy_routine_unique_log] ([id], [routine_id], [name], [owner], [action], [status], [log], [created_at])
VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7, ` + sqlServerCurrentSeconds + `)`,
	prune: fmt.Sprintf(`DELETE FROM [easy_routine_unique_log]
WHERE [id] IN (
    SELECT [id] FROM (
        SELECT [id], ROW_NUMBER() OVER (ORDER BY [created_at] DESC, [id] DESC) AS [row_number]
        FROM [easy_routine_unique_log]
		WHERE [routine_id] = @p1
    ) AS [ranked]
		WHERE [row_number] > %d
	)`, retainedLogsPerRoutineID),
}

var oracleLogStatements = sqlLogStatements{
	create: `DECLARE
	table_count PLS_INTEGER;
BEGIN
	EXECUTE IMMEDIATE 'CREATE TABLE "easy_routine_unique_log" ("id" VARCHAR2(64) NOT NULL, "routine_id" VARCHAR2(64) NOT NULL, "name" VARCHAR2(255) NOT NULL, "owner" VARCHAR2(128) NOT NULL, "action" VARCHAR2(16) NOT NULL, "status" VARCHAR2(32) NOT NULL, "log" CLOB, "created_at" NUMBER(19) NOT NULL, CONSTRAINT "easy_routine_unique_log_pk" PRIMARY KEY ("id"))';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -955 THEN
            RAISE;
        END IF;
		SELECT COUNT(*) INTO table_count FROM USER_TABLES WHERE TABLE_NAME = 'easy_routine_unique_log';
		IF table_count = 0 THEN
			RAISE;
		END IF;
END;`,
	createIndex: `DECLARE
	index_count PLS_INTEGER;
BEGIN
	BEGIN
		EXECUTE IMMEDIATE 'CREATE INDEX "easy_routine_unique_log_rid" ON "easy_routine_unique_log" ("routine_id")';
	EXCEPTION
		WHEN OTHERS THEN
			IF SQLCODE != -955 THEN
				RAISE;
			END IF;
			SELECT COUNT(*) INTO index_count
			FROM USER_IND_COLUMNS
			WHERE INDEX_NAME = 'easy_routine_unique_log_rid'
			  AND TABLE_NAME = 'easy_routine_unique_log'
			  AND COLUMN_NAME = 'routine_id'
			  AND COLUMN_POSITION = 1;
			IF index_count = 0 THEN
				RAISE;
			END IF;
	END;
END;`,
	insert: `INSERT INTO "easy_routine_unique_log" ("id", "routine_id", "name", "owner", "action", "status", "log", "created_at")
VALUES (:1, :2, :3, :4, :5, :6, :7, ` + oracleCurrentSeconds + `)`,
	prune: fmt.Sprintf(`DELETE FROM "easy_routine_unique_log"
WHERE "id" IN (
    SELECT "id" FROM (
        SELECT "id", ROW_NUMBER() OVER (ORDER BY "created_at" DESC, "id" DESC) AS "row_number"
	        FROM "easy_routine_unique_log"
		WHERE "routine_id" = :1
    )
		WHERE "row_number" > %d
	)`, retainedLogsPerRoutineID),
}
