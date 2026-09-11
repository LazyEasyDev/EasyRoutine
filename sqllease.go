package EasyRoutine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

const sqlRenewRetryDelay = 10 * time.Second

var sqlLeaseInitMu sync.Mutex

// SQLDialect identifies a database compatibility target.
type SQLDialect string

const (
	SQLPostgreSQL SQLDialect = "postgresql"
	SQLMySQL      SQLDialect = "mysql"
	SQLMariaDB    SQLDialect = "mariadb"
	SQLTiDB       SQLDialect = "tidb"
	SQLSQLite     SQLDialect = "sqlite"
	SQLServer     SQLDialect = "sqlserver"
	SQLGaussDB    SQLDialect = "gaussdb"
	SQLOracle     SQLDialect = "oracle"
)

// sqlLease coordinates unique supervisors and stores current state and
// lifecycle history in SQL.
type sqlLease struct {
	db              sqlLeaseExecer
	statements      sqlLeaseStatements
	logs            sqlLogStatements
	query           sqlQueryContext
	renewRetryDelay time.Duration
}

type sqlLeaseExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type sqlLeaseStatements struct {
	create          string
	acquireExisting string
	acquireNew      string
	renew           string
	release         string
	ttlArgument     func(time.Duration) (int64, bool)
	selectBase      string
	routineIDColumn string
	bindVariable    func(int) string
}

// InitSQLLease creates the current-state and lifecycle-history tables when
// needed and registers the resulting provider for StartUniqueSupervisor.
func InitSQLLease(ctx context.Context, db *sql.DB, dialect SQLDialect) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	if db == nil {
		return errors.New("SQL database is required")
	}

	sqlLeaseInitMu.Lock()
	defer sqlLeaseInitMu.Unlock()

	coordinatorMu.RLock()
	initialized := defaultCoordinator != nil
	coordinatorMu.RUnlock()
	if initialized {
		return errors.New("lease provider is already initialized")
	}

	backend, err := newSQLLease(db, dialect)
	if err != nil {
		return err
	}
	if err := backend.ensureSchema(ctx); err != nil {
		return err
	}
	return initLease(backend)
}

func newSQLLease(db sqlLeaseExecer, dialect SQLDialect) (*sqlLease, error) {
	statements, err := statementsForSQLDialect(dialect)
	if err != nil {
		return nil, err
	}
	logs, err := statementsForSQLLogDialect(dialect)
	if err != nil {
		return nil, err
	}
	backend := &sqlLease{
		db:              db,
		statements:      statements,
		logs:            logs,
		renewRetryDelay: sqlRenewRetryDelay,
	}
	if queryer, ok := db.(interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	}); ok {
		backend.query = queryer.QueryContext
	}
	return backend, nil
}

func (s *sqlLease) ensureSchema(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return errors.New("SQL lease is not initialized")
	}
	if _, err := s.db.ExecContext(ctx, s.statements.create); err != nil {
		return fmt.Errorf("create unique_routine table: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, s.logs.create); err != nil {
		return fmt.Errorf("create unique_routine_log table: %w", err)
	}
	if s.logs.createIndex != "" {
		if _, err := s.db.ExecContext(ctx, s.logs.createIndex); err != nil {
			return fmt.Errorf("create unique_routine_log routine ID index: %w", err)
		}
	}
	return nil
}

func (s *sqlLease) acquireState(ctx context.Context, state leaseState, ttlArgument int64) bool {
	id := routineID(state.Name)
	result, err := s.db.ExecContext(ctx, s.statements.acquireExisting,
		state.Name, state.Owner, ttlArgument, string(state.Status), state.SuccessCount, state.FailureCount, state.Log, id)
	if err != nil {
		return false
	}
	if rowsAffected(result) {
		return true
	}

	result, err = s.db.ExecContext(ctx, s.statements.acquireNew,
		id, state.Name, state.Owner, ttlArgument, string(state.Status), state.SuccessCount, state.FailureCount, state.Log)
	return err == nil && rowsAffected(result)
}

func (s *sqlLease) renewState(ctx context.Context, state leaseState, ttlArgument int64) bool {
	args := []any{
		ttlArgument,
		string(state.Status),
		state.SuccessCount,
		state.FailureCount,
		state.Log,
		routineID(state.Name),
		state.Owner,
	}
	result, err := s.db.ExecContext(ctx, s.statements.renew,
		args...)
	if err == nil {
		return rowsAffected(result)
	}
	if ctx.Err() != nil || !waitForDelay(ctx, s.renewRetryDelay) {
		return false
	}

	result, err = s.db.ExecContext(ctx, s.statements.renew, args...)
	return err == nil && rowsAffected(result)
}

func (s *sqlLease) releaseState(ctx context.Context, state leaseState) bool {
	_, err := s.db.ExecContext(ctx, s.statements.release,
		string(state.Status), state.SuccessCount, state.FailureCount, state.Log, routineID(state.Name), state.Owner)
	return err == nil
}

func rowsAffected(result sql.Result) bool {
	if result == nil {
		return false
	}
	rows, err := result.RowsAffected()
	return err == nil && rows > 0
}

func routineID(name string) string {
	sum := sha256.Sum256([]byte(name))
	return hex.EncodeToString(sum[:])
}

func ttlSeconds(ttl time.Duration) (int64, bool) {
	if ttl <= 0 {
		return 0, false
	}
	seconds := int64(ttl / time.Second)
	if ttl%time.Second != 0 {
		seconds++
	}
	return seconds, true
}

func statementsForSQLDialect(dialect SQLDialect) (sqlLeaseStatements, error) {
	switch dialect {
	case SQLPostgreSQL:
		return postgreSQLLeaseStatements, nil
	case SQLMySQL, SQLMariaDB, SQLTiDB:
		return mySQLLeaseStatements, nil
	case SQLSQLite:
		return sqliteLeaseStatements, nil
	case SQLServer:
		return sqlServerLeaseStatements, nil
	case SQLGaussDB:
		return postgreSQLLeaseStatements, nil
	case SQLOracle:
		return oracleLeaseStatements, nil
	default:
		return sqlLeaseStatements{}, fmt.Errorf("unsupported SQL dialect %q", dialect)
	}
}

const postgreSQLCurrentSeconds = "CAST(FLOOR(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)) AS BIGINT)"

var postgreSQLLeaseStatements = sqlLeaseStatements{
	create: `CREATE TABLE IF NOT EXISTS "unique_routine" (
	"routine_id" VARCHAR(64) PRIMARY KEY,
	"name" TEXT NOT NULL,
    "owner" TEXT NOT NULL,
	"expires_at" BIGINT NOT NULL,
	"status" VARCHAR(32) NOT NULL,
	"success_count" BIGINT NOT NULL,
	"failure_count" BIGINT NOT NULL,
	"log" TEXT NOT NULL,
	"updated_at" BIGINT NOT NULL
)`,
	acquireExisting: `UPDATE "unique_routine"
SET "name" = $1,
	"owner" = $2,
	"expires_at" = ` + postgreSQLCurrentSeconds + ` + $3,
	"status" = $4,
	"success_count" = $5,
	"failure_count" = $6,
	"log" = $7,
	"updated_at" = ` + postgreSQLCurrentSeconds + `
WHERE "routine_id" = $8 AND "expires_at" <= ` + postgreSQLCurrentSeconds,
	acquireNew: `INSERT INTO "unique_routine" ("routine_id", "name", "owner", "expires_at", "status", "success_count", "failure_count", "log", "updated_at")
VALUES ($1, $2, $3, ` + postgreSQLCurrentSeconds + ` + $4, $5, $6, $7, $8, ` + postgreSQLCurrentSeconds + `)`,
	renew: `UPDATE "unique_routine"
	SET "expires_at" = ` + postgreSQLCurrentSeconds + ` + $1,
	"status" = $2,
	"success_count" = $3,
	"failure_count" = $4,
	"log" = $5,
	"updated_at" = ` + postgreSQLCurrentSeconds + `
WHERE "routine_id" = $6 AND "owner" = $7 AND "expires_at" > ` + postgreSQLCurrentSeconds,
	release: `UPDATE "unique_routine"
SET "expires_at" = ` + postgreSQLCurrentSeconds + `,
	"status" = $1,
	"success_count" = $2,
	"failure_count" = $3,
	"log" = $4,
	"updated_at" = ` + postgreSQLCurrentSeconds + `
WHERE "routine_id" = $5 AND "owner" = $6`,
	ttlArgument:     ttlSeconds,
	selectBase:      `SELECT "name", "owner", "status", "success_count", "failure_count", "log", "expires_at", "updated_at" FROM "unique_routine"`,
	routineIDColumn: `"routine_id"`,
	bindVariable:    dollarVariable,
}

const mySQLCurrentSeconds = "UNIX_TIMESTAMP()"

var mySQLLeaseStatements = sqlLeaseStatements{
	create: `CREATE TABLE IF NOT EXISTS ` + "`unique_routine`" + ` (
	` + "`routine_id`" + ` VARCHAR(64) NOT NULL PRIMARY KEY,
	` + "`name`" + ` VARCHAR(255) NOT NULL,
    ` + "`owner`" + ` VARCHAR(128) NOT NULL,
	` + "`expires_at`" + ` BIGINT NOT NULL,
	` + "`status`" + ` VARCHAR(32) NOT NULL,
	` + "`success_count`" + ` BIGINT NOT NULL,
	` + "`failure_count`" + ` BIGINT NOT NULL,
	` + "`log`" + ` LONGTEXT NOT NULL,
	` + "`updated_at`" + ` BIGINT NOT NULL
)`,
	acquireExisting: `UPDATE ` + "`unique_routine`" + `
SET ` + "`name`" + ` = ?,
	` + "`owner`" + ` = ?,
	` + "`expires_at`" + ` = ` + mySQLCurrentSeconds + ` + ?,
	` + "`status`" + ` = ?,
	` + "`success_count`" + ` = ?,
	` + "`failure_count`" + ` = ?,
	` + "`log`" + ` = ?,
	` + "`updated_at`" + ` = ` + mySQLCurrentSeconds + `
WHERE ` + "`routine_id`" + ` = ? AND ` + "`expires_at`" + ` <= ` + mySQLCurrentSeconds,
	acquireNew: `INSERT INTO ` + "`unique_routine`" + ` (` + "`routine_id`" + `, ` + "`name`" + `, ` + "`owner`" + `, ` + "`expires_at`" + `, ` + "`status`" + `, ` + "`success_count`" + `, ` + "`failure_count`" + `, ` + "`log`" + `, ` + "`updated_at`" + `)
VALUES (?, ?, ?, ` + mySQLCurrentSeconds + ` + ?, ?, ?, ?, ?, ` + mySQLCurrentSeconds + `)`,
	renew: `UPDATE ` + "`unique_routine`" + `
	SET ` + "`expires_at`" + ` = ` + mySQLCurrentSeconds + ` + ?,
	` + "`status`" + ` = ?,
	` + "`success_count`" + ` = ?,
	` + "`failure_count`" + ` = ?,
	` + "`log`" + ` = ?,
	` + "`updated_at`" + ` = ` + mySQLCurrentSeconds + `
WHERE ` + "`routine_id`" + ` = ? AND ` + "`owner`" + ` = ? AND ` + "`expires_at`" + ` > ` + mySQLCurrentSeconds,
	release: `UPDATE ` + "`unique_routine`" + `
SET ` + "`expires_at`" + ` = ` + mySQLCurrentSeconds + `,
	` + "`status`" + ` = ?,
	` + "`success_count`" + ` = ?,
	` + "`failure_count`" + ` = ?,
	` + "`log`" + ` = ?,
	` + "`updated_at`" + ` = ` + mySQLCurrentSeconds + `
WHERE ` + "`routine_id`" + ` = ? AND ` + "`owner`" + ` = ?`,
	ttlArgument:     ttlSeconds,
	selectBase:      `SELECT ` + "`name`" + `, ` + "`owner`" + `, ` + "`status`" + `, ` + "`success_count`" + `, ` + "`failure_count`" + `, ` + "`log`" + `, ` + "`expires_at`" + `, ` + "`updated_at`" + ` FROM ` + "`unique_routine`",
	routineIDColumn: "`routine_id`",
	bindVariable:    questionVariable,
}

const sqliteCurrentSeconds = "CAST(strftime('%s', 'now') AS INTEGER)"

var sqliteLeaseStatements = sqlLeaseStatements{
	create: `CREATE TABLE IF NOT EXISTS "unique_routine" (
	"routine_id" TEXT PRIMARY KEY,
	"name" TEXT NOT NULL,
    "owner" TEXT NOT NULL,
	"expires_at" INTEGER NOT NULL,
	"status" TEXT NOT NULL,
	"success_count" INTEGER NOT NULL,
	"failure_count" INTEGER NOT NULL,
	"log" TEXT NOT NULL,
	"updated_at" INTEGER NOT NULL
) WITHOUT ROWID`,
	acquireExisting: `UPDATE "unique_routine"
SET "name" = ?,
	"owner" = ?,
	"expires_at" = ` + sqliteCurrentSeconds + ` + ?,
	"status" = ?,
	"success_count" = ?,
	"failure_count" = ?,
	"log" = ?,
	"updated_at" = ` + sqliteCurrentSeconds + `
WHERE "routine_id" = ? AND "expires_at" <= ` + sqliteCurrentSeconds,
	acquireNew: `INSERT INTO "unique_routine" ("routine_id", "name", "owner", "expires_at", "status", "success_count", "failure_count", "log", "updated_at")
VALUES (?, ?, ?, ` + sqliteCurrentSeconds + ` + ?, ?, ?, ?, ?, ` + sqliteCurrentSeconds + `)`,
	renew: `UPDATE "unique_routine"
	SET "expires_at" = ` + sqliteCurrentSeconds + ` + ?,
	"status" = ?,
	"success_count" = ?,
	"failure_count" = ?,
	"log" = ?,
	"updated_at" = ` + sqliteCurrentSeconds + `
WHERE "routine_id" = ? AND "owner" = ? AND "expires_at" > ` + sqliteCurrentSeconds,
	release: `UPDATE "unique_routine"
SET "expires_at" = ` + sqliteCurrentSeconds + `,
	"status" = ?,
	"success_count" = ?,
	"failure_count" = ?,
	"log" = ?,
	"updated_at" = ` + sqliteCurrentSeconds + `
WHERE "routine_id" = ? AND "owner" = ?`,
	ttlArgument:     ttlSeconds,
	selectBase:      `SELECT "name", "owner", "status", "success_count", "failure_count", "log", "expires_at", "updated_at" FROM "unique_routine"`,
	routineIDColumn: `"routine_id"`,
	bindVariable:    questionVariable,
}

const sqlServerCurrentSeconds = "DATEDIFF_BIG(SECOND, CAST('1970-01-01T00:00:00' AS DATETIME2), SYSUTCDATETIME())"

var sqlServerLeaseStatements = sqlLeaseStatements{
	create: `BEGIN TRY
    IF OBJECT_ID(N'unique_routine', N'U') IS NULL
    BEGIN
        CREATE TABLE [unique_routine] (
			[routine_id] VARCHAR(64) NOT NULL PRIMARY KEY,
			[name] NVARCHAR(255) NOT NULL,
            [owner] NVARCHAR(128) NOT NULL,
			[expires_at] BIGINT NOT NULL,
			[status] NVARCHAR(32) NOT NULL,
			[success_count] BIGINT NOT NULL,
			[failure_count] BIGINT NOT NULL,
			[log] NVARCHAR(MAX) NOT NULL,
			[updated_at] BIGINT NOT NULL
        )
    END
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() <> 2714
        THROW;
END CATCH`,
	acquireExisting: `UPDATE [unique_routine]
SET [name] = @p1,
	[owner] = @p2,
	[expires_at] = ` + sqlServerCurrentSeconds + ` + @p3,
	[status] = @p4,
	[success_count] = @p5,
	[failure_count] = @p6,
	[log] = @p7,
	[updated_at] = ` + sqlServerCurrentSeconds + `
WHERE [routine_id] = @p8 AND [expires_at] <= ` + sqlServerCurrentSeconds,
	acquireNew: `INSERT INTO [unique_routine] ([routine_id], [name], [owner], [expires_at], [status], [success_count], [failure_count], [log], [updated_at])
VALUES (@p1, @p2, @p3, ` + sqlServerCurrentSeconds + ` + @p4, @p5, @p6, @p7, @p8, ` + sqlServerCurrentSeconds + `)`,
	renew: `UPDATE [unique_routine]
	SET [expires_at] = ` + sqlServerCurrentSeconds + ` + @p1,
	[status] = @p2,
	[success_count] = @p3,
	[failure_count] = @p4,
	[log] = @p5,
	[updated_at] = ` + sqlServerCurrentSeconds + `
WHERE [routine_id] = @p6 AND [owner] = @p7 AND [expires_at] > ` + sqlServerCurrentSeconds,
	release: `UPDATE [unique_routine]
SET [expires_at] = ` + sqlServerCurrentSeconds + `,
	[status] = @p1,
	[success_count] = @p2,
	[failure_count] = @p3,
	[log] = @p4,
	[updated_at] = ` + sqlServerCurrentSeconds + `
WHERE [routine_id] = @p5 AND [owner] = @p6`,
	ttlArgument:     ttlSeconds,
	selectBase:      `SELECT [name], [owner], [status], [success_count], [failure_count], [log], [expires_at], [updated_at] FROM [unique_routine]`,
	routineIDColumn: `[routine_id]`,
	bindVariable:    sqlServerVariable,
}

const oracleCurrentSeconds = "FLOOR((CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE) - DATE '1970-01-01') * 86400)"

var oracleLeaseStatements = sqlLeaseStatements{
	create: `DECLARE
	table_count PLS_INTEGER;
BEGIN
	EXECUTE IMMEDIATE 'CREATE TABLE "unique_routine" ("routine_id" VARCHAR2(64) NOT NULL, "name" VARCHAR2(255) NOT NULL, "owner" VARCHAR2(128) NOT NULL, "expires_at" NUMBER(19) NOT NULL, "status" VARCHAR2(32) NOT NULL, "success_count" NUMBER(19) NOT NULL, "failure_count" NUMBER(19) NOT NULL, "log" CLOB, "updated_at" NUMBER(19) NOT NULL, CONSTRAINT "unique_routine_pk" PRIMARY KEY ("routine_id"))';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -955 THEN
            RAISE;
        END IF;
		SELECT COUNT(*) INTO table_count FROM USER_TABLES WHERE TABLE_NAME = 'unique_routine';
		IF table_count = 0 THEN
			RAISE;
		END IF;
END;`,
	acquireExisting: `UPDATE "unique_routine"
SET "name" = :1,
	"owner" = :2,
	"expires_at" = ` + oracleCurrentSeconds + ` + :3,
	"status" = :4,
	"success_count" = :5,
	"failure_count" = :6,
	"log" = :7,
	"updated_at" = ` + oracleCurrentSeconds + `
WHERE "routine_id" = :8 AND "expires_at" <= ` + oracleCurrentSeconds,
	acquireNew: `INSERT INTO "unique_routine" ("routine_id", "name", "owner", "expires_at", "status", "success_count", "failure_count", "log", "updated_at")
VALUES (:1, :2, :3, ` + oracleCurrentSeconds + ` + :4, :5, :6, :7, :8, ` + oracleCurrentSeconds + `)`,
	renew: `UPDATE "unique_routine"
	SET "expires_at" = ` + oracleCurrentSeconds + ` + :1,
	"status" = :2,
	"success_count" = :3,
	"failure_count" = :4,
	"log" = :5,
	"updated_at" = ` + oracleCurrentSeconds + `
WHERE "routine_id" = :6 AND "owner" = :7 AND "expires_at" > ` + oracleCurrentSeconds,
	release: `UPDATE "unique_routine"
SET "expires_at" = ` + oracleCurrentSeconds + `,
	"status" = :1,
	"success_count" = :2,
	"failure_count" = :3,
	"log" = :4,
	"updated_at" = ` + oracleCurrentSeconds + `
WHERE "routine_id" = :5 AND "owner" = :6`,
	ttlArgument:     ttlSeconds,
	selectBase:      `SELECT "name", "owner", "status", "success_count", "failure_count", "log", "expires_at", "updated_at" FROM "unique_routine"`,
	routineIDColumn: `"routine_id"`,
	bindVariable:    oracleVariable,
}
