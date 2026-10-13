package EasyRoutine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
)

type sqlSupervisorStatements struct {
	selectBase      string
	countsBase      string
	groupCounts     string
	history         string
	deleteState     string
	deleteHistory   string
	routineIDColumn string
	bindVariable    func(int) string
}

func (s *sqlLease) ListSupervisors(ctx context.Context, names ...string) ([]Supervisor, error) {
	if s == nil || s.query == nil {
		return nil, errors.New("SQL lease queries are not initialized")
	}
	items := make([]Supervisor, 0)
	routineIDs := uniqueRoutineIDs(names)
	if len(routineIDs) == 0 {
		return s.listSupervisorBatch(ctx, nil)
	}
	for start := 0; start < len(routineIDs); start += sqlNameFilterBatchSize {
		end := min(start+sqlNameFilterBatchSize, len(routineIDs))
		batch, err := s.listSupervisorBatch(ctx, routineIDs[start:end])
		if err != nil {
			return nil, err
		}
		items = append(items, batch...)
	}
	sortSupervisors(items)
	return items, nil
}

func (s *sqlLease) listSupervisorBatch(ctx context.Context, routineIDs []string) ([]Supervisor, error) {
	query := s.supervisors.selectBase
	countsQuery := s.supervisors.countsBase
	var args []any
	if len(routineIDs) > 0 {
		query, args = filteredSQLQuery(query, s.supervisors.routineIDColumn, s.supervisors.bindVariable, routineIDs)
		countsQuery, _ = filteredSQLQuery(countsQuery, s.supervisors.routineIDColumn, s.supervisors.bindVariable, routineIDs)
	}
	items, err := s.querySupervisors(ctx, query, args)
	if err != nil {
		return nil, err
	}
	routinesByID := make(map[string]*Supervisor, len(items))
	for index := range items {
		routinesByID[items[index].RoutineID] = &items[index]
	}
	rows, err := s.query(ctx, countsQuery+s.supervisors.groupCounts, args...)
	if err != nil {
		return nil, fmt.Errorf("query supervisor log counts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var routineID string
		var status RoutineStatus
		var count, latestAt int64
		if err := rows.Scan(&routineID, &status, &count, &latestAt); err != nil {
			return nil, fmt.Errorf("scan supervisor log counts: %w", err)
		}
		routine := routinesByID[routineID]
		if routine == nil {
			continue
		}
		routine.RetainedLogs += count
		switch status {
		case RoutineNotStarted:
			routine.LogCounts.NotStarted += count
		case RoutineRunning:
			routine.LogCounts.Running += count
		case RoutineDone:
			routine.LogCounts.Done += count
		case RoutinePanic:
			routine.LogCounts.Panic += count
			if latestAt > routine.LatestPanicAt {
				routine.LatestPanicAt = latestAt
			}
		default:
			routine.LogCounts.Other += count
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate supervisor log counts: %w", err)
	}
	sortSupervisors(items)
	return items, nil
}

func (s *sqlLease) querySupervisors(ctx context.Context, query string, args []any) ([]Supervisor, error) {
	rows, err := s.query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query supervisors: %w", err)
	}
	defer rows.Close()
	items := make([]Supervisor, 0)
	for rows.Next() {
		var item Supervisor
		var leaseActive int64
		if err := rows.Scan(
			&item.RoutineID, &item.Name, &item.Owner, &item.Status,
			&item.SuccessCount, &item.FailureCount, &item.ExpiresAt, &item.UpdatedAt, &leaseActive,
		); err != nil {
			return nil, fmt.Errorf("scan supervisor: %w", err)
		}
		item.LeaseActive = leaseActive != 0
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate supervisors: %w", err)
	}
	return items, nil
}

func sortSupervisors(items []Supervisor) {
	sort.Slice(items, func(left, right int) bool {
		if items[left].Name == items[right].Name {
			return items[left].RoutineID < items[right].RoutineID
		}
		return items[left].Name < items[right].Name
	})
}

func (s *sqlLease) GetSupervisorHistory(ctx context.Context, routineID string) ([]SupervisorLog, error) {
	if s == nil || s.query == nil {
		return nil, errors.New("SQL lease queries are not initialized")
	}
	rows, err := s.query(ctx, s.supervisors.history, routineID)
	if err != nil {
		return nil, fmt.Errorf("query supervisor history: %w", err)
	}
	defer rows.Close()
	items := make([]SupervisorLog, 0)
	for rows.Next() {
		var item SupervisorLog
		var logText sql.NullString
		if err := rows.Scan(&item.ID, &item.RoutineID, &item.Name, &item.Owner, &item.Action, &item.Status, &logText, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan supervisor history: %w", err)
		}
		item.Log = logText.String
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate supervisor history: %w", err)
	}
	return items, nil
}

func (s *sqlLease) DeleteSupervisorRecords(ctx context.Context, name string) error {
	if s == nil || s.begin == nil {
		return errors.New("SQL lease transactions are not initialized")
	}
	transaction, err := s.begin(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin supervisor deletion: %w", err)
	}
	defer transaction.Rollback()
	id := routineID(name)
	if _, err := transaction.ExecContext(ctx, s.supervisors.deleteHistory, id); err != nil {
		return fmt.Errorf("delete supervisor history: %w", err)
	}
	if _, err := transaction.ExecContext(ctx, s.supervisors.deleteState, id); err != nil {
		return fmt.Errorf("delete supervisor state: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit supervisor deletion: %w", err)
	}
	return nil
}

func statementsForSQLSupervisorDialect(dialect SQLDialect) (sqlSupervisorStatements, error) {
	quoteStart, quoteEnd := `"`, `"`
	var currentSeconds string
	var bindVariable func(int) string
	switch dialect {
	case SQLPostgreSQL, SQLGaussDB:
		currentSeconds, bindVariable = postgreSQLCurrentSeconds, dollarVariable
	case SQLMySQL, SQLMariaDB, SQLTiDB:
		quoteStart, quoteEnd = "`", "`"
		currentSeconds, bindVariable = mySQLCurrentSeconds, questionVariable
	case SQLSQLite:
		currentSeconds, bindVariable = sqliteCurrentSeconds, questionVariable
	case SQLServer:
		quoteStart, quoteEnd = "[", "]"
		currentSeconds = sqlServerCurrentSeconds
		bindVariable = func(index int) string { return "CAST(" + sqlServerVariable(index) + " AS VARCHAR(64))" }
	case SQLOracle:
		currentSeconds, bindVariable = oracleCurrentSeconds, oracleVariable
	default:
		return sqlSupervisorStatements{}, fmt.Errorf("unsupported SQL dialect %q", dialect)
	}
	quote := func(name string) string { return quoteStart + name + quoteEnd }
	columns := func(names ...string) string {
		quoted := make([]string, len(names))
		for index, name := range names {
			quoted[index] = quote(name)
		}
		return strings.Join(quoted, ", ")
	}
	stateTable := quote("easy_rountine_unique")
	logTable := quote("easy_routine_unique_log")
	routineIDColumn := quote("routine_id")
	predicate := " WHERE " + routineIDColumn + " = " + bindVariable(1)
	return sqlSupervisorStatements{
		selectBase: "SELECT " + columns("routine_id", "name", "owner", "status", "success_count", "failure_count", "expires_at", "updated_at") +
			", CASE WHEN " + quote("expires_at") + " > " + currentSeconds + " THEN 1 ELSE 0 END FROM " + stateTable,
		countsBase:  "SELECT " + columns("routine_id", "status") + ", COUNT(*), MAX(" + quote("created_at") + ") FROM " + logTable,
		groupCounts: " GROUP BY " + columns("routine_id", "status"),
		history: "SELECT " + columns("id", "routine_id", "name", "owner", "action", "status", "log", "created_at") +
			" FROM " + logTable + predicate + " ORDER BY " + quote("created_at") + " DESC, " + quote("id") + " DESC",
		deleteState:     "DELETE FROM " + stateTable + predicate,
		deleteHistory:   "DELETE FROM " + logTable + predicate,
		routineIDColumn: routineIDColumn,
		bindVariable:    bindVariable,
	}, nil
}
