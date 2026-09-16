# Real Database Backend Test Report

## Summary

Date: 2026-09-12
Repository: EasyRoutine, branch `main`, baseline commit `45fc682`
Host: Ubuntu 26.04 LTS, Linux ARM64, 4 CPUs, 7.2 GiB RAM
Toolchain: Go 1.26.0, Podman 5.7.0 rootless

All eight advertised SQL dialect values were exercised through real database engines. SQLite was tested in both WAL and rollback-journal modes. The expanded matrix also ran many different routine names simultaneously, with multiple contenders per name, and checked live/final status plus exact acquire/release history isolation. The final matrix passed after fixing two production defects and hardening two additional edge cases.

The server containers and downloaded database images were removed after validation. No test database process was left running.

## Backend Matrix

| Dialect | Engine used | Result | Final matrix time | Notes |
| --- | --- | ---: | ---: | --- |
| `postgresql` | PostgreSQL 17.11 (`postgres:17-alpine`) | PASS | 35.18 s | Native ARM64 server |
| `mysql` | MySQL 8.4.11 (`mysql:8.4`) | PASS | 31.11 s | Default changed-row driver semantics; no `clientFoundRows` workaround |
| `mariadb` | MariaDB 11.8 image | PASS | 29.56 s | Tested independently from MySQL |
| `tidb` | TiDB v8.5.3, protocol version `8.0.11-TiDB-v8.5.3` | PASS | 30.16 s | Native ARM64 standalone server |
| `sqlite` | Embedded SQLite through `modernc.org/sqlite` v1.58.0, WAL mode | PASS | 24.23 s | Full stress run; file-backed database, 15 s busy timeout |
| `sqlite` | Embedded SQLite through `modernc.org/sqlite` v1.58.0, DELETE journal mode | PASS | 22.10 s | File-backed rollback journal, 15 s busy timeout |
| `sqlserver` | Microsoft Azure SQL Edge 1.0.7, SQL Server 2019-derived T-SQL engine | PASS | 27.42 s | Byte-identical official image mirror; ARM64 compatibility engine; see limitations |
| `gaussdb` | openGauss 6.0.0 | PASS | 23.76 s | GaussDB/PostgreSQL compatibility path; see limitations |
| `oracle` | Oracle AI Database Free 23.26.3.0.0 (`oracle-free:23-slim-faststart`) | PASS | 26.52 s | Native ARM64 Oracle server |

## Final Stress Configuration

Each backend matrix used:

- 256 simultaneous lease contenders over 8 rounds: 2,048 synchronized acquisition attempts per matrix, with exactly one winner in every round.
- 48 competing `StartUniqueSupervisor` coordinators performing sequential lease handoff, while asserting maximum active task count stayed at one.
- 64 different routine names active at the same instant, with three competing supervisors per name (192 supervisor instances per matrix). Every name ran exactly once while global parallelism reached exactly 64.
- Exact-name identity variants differing only by case (`identity`/`IDENTITY`) or Unicode composition (`caf\u00e9`/`cafe\u0301`) ran concurrently without collisions.
- For every parallel name, live and final `Name`, `Owner`, `Status`, counters, log prefix, and timestamp invariants were checked. History was required to contain exactly one `acquire` and one `release`, with the correct owner and status and no cross-name records.
- 20 independent operating-system worker processes contending for one routine and using an exclusive filesystem marker to detect overlapping task execution.
- 8 independent processes racing to initialize the schema.
- 905 distinct routine names, plus duplicate filters, to cross the 900-bind query batch boundary.
- 80 concurrent history-producing release operations, followed by an exact assertion that only the newest 25 records remained.
- A 255-byte UTF-8 routine name, a roughly 160 KiB Unicode log, `math.MaxInt64` success and `math.MaxInt64 - 1` failure counters, and exact round-trip assertions.
- A 5-second test lease with 250 ms heartbeats. Renewal was observed for more than 7 seconds, beyond the initial TTL.
- A 1-second expired-lease takeover followed by stale renew and stale release checks.
- Panic status and log persistence, lease-loss cancellation, owner mismatch, idempotent release, public API initialization, filtered status/history, and query ordering checks.

Across the nine final engine/configuration matrices, the synchronized contention stage alone performed 18,432 acquisition attempts. The process stage started 180 independent worker processes, and the supervisor handoff stage exercised 432 supervisor owners. The new distinct-name stage started 1,728 supervisor instances, observed 576 distinct routine tasks, and verified 1,152 exact acquire/release history rows.

A final race-enabled real SQLite WAL run passed in 23.77 seconds with 128 contenders over 4 rounds, 24 same-name supervisors, 12 worker processes, and 64 simultaneous distinct names with three replicas each.

After the assertions were strengthened to require exact live/final log and timestamp fields, case-only and composed/decomposed Unicode identity pairs, exactly two history records per name, and non-empty unfiltered queries, the affected focused cases were rerun and passed on SQLite WAL and DELETE journals, PostgreSQL, MySQL, MariaDB, and TiDB. SQL Server, openGauss, and Oracle ran the complete strengthened matrix directly.

## Scenarios Executed Per Matrix

1. Concurrent schema initialization across processes.
2. Exported `InitSQLLease` and `GetSupervisorStatuses` initialization path.
3. Initial acquisition, live-lease exclusion, owner renewal, non-owner renewal rejection, idempotent release, and filtered plus unfiltered non-empty status/history queries.
4. Database-clock expiry, replacement-owner takeover, stale renew rejection, and stale release fencing.
5. Maximum UTF-8 name, large Unicode log, and signed 64-bit counter round trip.
6. Massive synchronized acquisition contention.
7. Concurrent history writes and exact retention pruning.
8. Deduplicated status/history queries spanning multiple bind batches, including identity checks for every returned routine.
9. Long-running supervisor heartbeat renewal beyond the original TTL.
10. Supervisor panic capture and persisted panic state.
11. Task cancellation after forced lease loss.
12. Repeated supervisor handoff with overlap detection.
13. Parallel execution of 64 distinct names with three contenders per name, exact-name variants, per-name overlap detection, live/final status checks, distinct owner checks, and exact acquire/release history verification.
14. Independent process contention with overlap detection.

## Defects Found and Fixed

### 1. PostgreSQL concurrent startup race

Observed failure:

- Multiple processes executing `CREATE TABLE IF NOT EXISTS` simultaneously failed with SQLSTATE `23505` on PostgreSQL's internal `pg_type_typname_nsp_index`.
- Four of eight schema workers failed in the reproducing run.

Root cause:

- PostgreSQL's `IF NOT EXISTS` does not eliminate every catalog race when identical objects are created concurrently.

Fix:

- Added bounded, context-aware retry around each idempotent schema object creation.
- Retries are restricted to duplicate-object SQLSTATEs `23505`, `42P07`, and `42710`.
- Maximum is four retries with a 200 ms delay; permission and unrelated SQL failures still return immediately.

Validation:

- The same eight-process schema race passed on PostgreSQL after the fix.
- Unit regression tests exercise all three accepted SQLSTATEs.
- The complete PostgreSQL matrix then passed.

### 2. MySQL-family false lease loss on same-second renewal

Observed failure:

- Immediate owner renewal returned false on MySQL 8.4.
- A competing owner subsequently acquired while the original supervisor should have retained the lease.

Root cause:

- `go-sql-driver/mysql` reports changed rows by default, not matched rows.
- Unix-second timestamps can make a valid renewal write values identical to the existing row. MySQL then reports zero changed rows even though the owner and expiry guards matched.

Fix:

- Added a MySQL-family ownership-confirmation query after a successful renewal reports zero changed rows.
- Confirmation requires the same routine ID, the same owner, and a database-clock-live expiry.
- This avoids requiring a special DSN and preserves strict database-clock `UpdatedAt` semantics.

Validation:

- Focused unit tests cover both zero-change confirmed ownership and zero-change absent ownership.
- Full MySQL 8.4, MariaDB 11.8, and TiDB 8.5.3 matrices passed with default driver settings.
- The long renewal test stayed exclusive beyond the original TTL.

## Additional Hardening

### Parallel distinct-name coverage

The retained suite now proves both sides of distributed scheduling: contenders sharing one exact name remain exclusive, while unrelated names can execute concurrently without global serialization. The stress run reached exactly 64 simultaneous task bodies, with three supervisors competing for each name. It verifies exact names, distinct owners, running and final states, counters, log prefixes, database timestamps, and exactly one acquire plus one release history event per name. No additional production-code defect was found by this expanded campaign.

### Saturating counters

`SuccessCount` and `FailureCount` now saturate at `math.MaxInt64`. They cannot wrap negative and cause lease action validation to reject an otherwise valid long-lived supervisor. A dedicated unit test verifies both counters.

### Oracle index conflict validation

When Oracle reports an existing-object conflict for the history index, initialization now checks `USER_IND_COLUMNS` for the expected table, column, and leading position instead of accepting any index with the same name.

### Integration resource bounds

The real-backend connection pool is capped at 16 open and 8 idle connections. Oracle Free initially returned connection errors when an oversized client pool was combined with process-level tests. The bounded pool still schedules all 256 contender goroutines while respecting the edition's session ceiling; the full Oracle matrix then passed.

## Verification Commands

The final repository checks passed:

- `go test -count=1 ./...`
- `go test -race -count=1 ./...`
- `go vet ./...`
- Race-enabled, integration-tagged SQLite WAL matrix
- Integration-tagged full matrix for every dialect listed above
- Final focused reruns of the strengthened distinct-name and unfiltered-query assertions on every remaining engine/configuration

The integration suite is build-tagged, so normal library tests do not require database servers. The base invocation, defaults, and stress knobs are documented in the README; the larger overrides used for this run are recorded above. Driver DSNs and test credentials used during this run were local, temporary, and are intentionally omitted from this report.

## Files Added or Changed

- `integration_real_test.go`: reusable real-backend matrix, subprocess workers, unfiltered non-empty queries, and mixed distinct-name parallel supervisor stress.
- `sqllease.go`: PostgreSQL schema-race retries and MySQL-family zero-change renewal confirmation.
- `sqllease_test.go`: regression tests for both backend defects and Oracle schema validation.
- `unique.go` and `unique_test.go`: saturating counters and tests.
- `sqllog.go`: stricter Oracle index validation.
- `README.md`: integration reproduction instructions, version prerequisites, renewal semantics, and counter behavior.
- `go.mod` and `go.sum`: integration-only database drivers for PostgreSQL, MySQL-family engines, SQL Server, Oracle, and SQLite.

## Confidence and Limitations

The runs provide strong evidence that the current SQL, bindings, rows-affected handling, schema creation, queries, pruning, and normal-contention supervisor lifecycle work on the tested engines and versions.

They do not convert a time-based lease into a fencing or exactly-once protocol. A paused or partitioned old owner can continue external side effects after expiry if its task ignores cancellation. Correctness-critical writes still require transactions, locks, constraints, idempotency keys, or fencing tokens.

Additional boundaries:

- SQL Server was exercised on Microsoft's ARM64 Azure SQL Edge engine because current official SQL Server Linux containers are not available for this ARM64 host. The final run used Datadog's byte-identical mirror of Microsoft's official Azure SQL Edge 1.0.7 image after repeated MCR transfer stalls. Azure SQL Edge was retired in 2025. The exact T-SQL paths used by EasyRoutine passed, but a current x86_64 SQL Server edition should remain in CI for release qualification.
- The GaussDB dialect was exercised on openGauss 6.0.0, the available standalone Gauss-compatible engine. Managed Huawei GaussDB service behavior was not tested.
- No multi-node replication, forced database restart, packet loss, network partition, or server failover was injected.
- SQLite results apply to processes sharing one local filesystem safely; they do not establish cross-host filesystem safety.
- History recording is intentionally best effort and not transactionally coupled to lease updates.
- Existing schemas are created if missing but are not generally migrated or fully validated.

## Conclusion

All final real-backend matrices, strengthened focused reruns, and repository validation commands passed. Two earlier backend correctness defects were reproduced and fixed; the new parallel distinct-name campaign found no additional production defect. The resulting suite is retained in the repository so the same coverage can be rerun against local servers or CI services before future releases.
