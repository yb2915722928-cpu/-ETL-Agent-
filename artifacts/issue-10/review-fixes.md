# Review fixes for #8–#10

Baseline: `a64be0e3854070e4065ebdae3ab85aaa3c94a2c2` on `integration/issues-8-10`; implementation branch `implement/review-fixes`.

## Confirmed issues

- #8: baseline public approval returned 200 with a decision timestamp exactly equal to the immutable expiry after actual managed connector reads. The corrected route captures completion time immediately after those reads and denies `>= expires_at` before decision mutation. Both approve/reject return 409, produce no decision or `approval.decided`, preserve `expires_at`, and commit `approval.denied` in the same transaction. Reports: `artifacts/issue-8/review-expiry-red.json`, `review-expiry-green.json`.
- #10: baseline real Celery worker ACKed ordinary failures while Outbox was already sent: PostgreSQL outage left the same accepted Run queued; deferred SQLSTATE40001 after an external POST left its durable Attempt submitting. Explicit task retry now covers driver transport/connect failures and the enumerated transient connection/transaction SQLSTATEs, leaving constraint/data/programming errors on the normal failure path. Celery ACK policy is unchanged. Reports: `review-database-red.json`, `review-database-green.json`.
- Actual corrected task logs show the same task ID retry after 10 seconds and then succeed. The external-effect case persists the original Attempt/request/request digest/correlation/target ownership before POST, rolls back only the result transaction, and discovers the original job. Exactly one isolated Engine job exists; Capability consumption increases only once. No resubmit path or production fault endpoint was added.

## Compact standards cleanups

- One private submission-authorization helper replaces both duplicate current-role/local-facts/managed-resource sequences. Both calls and the original two transactions remain; the tenant lock still spans the final authorization check, bounded POST and result transaction.
- `scripts/isolated_api_fixture.py` shares disposable PostgreSQL Schema clone, HTTP dependency override/Uvicorn lifecycle and teardown for #8/#9 and the new database regression. Clock and fault control remain in individual fixtures; no test framework/dependency or production test helper was added. Mount the complete `scripts` directory at `/tests`, with `PYTHONPATH=/app:/tests`.
- Commit fixture now records original envelope validity before exercising the cloned L1 DB: valid originals still verify, and every original envelope remains unchanged, including intentionally bad L1 signatures from prior fault verification. Treating every retained L1 fault fixture as a valid signature was an incompatible pre-existing assumption, not a production regression.
- Frontend uses a single tab-to-breadcrumb/title map with all existing labels retained. Frontend built once.

## Commands and outcomes

Run from the root Compose workspace unless marked worktree. All commands exited 0 except the intentionally red database reproduction (exit 1, both expected regressions).

```powershell
# Worktree: final API and web builds, each once.
docker build --add-host mirrors.aliyun.com:124.95.190.69 -f infra/aliyun/runtime.Dockerfile --target api -t etl-agent-l1-api:issue10 .
docker build --add-host mirrors.aliyun.com:124.95.190.69 -f infra/aliyun/runtime.Dockerfile --target web -t etl-agent-l1-web:issue10 .

# Root: run once on baseline image (red report), then rebuilt image (green report).
python .worktrees/review-fixes/scripts/verify_execution_database_retry.py --output .worktrees/review-fixes/artifacts/issue-10/review-database-green.json

docker compose --env-file .runtime/l1/deployment.env -f compose.l1.yaml up -d --no-build api worker publisher web

# Root: bind the review worktree scripts; isolated schemas, production image.
docker compose --env-file .runtime/l1/deployment.env -f compose.l1.yaml run --rm --no-deps -T -e PYTHONPATH=/app:/tests -v '<review-worktree>/scripts:/tests:ro' api python /tests/verify_preparations_fixtures.py
docker compose --env-file .runtime/l1/deployment.env -f compose.l1.yaml run --rm --no-deps -T -e PYTHONPATH=/app:/tests -v '<review-worktree>/scripts:/tests:ro' api python /tests/verify_commits_fixtures.py

# Worktree: pure public recheck, no extra execution or root tracked evidence edits.
python scripts/verify_execution_recheck.py --runtime '<root>/.runtime/l1' --output artifacts/issue-10/review-restart-recheck.json
```

The approval red was reproduced before its production change, then green before and after shared fixture extraction. Commit checks preserve capability/Preparation completion-time denial, current roles, atomic deferred-trigger rollback, identical expired-token receipt retries, and public-key-only verification. Final reports are `review-commit-fixtures.json` and `review-restart-recheck.json`. The original 308.446-second queue test was not rerun; its durable evidence was reread.

Final image identities (production sources unchanged after build):

- `etl-agent-l1-api:issue10`: `sha256:a1a302d6fb1ad46d8618b8febd7b645038080893aed6d1bd3768a415ff0cadc3`
- `etl-agent-l1-web:issue10`: `sha256:62801deb93f0a2d2625fc355d290423823de7436f971091155f43fa3b0f98035`
- Vite 6.3.5: 23 modules; production JS `index-D2Q99QQ6.js` 141.87 kB / gzip 48.56 kB; build 1.06 s.
- Worktree source SHA256 equals the recreated API container source: `backend/app/celery_app.py` = `4b79ab17acc01ea650a5c2a000f7ecde07defec7d18ccc90200b05623f104dbf`; `backend/app/execution_runtime.py` = `e955af034306c69023aea400406c9c715bfd0cc4ff9fa0b8c28f2c3ac79eec78`; `backend/app/preparations.py` = `fca1ec13bfd53922c9b5ea358d57ead3a12d5adc4571ce1ea5698ac0abed02e9`.

## Retained runtime and scope

Both database runs used disposable clone/unique Redis queue/temporary SQLite Engine/public-key-only worker subprocesses and removed their schema, queue and processes. Brief real L1 PostgreSQL stop/start was the only shared dependency fault. Public Engine ledger equality was checked around each run: exactly two original durable jobs retained. Final recreated API/worker/Engine preserve those job IDs/correlations/ownership, blocked physical alias and metadata-only receipt with 308.446-second durable queue age. Ordinary deployment, unrelated PostgreSQL/TEI services and volumes were not altered. No real ETL, publish, cleanup, private worker signing seed or ticket closure was introduced. Parent owns final ordinary deployment/browser verification and ticket lifecycle.
