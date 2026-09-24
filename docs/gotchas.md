# Gotchas — traps, with the ten-second check

Format: **symptom → real cause → ten-second check.** Silent traps first.

Entries above the seed line were paid for by earlier projects on this machine
(`~/.claude/templates/GOTCHA_SEED.md`, wrenfield-retail, wikiflow). Provenance is in brackets.

## Seeded

### Environment
1. **Containers OOM or flap despite plenty of host RAM.** WSL2 has a fixed share (32 GB on this
   machine). *Check:* `docker info --format "{{.MemTotal}}"`; the stack's `mem_limit` total must
   leave headroom for other stacks. [seed A2]
2. **Ports already taken at `up`.** Another project's stack, or the native Postgres on 5432.
   *Check:* `.\make.ps1 doctor` resolves the HOLDER of each port; ours vs foreign are different
   outcomes. [seed A5]
3. **`/bin/bash^M: bad interpreter` inside a container.** CRLF endings from a Windows checkout.
   *Check:* `.gitattributes` forces LF for everything except `*.ps1`. [seed A6]
4. **A PowerShell script breaks on an em-dash or writes UTF-16.** PowerShell 5.1 reads BOM-less
   UTF-8 as cp1252 and writes UTF-16 by default. *Check:* `.ps1` files stay ASCII-only; pass
   `-Encoding utf8` when writing files other tools read. [wrenfield #5]
5. **Paths break at "data engineer".** The repo path contains a space. *Check:* quote every
   path; ruff `PTH` keeps Python on `pathlib`. [wikiflow ADR-0004]

### Shell and parsers
6. **Backticks or `$` in an unquoted heredoc run as commands.** *Check:* quote the delimiter
   (`<<'EOF'`) unless expansion is wanted. [seed B10]
7. **Run parameters spliced into shell text.** *Check:* pass them through env vars, never
   interpolate them into a command string (CI does this for PR titles). [wrenfield #9]

### Git
8. **A `!` re-include in `.gitignore` is silently inert.** A bare directory exclusion stops git
   descending. *Check:* `git check-ignore -v <path>` in both directions (verified at init for
   `data/olist.sha256` and `automation/runs/**/*.json`). [seed C14]
9. **A hook that is not executable is skipped in silence.** *Check:* `git ls-files -s .githooks`
   shows `100755`. [seed C15/C18]
10. **A sha256 pin and `eol=lf` normalisation fight.** *Check:* never pin the hash of a file git
    normalises; the Olist CSVs are pinned but never committed. [seed C17]

### Packaging
11. **A dependency add moved a major version somewhere else.** *Check:* diff `uv.lock` after
    every dependency change; an unexpected moved package is a finding. [seed D20]

### Containers and storage
12. **A storage 403 that reads like a wrong password.** Often a missing LIST permission on the
    bucket. *Check:* the policy includes `s3:ListBucket`. [seed E43]
13. **A `chown -R` at the end of a Dockerfile doubles the image.** *Check:* create the user
    before installing anything. [seed E34]
14. **A named volume mounted into Airflow is root-owned, and the first write fails with an
    error that looks like something else.** *Check:* the init service chowns the exact mount
    target. [wrenfield CLAUDE.md]

### Airflow 3
15. **A manual trigger sits `queued` forever after `down -v`.** Every DAG is paused again.
    *Check:* `select dag_id, is_paused from dag`. [wrenfield #1]
16. **A bare cron schedule has a zero-width data interval.** Airflow 3 defaults to
    `CronTriggerTimetable`. *Check:* period DAGs declare `CronDataIntervalTimetable`. [wrenfield #2]
17. **`DagBag.get_dag()` hits the metadata DB even in a parse test.** *Check:* tests read
    `bag.dags[...]`. [wrenfield #6]
18. **Backfill ignores the DAG's `max_active_runs`.** *Check:* pass `--max-active-runs 1`
    explicitly. [wrenfield CLAUDE.md]
19. **2.x tutorials describe a different product.** *Check:* the official 3.x reference compose
    and the live binary's `--help`, not blog posts. [wrenfield #7]

### Verifiers
20. **A CLI exits 0 for a run that failed.** *Check:* assert on the artifact the run exists to
    produce, not the exit code. [seed F45]
21. **A guard that fires on correct behaviour.** *Check:* assert properties (a count that must
    be zero, an identity), never this month's values. [seed F50]

---
<!-- ======== SEED LINE: entries below are earned by THIS project ======== -->

## Earned here

22. **A PowerShell helper named `Uv` recursed forever instead of running `uv`.** PowerShell
    command names are case-insensitive, so the function shadows the executable. *Check:* helper
    functions use a verb-noun name (`Invoke-Uv`). (M0-S1)
23. **`docker ps --format '{{.Label "x"}}'` returns nothing from Windows PowerShell 5.1.** It
    strips the inner quotes when passing the argument, and the template fails silently.
    *Check:* use `{{.Labels}}` and parse `key=value`. (M0-S3)
24. **Killing a venv `python.exe` leaves the server running.** On Windows the venv interpreter
    is a launcher that starts the real Python as a child process. *Check:*
    `Get-NetTCPConnection -LocalPort N` → the owning PID's command line. (M0-S3)
25. **Lakekeeper warehouse JSON copied from older examples is rejected or ignored.** In v0.13
    the S3 credential fields are `access-key-id` and `secret-access-key`, and the flavor is
    `s3-compat`. *Check:* `docker run --rm quay.io/lakekeeper/catalog:<tag> management-openapi`
    is the schema for that exact tag. (M0-S3)
26. **`compose up` does not re-run a one-shot service that already exited 0**, so a second
    `up` never exercised the bootstrap. *Check:* prove idempotence with
    `docker compose run --rm <one-shot>`. (M0-S3)
27. **Every Postgres connection from the Windows host takes seconds, or hangs.** `localhost`
    resolves to `::1` first, and libpq waits for that attempt to time out, because the stack
    publishes ports on `127.0.0.1` only. *Check:* connect to `127.0.0.1` (instant) versus
    `localhost` with `connect_timeout=5` (exactly 5 s). Host-side clients use `127.0.0.1`. (M1-S1)
28. **`docker compose exec kafka /opt/kafka/bin/...` fails with "C:/Program Files/Git/opt/...
    no such file".** Git Bash rewrites arguments that look like absolute POSIX paths.
    *Check:* `export MSYS_NO_PATHCONV=1` first, or run the command from PowerShell. (M1-S2)
29. **`docker compose up --wait` fails with "container … exited (0)" on a healthy stack.** A
    one-shot service that nobody depends on counts as failed once it exits, even with exit 0.
    It passes by luck while the one-shot is still running. *Check:* every one-shot is named by
    some service's `depends_on: condition: service_completed_successfully`. (M2-S1)
30. **Great Expectations reports 0 violations on a batch that has hundreds.** Two sources of
    this, which together read as "all good": (a) GX puts an evaluation error inside the result
    instead of raising, and a missing `unexpected_count` read as 0; (b) two streaming queries
    shared one GX context, so `get_batch` swapped their dataframes and a rule ran against the
    other query's rows. A Checker per query did NOT fix (b): GX resolves datasources through a
    process-wide singleton (`ProjectManager`, the most recently created context), so the
    contexts were never separate. *Check:* `lake.bronze.quality_metrics` rows with `NOT agree`,
    and the job log for `GX could not evaluate`. Fix: one lock around every GX validation in
    the process, and treat NULL as a disagreement. (M2-S2)
31. **The PR checks you watched were from the wrong run.** After `gh pr edit`, `gh pr checks
    --watch` can show the `edited` run, where every job but story-lint is skipped by design.
    *Check:* confirm the full suite on the head commit (or on main after merge) before calling
    it green. (M2-S1)
32. **Every CDC delete lands on a NULL key, and deleted rows stay live in silver.** A
    Debezium delete's `after` is JSON `null`, which is a JSON value, not SQL NULL, so
    `coalesce(json_extract(p,'$.after'), json_extract(p,'$.before'))` picks the null object.
    All deletes then collapse into one keyless row. *Check:* `not_null` on every key column of
    every silver table, and choose `before`/`after` by `op`, never by coalesce. (M3-S1)
33. **The gate quarantined a defect, yet the bad value is in silver.** With REPLICA IDENTITY
    FULL, every later update re-sends the whole row, bad value included. The gate refuses the
    message that *introduced* the defect, not the value. *Check:* silver's `_dq_issues` column
    (row-level checks on current state); gold filters on it. (M3-S1)
34. **A currency conversion is a cent off on a handful of rows.** `sum()` widens a
    `decimal(12,2)` to `decimal(38,2)`. Multiplying that by a `decimal(18,10)` rate needs more
    than 38 digits, so Trino silently reduces the product's scale, and the later `round(x, 2)`
    rounds twice (18.1149996 became 18.115, then 18.12). *Check:* cast aggregates back to a
    small precision before multiplying, and test `amount = round(brl * rate, 2)` on every
    row. (M3-S2)
35. **Seven silver models fail with "source and target schemas are out of sync".** A column
    added in a SHARED macro (`_dq_issues` in `cdc_current_state`) changes every model that
    uses it, but only the models you rebuilt got the column. `on_schema_change: fail` is doing
    its job. *Check:* after editing a macro, `dbt ls --select` its dependents, and full-refresh
    all of them, not only the ones you were working on. (M3-S2, left over from M3-S1)
