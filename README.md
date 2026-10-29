# Dagster dbt Cloud null-job proof (local assembly complete)

Candidate `c824e2ed45f16cbde49458638821f7eef6d76433` / tree `cdc032f582f52f73ac5f8359c858061dbaa85aff`, based on `9f23277a83945062eec5f7d9c3b052bc91b5c7c6`.
This local packet is **READY for root final publication review only**. Hosted Snowflake and post-PR maintainer/CI gates remain separate.

When run details contain `job: null`, reading the job resource preserves `generate_docs` so artifact selection uses the right dbt command step. The original #34318 fix/test commits retain bunnysayzz's public Git author names, emails, dates and `-x` trailers; this follow-up also covers artifact-step selection. See SOURCE_PROVENANCE.json, original-source-commits.patch and LICENSE (Apache-2.0).

The source TCP/HTTP fixture produced 0/4/8 successful cases for baseline/original34318/candidate; the installed wheel also passed8. These requests used loopback, not a cloud account. Actual local dbt1.7.19 / DuckDB adapter1.7.5 build and docs artifacts are included with their logs. The four entry scripts are byte-identical to the executed copies. runtime/portable-RUNS.json records actual existing-environment Python commands; a fresh uv setup was not executed here.

Official whole `just -f justfile ty` expanded to `python scripts/run-ty.py --all`: outerexit0, airflow286 + master4625 =4911 files, 0errors/0warnings. Final setup used Python3.12.14, ty0.0.91, an executable TMPDIR and uv copy mode; an earlier noexec startup failure and an interrupted installation remain private historical evidence. A 38.49s installation pause was recorded. Rust was not installed; the actual frozen packages installed successfully. This proves the recorded command, not identity with every hosted CI image.

The nine downstream checks passed (2014 tests,3 original skips). Docs processed1115 symbols with71 packages declared,68 built-in import warnings,0errors and1 docstring warning. It does not establish successful imports or checking of all71 packages. The original skips and full warning output remain in the logs.

All seven DG scopes have complete passing candidate executions. The first valid slow1 batch failed one stderr assertion despite RUN_SUCCESS (796pass/1fail/2originalskip across that batch). Two later explicit full5-case candidate executions passed, with and without the pure startup observer. Root accepted this scope using those executions; the original failure log is retained. Its exact cause is unproven; a single default tail-mechanism probe did not reproduce it. Baseline success is diagnostic only. No automatic retry, assertion change or tail-delay override was added. See dg/QUALIFICATION.json, dg/EXECUTIONS.json and all original failure/control logs.

The final source archive contains633 raw-Git-byte-matching payload files plus1 generated PKG-INFO, including final regression test SHA `71e9f248f79f1499662300b143a76c2bc931ba93751008104fd18a498872d27b`. All56 installed wheel payloads equal the wheel bytes. The production file matches raw Git SHA `229b3d639533f33a61d0990ddde66db8b95d5cbbea77142de8b7308feda8ab1d`;49 other source files require CRLF-to-LF normalization for their Git comparison. These are distinct bindings.

## Reproduce the local loopback checks

Use a compatible existing Dagster/dbt1.7 environment; commands below are portable equivalents, not a claim that fresh installation ran:

```sh
python compare_http.py --repo /path/to/candidate-checkout --output ./reproduced-http
python generate_artifacts.py --output ./reproduced-artifacts
PYTHONPATH=./reproduced-http/variants/candidate DAGSTER_DBT_FIXTURE_DIR=./reproduced-artifacts/real-artifact-fixture python reproduce_real_artifacts.py --label candidate --output ./reproduced-artifacts
```

The comparison script sets variant imports only for its own child processes; those environment variables do not carry into the parent shell. The last command above therefore explicitly selects the copied candidate source and generated artifact fixture. In PowerShell, after the first two commands, use:

```powershell
$env:PYTHONPATH = (Resolve-Path ./reproduced-http/variants/candidate).Path
$env:DAGSTER_DBT_FIXTURE_DIR = (Resolve-Path ./reproduced-artifacts/real-artifact-fixture).Path
python reproduce_real_artifacts.py --label candidate --output ./reproduced-artifacts
```

An optional fresh-environment setup example from the package directory is `uv sync --frozen --no-default-groups --group test --group test-dbt17 --python 3.12`; this setup command was not run by the portable-entry check.

CI_SCOPE.json rebuilds the actual121 check list. Core103 passed: the final V2 raw-node audit validates all103 actual zero-exit jobs, full19 exact marker unions and14576 tracked-file before/after bindings. See core/FINAL_NODE_COVERAGE_AUDIT_V2.json, core/logs, core/reference and core/source. core/DELIVERY.json and core/history preserve earlier actual failures and their later full-scope executions, including the two earlier HDD asset-check failures; exclusive I/O causation and a proven flake are not claimed. Repository original skips and configured --reruns3 remain visible; no extra assertion retry was added. No old targeted-two-file ty or stale scoped readiness manifest is reused.

The submitted branch stays at `c824e2ed45f16cbde49458638821f7eef6d76433`. A separate prospective merge with upstream `ab2572e65545f9f29b1af3556f41a1101f328012` (actual upstream tree `9d36b54573bb7c1a45bbd2958d6a99ae71344150`) is `ea452dc9e379d8d10f61c4a0df7378de5f33b252` / tree `8f26fcadce62050c5f8f0c764968a4a04457eb13`. The contribution still selects121 checks; the new upstream core protobuf job is not selected by these two changed paths, and no repository requirement to continuously rebase or repeat all120 on every master advance was found. The four logical dbt compatibility ranges use full upstream-changed core files and the full cloud asset file at dbt1.7/1.12; actual process boundaries, original skips, preserved aggregate/baseline diagnostics and new upstream nodes are in integration/CORE_DELIVERY.json and its full logs. Separate whole ty/Ruff/original official docs and the two complete DG files (14pass) ran on the14577-file integration tree. The official docs setup passed with1115symbols/68import warnings/0errors/1warning. An additional803-package editable master environment reported4docs errors/2warnings in unchanged Azure/Snowflake files; that failure is retained in integration/quality/official-docstrings.log. It is not a baseline waiver or a claim every recorded command was green. Old120 qualification and old package artifacts bind c824; they were not relabelled as full120 executions or package builds on the prospective integration.

Two dbt112 rest/CLI original Tox JSON files were overwritten by a Windows case-insensitive filename collision. Their direct pytest return codes are not independently retained and remain null; complete original62/8 PASS stdout, actual host Tox exit0 with unchanged propagation, collections and source bindings are retained. No original JSON was reconstructed and no passed test rerun. The passing dbt17 aggregate also emitted SQLAlchemy closed-database cleanup stderr; its complete native stderr is retained. See integration/CORE_DELIVERY.json and integration/core/native.

Raw text logs retain every line; only literal private path prefixes are normalized. PROVENANCE.json records each source SHA, public SHA and transformation count. SHA256_MANIFEST.json covers this packet; the manifest is not self-hashed. No publication, hosted-service run or human review is claimed by this generator.

Original hash fields in normalized producer reports still describe their original source bytes. Explicit `publicPath` / `publicSha256` pairs describe packet-root-relative files after normalization; `sourceRawPath` / `sourceRawSha256` preserve the separate original receipt reference. PROVENANCE.json connects both byte sequences. Linux path normalization matches absolute path tokens, preserving relative evidence-directory names.
