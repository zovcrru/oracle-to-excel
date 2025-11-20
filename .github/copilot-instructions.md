# Copilot / AI agent instructions for oracle_to_excel

Short, focused guidance so an AI can be productive immediately.

- **Big picture**: CLI tool that exports DB table rows to Excel. Entry point: `src/oracle_to_excel/main.py` (console script `oracle-to-excel` in `pyproject.toml`). Core subsystems: configuration (`src/oracle_to_excel/config.py`, alternative `env_config.py`), logging (`src/oracle_to_excel/logger.py`), DB layer (`src/oracle_to_excel/database.py` and `database_refactored.py`), DB-specific query builders (`src/oracle_to_excel/queries/*`), type transformers (`src/oracle_to_excel/transformers.py`). Tests live in `tests/`.

- **Runtime / environment**:
  - Requires Python >= 3.14 (see `pyproject.toml`).
  - Install deps and dev extras for full test/run: `pip install -e .[dev]` (Windows venv: `.venv\\Scripts\\activate.bat`).
  - Run the CLI after install: `oracle-to-excel` (or `python -m oracle_to_excel` once package installed).

- **Configuration**:
  - App reads `.env` via `src/oracle_to_excel/config.py`. Use `DB_TYPE` and `DB_CONNECT_URI` (mandatory).
  - `config.py` masks sensitive values by storing originals under `_original_{KEY}` — keep this convention when editing code that logs or mutates config.
  - `config.py` supports `--create-example`: `python -m oracle_to_excel.config --create-example` to produce `.env.example`.
  - There is a Pydantic-based variant (`env_config.py`) implementing `Settings` — it coexists; be careful which one you change. Current runtime (`main.py`) imports `config.py`.

- **Database layer notes**:
  - `database.py` is the module used by `main.py`. `database_refactored.py` is a newer/alternate implementation (do not swap without updating imports in `main.py`).
  - Top-level imports for `psycopg` and `oracledb` will raise if those packages are missing — tests and static analysis may require installing them even when exercising only SQLite paths.
  - Oracle thick-mode defaults point to `D:\\instantclient_12_1` on Windows; initialization is best-effort but will raise if `oci.dll` missing. Avoid hardcoding paths when adding features.
  - Query patterns: each DB has a query builder and stream executor in `src/oracle_to_excel/queries/`. Use `queries.base.build_query` and `queries.base.execute_query_stream` to keep DB-agnostic logic.

- **Type conversions & Excel**:
  - Transform DB-native types to Excel-safe values in `transformers.py` (dates → ISO strings, Decimal → float, LOBs read()). Prefer using `transform_row` for row conversions.

- **Logging / instrumentation**:
  - Centralized logging in `logger.py` — use `get_logger('<module>')` and `setup_logging()` for bootstrapping. The project includes sensitive-data masking filters; preserve them when adding new log output.
  - Use `@log_execution_time` or `@log_function_call` for performance visibility where appropriate.

- **Tests / CI**:
  - Tests are run with `pytest` (configured in `pyproject.toml`). Run locally with `pytest` after installing deps.
  - Because DB adapters are imported at module import time, install main dependencies before running tests: `pip install -e .` or `pip install -r requirements-dev.txt` + runtime deps.

- **Style & tooling**:
  - Follow `pyproject.toml` settings: ruff as linter/formatter rules and `line-length=100`.
  - Keep changes minimal and avoid reformatting unrelated files.

- **When making changes (practical rules for an AI developer)**:
  - Edit `config.py` only if the change is intended for the current runtime; `env_config.py` is Pydantic-based and can be used as reference.
  - If touching the DB layer, prefer small, focused refactors; note `database_refactored.py` and keep `main.py` compatibility.
  - Preserve `_original_` masking and `mask_sensitive` behavior when adding logging or CLI flags that print config.
  - Add tests under `tests/` for any behavioral changes and run `pytest` locally.

If anything above is unclear or you want CI-specific commands (GitHub Actions), tell me which bits to expand.
