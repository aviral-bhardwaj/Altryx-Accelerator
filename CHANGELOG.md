# Changelog

## [1.1.0] — 2026-10-29

### Breaking changes
- Renamed the top-level `src` package to `altryx_accelerator`; consumers must update imports. Moved the `pyspark_generator` and `validation` compatibility facades into the new package. `convert.py` is a thin wrapper; `altryx-convert` targets the packaged CLI and works from any cwd.

### Converter correctness
- Implemented DynamicRename from Designer XML: FirstRow, Formula via expression_parser, Add/Remove Prefix/Suffix, case changes, RightInputRows mapped/positional lookups and right-input field names. Recognizes the customer's Targets/Source input anchors and selected/unknown field rules. FirstRow removes exactly one row without mutating the upstream branch.
- Audited all four sample workflows: the customer flow includes 18 DynamicRename tools and ten joins. Fixed join-side selection and duplicate-column addressing, case-insensitive expression keywords/functions, string concatenation, Boolean formula branch coercion and User namespace workflow constants. Parse input field metadata for synthetic Spark fixtures.
- Generated SOURCE_TABLES and SOURCE_LABELS configuration cells enumerate external inputs. Exact tool IDs precede annotation/path-substring mappings; existing JSON CLI/notebook configuration is preserved. Removed inline TODO table reads. Windows Excel inputs also require UC mapping, because the customer lookup workbook is not delivered and Excel connectors are not bundled.
- Browse remains an inspection passthrough; annotations/containers are excluded from structural coverage. Partial tools remain explicitly warned executable fallbacks; GenerateRows now produces an empty frame rather than fabricated records. No claim of semantic completion for partial tools.
- Made ipynb,py the CLI and interactive notebook default; kept the job's documented ipynb-only Workspace collision workaround.

### Distribution and Marketplace
- Ship self-sufficient wheel package data with repo/explicit configuration overrides, version 1.1.0, build/dev dependencies and isolated-Python test compatibility.
- Four self-contained Marketplace notebooks install the Volume wheel, same-version GitHub Release fallback, or a hidden base64 wheel under 300 KB. Python restart is guarded locally and following cells reload state; no Git-folder bootstrap remains. Offline fallback requires dependencies already provisioned.
- Added synthetic-source Quickstart, source mapping/execution tutorial and widget-driven Batch Migration notebook; the bundle uses notebook 04 and the renamed standalone validation task. Runtime validation is opt-in for jobs until real sources are mapped.
- Rewrote publishing around supported Notebooks/Files assets in a Delta Sharing share, Private Exchange acceptance, public-provider prerequisites and the lack of partner-listed Apps. Added an idempotent Databricks SDK publisher and workspace-free dry-run validation; notebook bytes are attached as NOTEBOOK_FILE objects, not merely copied into a Volume.
- Listing provider is Vrahad Analytics; kept the support contact; set the privacy-policy URL with an explicit pre-submission check. Added original Pillow-generated 512x512 icon and README.pdf. Publication/audience selection remains in Provider Console.
- CI builds/uploads the wheel, attaches it to v* GitHub Releases, runs with/without Spark, validates publishing assets offline and preserves offline bundle checks.

### Verification and decisions
- Added per-mode DynamicRename syntax/data tests, customer-flow execution with metadata-based stub tables, source mapping precedence and mocked SDK reconciliation tests. Fixed an existing Spark-only test to expect TextInput's numeric auto-typing (the implementation already inferred integers); kept the original Skill Mode Run All assertions intact.
- Commit deterministic generated ipynb/py/report JSON artifacts under tests/output; timestamps omitted and notebook cell IDs stable. CI regenerates them and rejects drift. The absent customer Excel lookup is represented only by an explicitly synthetic fixture: execution/schema coverage does not prove parity with real customer data.
- Local verification: 336 passed with PySpark 3.5.3; 307 passed and 24 skipped in a fresh venv with PySpark absent. Added executable-fallback coverage for all eight partial tools and a Quickstart Run All test using an isolated wheel installation. The final built/embedded wheel was rechecked after the CLI banner correction; the temporary approval-service limit cleared and the final Spark suite passed again.
- Validation gates cover all samples, wheel installation/console execution from another cwd, local notebook Run All, Spark-present/absent suites, dry-run publishing and source/secret scans. Workspace authentication, live Delta Sharing consumption, cluster-specific installation and Private Exchange/public publication require a Databricks workspace and remain external acceptance checks.

## [1.0.1] — 2026-07-02

### Added
- Data-level end-to-end tests (`tests/test_e2e_spark.py`): generated notebooks
  are executed on a real Spark session and output values are asserted against
  hand-computed Alteryx semantics (filter True/False ports, join key dedup,
  aggregations, IF/ELSEIF/IIF formulas, string functions, casts, unions).
- Notebook "Run All" test (`tests/test_notebook_runall.py`): executes every
  code cell of the Skill Mode notebook and verifies exported artifacts.
- README: full step-by-step usage guide (Databricks UI, local CLI, CI/CD),
  source-table mapping walkthrough, and troubleshooting table.

### Fixed
- CI "Bundle validate" job no longer fails when `DATABRICKS_HOST`/
  `DATABRICKS_TOKEN` secrets are missing — it falls back to offline bundle
  structure checks with a clear notice; full `databricks bundle validate`
  runs automatically once secrets are configured.

## [1.0.0] — 2026-07-02

### Added — "Skill Mode" release
- **Single Skill Mode notebook** (`notebooks/01_Skill_Mode_Alteryx_to_Databricks.ipynb`)
  replacing the previous fragmented per-stage notebooks: end-to-end
  upload → parse → visualize DAG → convert → self-correct → export → bundle deploy.
- **Plugin-style tool registry** (`altryx_accelerator/tools/`): new Alteryx tools are added as
  Python classes with `@register("ToolType")` — no core changes required.
- **YAML-driven engine** (`config/tool_mapping.yaml` + `altryx_accelerator/converter_engine.py`)
  with per-tool conversion status tracking and graceful fallbacks for
  unsupported tools.
- **Self-correcting validation loop** (`altryx_accelerator/self_correction.py`): AST-based
  structural validation of generated PySpark against the original `.yxmd` DAG,
  strict-mode regeneration on FAILED, optimization rules (broadcast/Delta/ZORDER)
  on WARNING, up to 3 iterations, validation report embedded as the notebook's
  top `%md` cell.
- **Databricks exporter** (`altryx_accelerator/databricks_exporter.py`): `.ipynb` (nbformat),
  Databricks source `.py`, and `.dbc` archive output, plus recursive batch mode
  mirroring folder structures.
- **Databricks Asset Bundle** packaging: root `databricks.yml` with
  `dev`/`staging`/`prod` targets, `resources/alteryx_migration_job.yml`
  multi-task migration workflow, and a validation gate (`altryx_accelerator/job_validate.py`).
- **CI/CD**: `.github/workflows/bundle-deploy.yml` — tests + `bundle validate`
  on every push, deploy on `main`/manual dispatch.
- **Marketplace readiness**: `marketplace/` listing metadata and publishing
  guide, MIT license, `pyproject.toml` packaging, sample workflows under
  `tests/sample_workflows/`.

### Changed
- Parser now prefers `lxml` (stdlib `xml.etree` fallback).
- Input converters emit escape-safe string literals and flag local
  `.yxdb`/Windows-path sources as Unity Catalog mapping TODOs instead of
  generating broken code.
- `convert.py` gained `--self-correct`, `--format py|ipynb|dbc`, and `--batch`.

### Compatibility
- `altryx_accelerator.pyspark_generator` and `altryx_accelerator.validation` remain as import shims; the
  full 271-test suite passes unchanged.
