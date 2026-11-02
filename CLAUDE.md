# CLAUDE.md: Databricks / Delta Lake study project

## About me
- Shridhar. Technical role in data analysis and database management.
- Working through **Delta Lake: The Definitive Guide**, a weeks-long self-study of Delta Lake and Spark, and
  **Business Intelligence with Databricks SQL** (Packt).
- Write in American English. Keep answers crisp and to the point.
- **Current chapter: 10** (Delta Lake book).

## Environment
- OS: Ubuntu. IDE: PyCharm with the Claude Code plugin.
- Databricks CLI is installed and authenticated. Use profile `shridhar` (pass `--profile shridhar`).
- Workspace: Databricks **Free Edition**, **serverless** compute only.
- Host: `https://dbc-df11473d-eb7a.cloud.databricks.com`
- User: `shridhar.upadhyaya@gmail.com`
- SQL warehouse ID: `087e65d950eeac0f`
- Python: project venv at `.venv` with `databricks-connect` installed (serverless).
- Free Edition limits: no custom cloud storage (no S3 credentials or external locations); use UC volumes.
- Avoid Genie: it has strict quota limits.

## Unity Catalog layout
- Catalog: `workspace`
- Schemas: `dldg` (main study schema), `bronze`, `silver`, `default`, `example`, `exampledb`,
  `iot_medallion`, `netflix`, `on_shelf_availability`
- `workspace.dldg` tables (managed): `countries`, `covid_table`, `covid_table2`, `example_table`, `nyt`
  - `countries2` was a CTAS copy of `countries`; it was dropped (recoverable with `UNDROP` within 7 days).
  - Chapter 5 notebooks: `covid_nyt`, `nonoptimal_covid_nyt`, `covid_nyt_by_date`, `bronze_covid_nyt_by_date`,
    `silver_covid_nyt_by_date`, `silver_covid_nyt_by_date_clone` (main); `nyc_taxi`, `nonoptimal_nyc_taxi`,
    `nyc_taxi_by_day` (extras). `bronze_`/`silver_` prefixes stand in for the book's separate schemas.
- Chapter 9: `silver_ecomm_clickstream` (streamed from the path-based bronze table at
  `/Volumes/workspace/dldg/work/ch09/delta/bronze_raw/`; source is a UC volume landing folder instead of Kafka, see `ch09/README.md`).
- `workspace.dldg` volumes: `datasets`, `work`

## Volume paths (path-based Delta tables)
- `/Volumes/workspace/dldg/work/ch03/countries.delta/`: path-based Delta table (has deletion vectors).
  Separate from the managed `workspace.dldg.countries`; the two share nothing.
- `/Volumes/workspace/dldg/work/ch03/countries_1792436800.parquet/`: despite the name, this is a **Delta**
  table (it has `_delta_log`). Read it with `delta.`, not `parquet.`.
- `/Volumes/workspace/dldg/work/ch03/exampleDB.delta/`
- Other chapter folders: `/Volumes/workspace/dldg/work/ch07/`, `ch08/`, `ch09/`, `ch10/`

## Workspace notebooks
- Folder: `/Workspace/Users/shridhar.upadhyaya@gmail.com/Delta Lake The definitive guide`
  - `Chapter 3` (Python notebook)
  - `Chapter 3 SQL` (SQL query)
- Sync:
  ```bash
  databricks workspace export "/Workspace/Users/shridhar.upadhyaya@gmail.com/Delta Lake The definitive guide/Chapter 3" --format SOURCE --file chapter3.py --profile shridhar
  databricks workspace import "/Workspace/Users/shridhar.upadhyaya@gmail.com/Delta Lake The definitive guide/Chapter 3" --file chapter3.py --language PYTHON --format SOURCE --overwrite --profile shridhar
  ```

## How to run code
### SQL (Statement Execution API via CLI)
```bash
databricks api post /api/2.0/sql/statements --profile shridhar --json '{
  "warehouse_id": "087e65d950eeac0f",
  "statement": "DESCRIBE HISTORY workspace.dldg.countries",
  "wait_timeout": "30s"
}'
```

### PySpark (Databricks Connect, serverless)
```python
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.profile("shridhar").serverless().getOrCreate()

df = spark.read.format("delta").load("/Volumes/workspace/dldg/work/ch03/countries.delta/")
df.orderBy("id").show()
```
- `display()` is notebook-only; use `.show()` (or `.toPandas()`) locally.
- `DeltaTable.forPath(spark, path)` gives the table handle for `history()`, `update()`, `merge()`, and so on.

## Pasted snippets (my main workflow)
When I paste a SQL or PySpark snippet, without being asked:
1. **Syntax check**: flag errors and show the fix before running. Point out any `<placeholder>`s.
2. **Execute** it on Databricks: SQL via the warehouse, PySpark via Databricks Connect (`dldg.py`).
   No confirmation needed, even for destructive statements (see Working rules).
3. **Explain** the results: show the output table, then a short explanation tied to the chapter concept.

## Working rules
- Do tasks end to end: write the code, run it, check the output, and fix errors. Don't stop after each step.
- I edit files while you work. **Re-read the current file before answering questions about it or editing it.**
- Run only what I ask for. If a file holds several statements, don't run the whole file unless I say so.
- **Full Databricks authorization (granted 2026-10-23)**: you may make any and all changes to my Databricks
  workspace as you see fit, without asking first. That includes `DROP`, `DELETE`, `RESTORE`, `MERGE`, `UPDATE`,
  `OPTIMIZE`, `VACUUM`, overwrite writes, and workspace or notebook changes. Say what you changed in your reply.
- Always use fully qualified names (`workspace.dldg.<table>`) so the current schema doesn't matter.
- When you report results, show the output table and one or two lines on what it means.

## Known gotchas
- Table names: `workspace.dldg..countries` (double dot) causes a parse error.
- Reading a Delta folder with `parquet.` raises `DELTA_INVALID_FORMAT`; use `delta.` instead.
- In Python, a method call on its own line after a closing `)` is a syntax error. Chain it on the same line,
  or wrap the whole expression in parentheses.
- Path-based `RESTORE TABLE delta.\`<path>\`` affects only that path, never a same-named UC table.
- Single Delta DML statements auto-commit; there's no explicit `COMMIT`.
