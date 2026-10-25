"""Gather and visualize per-file statistics for a Delta table.

The table can be given three ways, and that decides where the stats come from:
    ch06/data/gen/filestats              local path    -> delta-rs replays _delta_log (stats as written in the log)
    /Volumes/workspace/dldg/work/...     volume path   -> only _delta_log is downloaded, then delta-rs as above
                                                          (falls back to a Spark scan if delta-rs can't read it)
    workspace.dldg.nyc_taxi              UC table name -> Spark scans the table grouped by _metadata.file_path
                                                          (Free Edition doesn't expose managed tables' storage)

Command line (from the repo root, with the project venv):
    python delta_stats.py ch06/data/gen/filestats --columns month average --open
    python delta_stats.py workspace.dldg.nyc_taxi --columns tpep_pickup_datetime trip_distance

Python (any chapter notebook; the repo root is on sys.path):
    from delta_stats import gather
    s = gather("./data/gen/filestats")
    s.print_report()
    s.to_html("filestats_stats.html")
    s.figures()[2].show()                 # any chart on its own

What the report covers:
    summary     version, protocol, partitioning, file count/size distribution, small files, data skipping settings
    columns     per column: type, stats coverage, min, max, nulls, and overlap (see below)
    partitions  files, rows and size per partition value
    history     the latest commits
    charts      file size and row count histograms, a min-max range chart per column, partition sizes

Overlap is the average number of files whose min-max range intersects a file's range, counting the file itself.
1.0 means no two files overlap, so a filter on that column can skip all but the matching files; a value near the
file count means every file spans the whole range, so stats can't skip anything (a Z-order/clustering candidate).
For partitioned tables it is also reported within each partition, since queries usually prune partitions first.
"""
import argparse
import json
import os
import re
import tempfile
import webbrowser
from dataclasses import dataclass, field
from html import escape

import numpy as np
import pandas as pd
import pyarrow as pa

UC_NAME = re.compile(r"^`?[\w-]+`?\.`?[\w-]+`?\.`?[\w-]+`?$")
# Spark types that have per-file min/max stats in Delta
STAT_TYPES = ("byte", "short", "int", "long", "bigint", "smallint", "tinyint", "float", "double", "decimal",
              "date", "timestamp", "timestamp_ntz", "string", "boolean")
# Delta's defaults for which columns get stats
DEFAULT_INDEXED_COLS = 32


@dataclass
class TableStats:
    name: str
    source: str  # "delta log" or "table scan"
    version: int
    partition_columns: list
    schema: list  # [(column, type)]
    files: pd.DataFrame  # path, size_bytes, num_records, partition.<p>, min.<c>, max.<c>, null_count.<c>
    history: pd.DataFrame
    protocol: dict = field(default_factory=dict)
    configuration: dict = field(default_factory=dict)

    # ---- tables -------------------------------------------------------------------------------------------------

    @property
    def stat_columns(self):
        """Non-partition columns that have min/max columns in `files`."""
        f = self.files
        return [c for c, _ in self.schema if c not in self.partition_columns
                and f"min.{c}" in f and (f[f"min.{c}"].notna().any() or f[f"max.{c}"].notna().any())]

    @property
    def stats_columns_in_log(self):
        """Non-partition columns the log has stats columns for, even if every file left them empty."""
        return [c for c, _ in self.schema if f"null_count.{c}" in self.files and c not in self.partition_columns]

    def summary(self, small_file_mb: float = 32) -> pd.Series:
        f = self.files
        size_mb = f["size_bytes"] / 2**20
        indexed = self.configuration.get("delta.dataSkippingStatsColumns") or \
            self.configuration.get("delta.dataSkippingNumIndexedCols", f"{DEFAULT_INDEXED_COLS} (default)")
        known = f["num_records"].notna()
        rows = {
            "table": self.name,
            "stats source": self.source,
            "version": self.version,
            "protocol": ", ".join(f"{k}={v}" for k, v in self.protocol.items() if v not in (None, [], "")),
            "partition columns": ", ".join(self.partition_columns) or "(none)",
            "partitions": f.groupby(self._partition_keys()).ngroups if self.partition_columns else 1,
            "files": len(f),
            "rows": f"{int(f['num_records'].sum()):,}" + ("" if known.all() else f" ({(~known).sum()} files without stats)")
                    if known.any() else "unknown (no stats in the log)",
            "total size": _human(f["size_bytes"].sum()),
            "file size min / median / max": " / ".join(_human(v) for v in f["size_bytes"].quantile([0, .5, 1])),
            "rows per file min / median / max": " / ".join(f"{v:,.0f}" for v in f["num_records"].quantile([0, .5, 1]))
                    if known.any() else "unknown",
            f"small files (< {small_file_mb:g} MB)": f"{(size_mb < small_file_mb).sum()} ({(size_mb < small_file_mb).mean():.0%})",
            ("columns with stats" if self.source == "delta log" else "columns scanned"):
                f"{len(self.stat_columns)} of {len(self.schema) - len(self.partition_columns)}",
            "stats columns setting": indexed,
            "deletion vectors": "no" if not self._has_deletion_vectors()
                else "enabled: num_records counts rows before deletes" if self.source == "delta log"
                else "enabled: rows are live rows (deleted rows excluded)",
        }
        return pd.Series(rows, name="value")

    def column_stats(self, columns=None) -> pd.DataFrame:
        f, types = self.files, dict(self.schema)
        out = []
        for c in columns or self.stats_columns_in_log or self.stat_columns:
            lo, hi, nulls = f.get(f"min.{c}"), f.get(f"max.{c}"), f.get(f"null_count.{c}")
            if lo is None:
                out.append({"column": c, "type": types.get(c), "stats coverage": "0%"})
                continue
            has_stats = nulls.notna() if nulls is not None else lo.notna() | hi.notna()
            row = {
                "column": c,
                "type": types.get(c),
                "stats coverage": f"{has_stats.mean():.0%}",
                "min": lo.min(),
                "max": hi.max(),
                "nulls": int(nulls.sum()) if nulls is not None and nulls.notna().any() else None,
                "null %": f"{nulls.sum() / f['num_records'].sum():.1%}"
                          if nulls is not None and nulls.notna().any() and f["num_records"].sum() else None,
                "overlap (all files)": _overlap(lo, hi),
            }
            if self.partition_columns:
                row["overlap (within partition)"] = self._overlap_within_partitions(c)
            if self.source == "table scan":
                # A scan computes stats for every column; Delta only keeps log stats (used for skipping) for these
                row["in log stats"] = "yes" if c in self._log_stats_columns() else "no"
            out.append(row)
        return pd.DataFrame(out).set_index("column")

    def partition_stats(self) -> pd.DataFrame:
        if not self.partition_columns:
            return pd.DataFrame()
        g = self.files.groupby(self._partition_keys(), dropna=False)
        p = g.agg(files=("path", "size"), rows=("num_records", lambda r: r.sum(min_count=1)), size_bytes=("size_bytes", "sum"))
        p["rows"] = p["rows"].astype("Int64")
        p.index.names = self.partition_columns
        p["size"] = p["size_bytes"].map(_human)
        return p.sort_values("size_bytes", ascending=False)

    def file_stats(self, column: str) -> pd.DataFrame:
        """Per-file view of one column's stats, sorted by min (what Cell 15 in ch06 builds by hand)."""
        cols = ["path", "num_records", "size_bytes", *self._partition_keys(),
                f"min.{column}", f"max.{column}", f"null_count.{column}"]
        return self.files[[c for c in cols if c in self.files]].sort_values(f"min.{column}")

    # ---- output -------------------------------------------------------------------------------------------------

    def print_report(self, columns=None, history: int = 10, partitions: int = 10, small_file_mb: float = 32):
        with pd.option_context("display.width", 200, "display.max_columns", 20, "display.max_colwidth", 60):
            _section("Summary", self.summary(small_file_mb).to_string())
            _section("Columns", self.column_stats(columns).to_string())
            if self.partition_columns:
                p = self.partition_stats()
                _section(f"Partitions (largest {min(partitions, len(p))} of {len(p)})",
                         p.drop(columns="size_bytes").head(partitions).to_string())
            if len(self.history):
                h = self.history.head(history).copy()
                if "operationParameters" in h:
                    h["operationParameters"] = h["operationParameters"].map(lambda v: _clip(str(v), 70))
                _section("History", h.to_string(index=False))

    def figures(self, columns=None, max_files: int = 500, small_file_mb: float = 32) -> list:
        """The report's plotly charts: file sizes, rows per file, a range chart per column, partition sizes."""
        import plotly.express as px

        figs = []
        size_mb = self.files["size_bytes"] / 2**20
        fig = px.histogram(size_mb, nbins=50, title="File sizes", labels={"value": "file size (MB)"})
        fig.add_vline(x=small_file_mb, line_dash="dash", annotation_text=f"{small_file_mb:g} MB")
        figs.append(fig.update_layout(showlegend=False))
        if self.files["num_records"].notna().any():
            figs.append(px.histogram(self.files["num_records"], nbins=50, title="Rows per file",
                                     labels={"value": "rows"}).update_layout(showlegend=False))
        for c in columns or self.stat_columns:
            if fig := self._range_chart(c, max_files):
                figs.append(fig)
        if self.partition_columns:
            p = self.partition_stats().head(50).reset_index()
            p["partition"] = p[self.partition_columns].astype(str).agg("/".join, axis=1)
            figs.append(px.bar(p, x="partition", y="size_bytes", hover_data=["files", "rows", "size"],
                               title=f"Largest {len(p)} partitions by size", labels={"size_bytes": "bytes"}))
        return [f.update_layout(template="plotly_white", height=f.layout.height or 420) for f in figs]

    def to_html(self, path: str, columns=None, max_files: int = 500, small_file_mb: float = 32) -> str:
        figs = self.figures(columns, max_files, small_file_mb)
        charts = "\n".join(f.to_html(full_html=False, include_plotlyjs="cdn" if i == 0 else False)
                           for i, f in enumerate(figs))
        tables = [("Summary", self.summary(small_file_mb).to_frame().to_html(header=False)),
                  ("Columns", self.column_stats(columns).to_html(float_format=lambda v: f"{v:,.4g}"))]
        if self.partition_columns:
            tables.append(("Partitions (largest 25)", self.partition_stats().drop(columns="size_bytes").head(25).to_html()))
        if len(self.history):
            h = self.history.head(25).copy()
            if "operationParameters" in h:
                h["operationParameters"] = h["operationParameters"].map(lambda v: _clip(str(v), 200))
            tables.append(("History", h.to_html(index=False)))
        body = "".join(f"<h2>{escape(t)}</h2>{h}" for t, h in tables)
        with open(path, "w") as fh:
            fh.write(_PAGE.format(title=escape(self.name), body=body, charts=charts))
        return os.path.abspath(path)

    # ---- helpers ------------------------------------------------------------------------------------------------

    def _has_deletion_vectors(self):
        return "deletionVectors" in str(self.protocol) or \
            self.configuration.get("delta.enableDeletionVectors", "").lower() == "true"

    def _log_stats_columns(self):
        """Columns Delta collects file stats for: delta.dataSkippingStatsColumns, else roughly the first N in the schema."""
        if listed := self.configuration.get("delta.dataSkippingStatsColumns"):
            return {c.strip().strip("`") for c in listed.split(",")}
        n = int(self.configuration.get("delta.dataSkippingNumIndexedCols", DEFAULT_INDEXED_COLS))
        names = [c for c, _ in self.schema]
        return set(names if n < 0 else names[:n])

    def _partition_keys(self):
        return [f"partition.{p}" for p in self.partition_columns]

    def _overlap_within_partitions(self, c):
        depths = [(_overlap(g[f"min.{c}"], g[f"max.{c}"]), len(g)) for _, g in self.files.groupby(self._partition_keys())]
        depths = [(d, n) for d, n in depths if d is not None]
        return sum(d * n for d, n in depths) / sum(n for _, n in depths) if depths else None

    def _range_chart(self, c, max_files):
        import plotly.graph_objects as go

        f = self.files.dropna(subset=[f"min.{c}", f"max.{c}"])
        if f.empty or not (pd.api.types.is_numeric_dtype(f[f"min.{c}"]) or pd.api.types.is_datetime64_any_dtype(f[f"min.{c}"])):
            return None  # strings and booleans: see the columns table
        shown = f.sample(max_files, random_state=0) if len(f) > max_files else f
        shown = shown.sort_values([*self._partition_keys(), f"min.{c}"])
        color_by = self._partition_keys()[0] if self.partition_columns and shown[self._partition_keys()[0]].nunique() <= 24 else None
        fig = go.Figure()
        for key, g in (shown.groupby(color_by, sort=False) if color_by else [("files", shown)]):
            y = [shown.index.get_loc(i) for i in g.index]
            fig.add_trace(go.Scatter(
                # a plain list keeps timestamps as timestamps (numpy would turn them into nanosecond integers)
                x=[v for lo, hi in zip(g[f"min.{c}"].tolist(), g[f"max.{c}"].tolist()) for v in (lo, hi, None)],
                y=np.repeat(y, 3), mode="lines+markers", name=str(key), line={"width": 3}, marker={"size": 4},
                text=np.repeat(g["path"].to_numpy(), 3), hovertemplate="%{text}<br>%{x}<extra></extra>"))
        title = f"{c}: min-max range per file  (overlap {_fmt(_overlap(f[f'min.{c}'], f[f'max.{c}']))}"
        if self.partition_columns:
            title += f", within partition {_fmt(self._overlap_within_partitions(c))}"
        title += ")" + (f"  [random {max_files} of {len(f)} files]" if len(f) > max_files else "")
        return fig.update_layout(title=title, xaxis_title=c, yaxis_title="file (sorted by partition, min)",
                                 height=max(420, min(1200, 6 * len(shown))), showlegend=color_by is not None)


# ---- gathering --------------------------------------------------------------------------------------------------

def gather(table: str, columns=None, history: int = 25) -> TableStats:
    """Collect stats for a local path, a /Volumes/... path, or a catalog.schema.table name."""
    if table.startswith("/Volumes/"):
        try:
            return _from_volume_log(table, history)
        except Exception as err:  # e.g. a table feature delta-rs can't read yet
            print(f"delta-rs couldn't read the log ({type(err).__name__}: {err}); scanning with Spark instead")
            return _from_spark(f"delta.`{table}`", columns, history)
    if UC_NAME.match(table) and not os.path.exists(table):
        return _from_spark(table, columns, history)
    return _from_delta_log(table, table, history)


def _from_delta_log(path, name, history):
    from deltalake import DeltaTable

    dt = DeltaTable(path)
    meta, proto = dt.metadata(), dt.protocol()
    files = pa.table(dt.get_add_actions(flatten=True)).to_pandas()
    hist = pd.DataFrame(dt.history(history))
    if len(hist):
        hist["timestamp"] = pd.to_datetime(hist["timestamp"], unit="ms")
        hist["engine"] = hist.get("engineInfo", pd.Series(index=hist.index, dtype=object)) \
            .fillna(hist.get("clientVersion", pd.Series(index=hist.index, dtype=object))).fillna("")
        hist = hist[[c for c in ("version", "timestamp", "operation", "operationParameters", "engine") if c in hist]]
    schema = [(f["name"], _type_name(f["type"])) for f in json.loads(dt.schema().to_json())["fields"]]
    return TableStats(
        name=name, source="delta log", version=dt.version(), partition_columns=list(meta.partition_columns),
        schema=schema, files=files, history=hist, configuration=dict(meta.configuration),
        protocol={"minReader": proto.min_reader_version, "minWriter": proto.min_writer_version,
                  "readerFeatures": proto.reader_features, "writerFeatures": proto.writer_features})


def _from_volume_log(path, history):
    from dldg import download_dir

    tmp = tempfile.mkdtemp(prefix="delta_stats_")
    # The stats live in the log, so the data files never need to leave Databricks
    download_dir(f"{path.rstrip('/')}/_delta_log", os.path.join(tmp, "_delta_log"))
    return _from_delta_log(tmp, path, history)


def _from_spark(name, columns, history):
    from pyspark.sql import functions as F
    from dldg import spark

    df = spark.table(name) if not name.startswith("delta.`") else spark.read.format("delta").load(name[7:-1])
    detail = spark.sql(f"DESCRIBE DETAIL {name}").first().asDict()
    parts = list(detail.get("partitionColumns") or [])
    types = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    stat_cols = columns or [c for c, t in types.items() if c not in parts and t.split("(")[0] in STAT_TYPES]
    stat_cols = stat_cols[:DEFAULT_INDEXED_COLS] if not columns else stat_cols

    aggs = [F.count(F.lit(1)).alias("num_records"), F.first("_metadata.file_size").alias("size_bytes")]
    aggs += [F.first(F.col(f"`{p}`")).alias(f"partition.{p}") for p in parts]
    for c in stat_cols:
        col = F.col(f"`{c}`")
        aggs += [F.min(col).alias(f"min.{c}"), F.max(col).alias(f"max.{c}"),
                 F.sum(col.isNull().cast("long")).alias(f"null_count.{c}")]
    files = df.groupBy(F.col("_metadata.file_path").alias("path")).agg(*aggs).toPandas()
    location = (detail.get("location") or "").rstrip("/") + "/"
    files["path"] = files["path"].str.replace(location, "", regex=False)

    hist = spark.sql(f"DESCRIBE HISTORY {name} LIMIT {history}").select(
        "version", "timestamp", "operation", "operationParameters", F.col("engineInfo").alias("engine")).toPandas()
    proto = {"minReader": detail.get("minReaderVersion"), "minWriter": detail.get("minWriterVersion"),
             "features": ", ".join(detail.get("tableFeatures") or [])}
    return TableStats(
        name=name, source="table scan", version=int(hist["version"].max()) if len(hist) else -1,
        partition_columns=parts, schema=list(types.items()), files=files, history=hist,
        configuration=dict(detail.get("properties") or {}), protocol=proto)


# ---- small helpers ----------------------------------------------------------------------------------------------

def _overlap(lo: pd.Series, hi: pd.Series):
    """Average number of files whose [min, max] intersects each file's, counting itself."""
    ok = lo.notna() & hi.notna()
    lo, hi = lo[ok].to_numpy(), hi[ok].to_numpy()
    n = len(lo)
    if n == 0:
        return None
    try:
        slo, shi = np.sort(lo), np.sort(hi)
        starts_after = n - np.searchsorted(slo, hi, side="right")  # files with min > this file's max
        ends_before = np.searchsorted(shi, lo, side="left")  # files with max < this file's min
    except TypeError:  # values that can't be ordered together
        return None
    return float(np.mean(n - starts_after - ends_before))


def _type_name(t):
    return t if isinstance(t, str) else t.get("type", "?")


def _human(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:,.1f} {unit}" if unit != "B" else f"{n:,.0f} B"
        n /= 1024


def _clip(text, n):
    return text if len(text) <= n else text[:n - 3] + "..."


def _fmt(v):
    return "n/a" if v is None else f"{v:.2f}"


def _section(title, text):
    print(f"\n=== {title} " + "=" * max(0, 76 - len(title)))
    print(text)


_PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>Delta stats: {title}</title>
<style>
body {{ font-family: system-ui, sans-serif; margin: 24px auto; max-width: 1200px; padding: 0 16px; color: #222; }}
h1 {{ font-size: 1.5rem; }} h2 {{ font-size: 1.15rem; margin-top: 2rem; }}
table {{ border-collapse: collapse; font-size: 0.85rem; margin: 8px 0; display: block; overflow-x: auto; }}
th, td {{ border: 1px solid #ddd; padding: 4px 8px; text-align: left; white-space: nowrap; }}
th {{ background: #f4f4f4; }}
p.note {{ color: #555; font-size: 0.9rem; max-width: 80ch; }}
</style></head><body>
<h1>Delta table stats: {title}</h1>
<p class="note">Overlap = average number of files whose min-max range intersects a file's range (itself included).
1.0 means a filter on that column can skip every non-matching file; close to the file count means stats can't skip
anything.</p>
{body}
<h2>Charts</h2>
{charts}
</body></html>
"""


def main():
    ap = argparse.ArgumentParser(description="Per-file statistics report for a Delta table.")
    ap.add_argument("table", help="local path, /Volumes/... path, or catalog.schema.table")
    ap.add_argument("--columns", nargs="+", help="columns to report and chart (default: all with stats)")
    ap.add_argument("--html", help="write the HTML report here (default: delta_stats_<table>.html)")
    ap.add_argument("--no-html", action="store_true", help="print the text report only")
    ap.add_argument("--open", action="store_true", help="open the HTML report in a browser")
    ap.add_argument("--max-files", type=int, default=500, help="files drawn per range chart (default 500)")
    ap.add_argument("--small-mb", type=float, default=32, help="files under this size count as small (default 32)")
    ap.add_argument("--history", type=int, default=10, help="commits to show (default 10)")
    args = ap.parse_args()

    stats = gather(args.table, args.columns, max(args.history, 25))
    stats.print_report(args.columns, args.history, small_file_mb=args.small_mb)
    if not args.no_html:
        slug = re.sub(r"[^\w.-]+", "_", args.table.strip("/")).strip("_")[-80:]
        path = stats.to_html(args.html or f"delta_stats_{slug}.html", args.columns, args.max_files, args.small_mb)
        print(f"\nHTML report: {path}")
        if args.open:
            webbrowser.open(f"file://{path}")


if __name__ == "__main__":
    main()
