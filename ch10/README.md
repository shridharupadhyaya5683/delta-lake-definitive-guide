# Chapter 10. Performance Tuning: Optimizing your Data Pipelines with Delta Lake
> This chapter provides notebooks to explore what options are available to tune your Delta Lake tables for optimal performance.

This chapter covers more advanced features to squeeze the most out of your Delta lake tables.

## Databricks Free Edition
Both notebooks run on Databricks Free Edition (serverless) through `dldg.py`; no Docker needed.
- `chapter_10.ipynb`: the partitioning example is written locally with the `deltalake` package and copied to
  `/Volumes/workspace/dldg/work/ch10/partitioning.example.delta/`. The book's `delta.autoCompact.*`,
  `delta.optimizeWrites`, and `delta.targetFileSize` settings are applied as table properties
  (`delta.autoOptimize.autoCompact`, `delta.autoOptimize.optimizeWrite`, `delta.targetFileSize`), because
  they aren't Spark session configs. The liquid clustering and bloom filter examples build
  `workspace.example.wikipages` (about 5.8M rows, about 10 GB) from `/databricks-datasets/`, so they take a few minutes.
- `chapter_10_bonus.ipynb`: pure pandas and plotly, runs locally and writes `zorder.html`. `get_markdown()` needs `tabulate`.
