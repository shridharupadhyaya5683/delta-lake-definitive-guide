"""Shared helpers for running the book's exercises against Databricks Free Edition (serverless).

Usage in a notebook or script:
    from dldg import spark, DATASETS, WORK
    df = spark.read.format("delta").load(f"{DATASETS}/COVID-19_NYT")

The notebooks were written for the book's Docker image, so this module also provides
replacements for things that don't exist when Spark runs remotely:
    /opt/spark/data/datasets  -> DATASETS
    /opt/spark/work-dir       -> WORK
    spark.conf.set            -> set_conf (serverless rejects most Spark configs)
    !tree / rm -rf            -> tree() / rm() on volume paths
"""
import os

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

PROFILE = "shridhar"
CATALOG = "workspace"
SCHEMA = "dldg"
# Local ./datasets is mirrored here
DATASETS = f"/Volumes/{CATALOG}/{SCHEMA}/datasets"
# Scratch space for path-based tables, checkpoints and files the exercises write
WORK = f"/Volumes/{CATALOG}/{SCHEMA}/work"

if "DATABRICKS_RUNTIME_VERSION" in os.environ:
    # Inside a Databricks notebook (e.g. a Git folder in the browser): reuse its session and credentials
    spark = DatabricksSession.builder.getOrCreate()
    w = WorkspaceClient()
else:
    spark = DatabricksSession.builder.profile(PROFILE).serverless(True).getOrCreate()
    w = WorkspaceClient(profile=PROFILE)
spark.sql(f"USE {CATALOG}.{SCHEMA}")


def set_conf(key: str, value: str) -> None:
    """spark.conf.set that warns instead of failing for configs serverless doesn't allow."""
    try:
        spark.conf.set(key, value)
    except Exception:
        print(f"Skipped {key}={value}: not settable on serverless compute")


def tree(path: str, indent: str = "") -> None:
    """Print a volume directory tree (replacement for `!tree`)."""
    for entry in sorted(w.files.list_directory_contents(path), key=lambda e: e.path):
        name = entry.path.rstrip("/").rsplit("/", 1)[-1]
        print(f"{indent}{name}{'/' if entry.is_directory else ''}")
        if entry.is_directory:
            tree(entry.path, indent + "    ")


def rm(path: str) -> None:
    """Recursively delete a volume directory if it exists (replacement for `rm -rf`)."""
    try:
        entries = list(w.files.list_directory_contents(path))
    except NotFound:
        return
    for entry in entries:
        if entry.is_directory:
            rm(entry.path)
        else:
            w.files.delete(entry.path)
    w.files.delete_directory(path)


def upload_dir(local: str, remote: str) -> None:
    """Copy a local directory (e.g. a table written with the `deltalake` package) into a volume."""
    for root, _, files in os.walk(local):
        for name in files:
            src = os.path.join(root, name)
            dst = f"{remote.rstrip('/')}/{os.path.relpath(src, local)}"
            with open(src, "rb") as fh:
                w.files.upload(dst, fh, overwrite=True)
