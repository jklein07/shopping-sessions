#!/usr/bin/env python3
"""Exact, order-insensitive comparison of two output folders.

Each folder holds either `<table>.csv` (expected-CSV format) or a `<table>/`
directory of Parquet files. Usage: compare.py A B   (exit 0 when equal)
"""
from pathlib import Path
import sys

import polars as pl

SCHEMAS = {
    "user_daily": {"user_id": pl.String, "day": pl.Date, "sessions": pl.Int64, "events": pl.Int64,
                   "revenue_cents": pl.Int64, "top_category": pl.String},
    "campaign_daily": {"campaign_id": pl.String, "channel": pl.String, "day": pl.Date, "sessions": pl.Int64,
                       "purchasing_sessions": pl.Int64, "revenue_cents": pl.Int64,
                       "assisted_sessions": pl.Int64},
}


def load(folder: Path, table: str) -> pl.DataFrame:
    schema = SCHEMAS[table]
    csv = folder / f"{table}.csv"
    if csv.is_file():
        df = pl.read_csv(csv, schema_overrides={k: (pl.String if v == pl.Date else v) for k, v in schema.items()},
                         missing_utf8_is_empty_string=False)
        if df.height == 0:
            return pl.DataFrame(schema=schema)
        return df.with_columns(pl.col("day").str.to_date())
    frames = []
    for path in sorted((folder / table).glob("*.parquet")):
        if path.name.startswith((".", "_")) or path.is_symlink() or not path.is_file():
            continue
        frame = pl.read_parquet(path)
        if set(frame.columns) != set(schema):
            raise ValueError(f"{path}: columns {frame.columns}")
        for column, expected in schema.items():
            actual = frame.schema[column]
            if expected == pl.Int64 and actual.is_integer():
                continue
            if expected == pl.String and actual.base_type() in (pl.String, pl.Categorical, pl.Enum):
                continue
            if actual != expected:
                raise ValueError(f"{path}: {column} has type {actual}, expected {expected}")
        frames.append(frame.select(schema.keys()).cast(schema))
    if not frames:
        raise ValueError(f"no parquet in {folder / table}")
    return pl.concat(frames)


def main() -> int:
    left, right = map(Path, sys.argv[1:3])
    quiet = "-q" in sys.argv
    matched = True
    for table, schema in SCHEMAS.items():
        a, b = load(left, table), load(right, table)
        keys = [c for c in ("user_id", "campaign_id", "day") if c in schema]
        dup_a = a.height - a.n_unique(subset=keys)
        dup_b = b.height - b.n_unique(subset=keys)
        only_a = a.join(b, on=list(schema), how="anti", nulls_equal=True)
        only_b = b.join(a, on=list(schema), how="anti", nulls_equal=True)
        if not quiet or dup_a or dup_b or only_a.height or only_b.height:
            print(f"{table}: rows {a.height} vs {b.height}; only in A {only_a.height}; "
                  f"only in B {only_b.height}; duplicate keys {dup_a} vs {dup_b}")
        if dup_a or dup_b or only_a.height or only_b.height:
            matched = False
            with pl.Config(tbl_cols=-1, tbl_width_chars=200):
                print(only_a.sort(keys).head(8))
                print(only_b.sort(keys).head(8))
    print("MATCH" if matched else "DIFFER")
    return 0 if matched else 1


if __name__ == "__main__":
    sys.exit(main())
