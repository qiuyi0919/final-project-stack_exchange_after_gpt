#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


SCRIPTS_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SCRIPTS_ROOT.parent
DEFAULT_DB_PATH = SCRIPTS_ROOT / "data" / "serving.db"
DEFAULT_MONTHLY_CSV = PROJECT_ROOT / "outputs" / "emr" / "monthly_A_flat.csv"
DEFAULT_RDD_CSV = PROJECT_ROOT / "outputs" / "emr" / "rdd_results_A.csv"


def to_float(value: str) -> float | None:
    return float(value) if value not in {"", "nan", "NaN"} else None


def to_int(value: str) -> int | None:
    return int(float(value)) if value not in {"", "nan", "NaN"} else None


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP TABLE IF EXISTS monthly_cqi;
        DROP TABLE IF EXISTS rdd_results;
        DROP TABLE IF EXISTS batch_manifest;

        CREATE TABLE monthly_cqi (
          year_month TEXT NOT NULL,
          experience_tier TEXT NOT NULL,
          tag_category TEXT NOT NULL,
          mean_CQI REAL,
          n_posts INTEGER,
          mean_score REAL,
          mean_answer_count REAL,
          month TEXT,
          PRIMARY KEY (experience_tier, tag_category, year_month)
        );

        CREATE INDEX idx_monthly_cqi_filter
          ON monthly_cqi (experience_tier, tag_category, year_month);

        CREATE TABLE rdd_results (
          experience_tier TEXT NOT NULL,
          tag_category TEXT NOT NULL,
          beta_post REAL,
          pvalue REAL,
          ci_low REAL,
          ci_high REAL,
          n_months INTEGER,
          pre_months INTEGER,
          post_months INTEGER,
          n_posts INTEGER,
          PRIMARY KEY (experience_tier, tag_category)
        );

        CREATE TABLE batch_manifest (
          batch_id TEXT PRIMARY KEY,
          s3_path TEXT,
          size_bytes INTEGER,
          uploaded_at TEXT,
          processed_at TEXT,
          status TEXT
        );
        """
    )


def load_monthly(conn: sqlite3.Connection, csv_path: Path) -> int:
    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        rows = [
            (
                row["year_month"],
                row["experience_tier"],
                row["tag_category"],
                to_float(row["mean_CQI"]),
                to_int(row["n_posts"]),
                to_float(row["mean_score"]),
                to_float(row["mean_answer_count"]),
                row.get("month") or f"{row['year_month']}-01",
            )
            for row in reader
        ]

    conn.executemany(
        """
        INSERT INTO monthly_cqi (
          year_month, experience_tier, tag_category, mean_CQI, n_posts,
          mean_score, mean_answer_count, month
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def load_rdd(conn: sqlite3.Connection, csv_path: Path) -> int:
    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        rows = [
            (
                row["experience_tier"],
                row["tag_category"],
                to_float(row["beta_post"]),
                to_float(row["pvalue"]),
                to_float(row["ci_low"]),
                to_float(row["ci_high"]),
                to_int(row["n_months"]),
                to_int(row.get("pre_months", "")),
                to_int(row.get("post_months", "")),
                to_int(row["n_posts"]),
            )
            for row in reader
        ]

    conn.executemany(
        """
        INSERT INTO rdd_results (
          experience_tier, tag_category, beta_post, pvalue, ci_low, ci_high,
          n_months, pre_months, post_months, n_posts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def seed_manifest(conn: sqlite3.Connection, monthly_csv: Path, rdd_csv: Path) -> int:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rows = [
        (
            "monthly_A_flat",
            "s3://se-data-dump-2025/curated/monthly_A_flat.csv",
            monthly_csv.stat().st_size,
            now,
            now,
            "processed",
        ),
        (
            "rdd_results_A",
            "s3://se-data-dump-2025/curated/rdd_results_A.csv",
            rdd_csv.stat().st_size,
            now,
            now,
            "processed",
        ),
    ]
    conn.executemany(
        """
        INSERT INTO batch_manifest (
          batch_id, s3_path, size_bytes, uploaded_at, processed_at, status
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load EMR CSV outputs into the serving SQLite database.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--monthly-csv", type=Path, default=DEFAULT_MONTHLY_CSV)
    parser.add_argument("--rdd-csv", type=Path, default=DEFAULT_RDD_CSV)
    args = parser.parse_args()

    args.db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(args.db) as conn:
        create_schema(conn)
        monthly_rows = load_monthly(conn, args.monthly_csv)
        rdd_rows = load_rdd(conn, args.rdd_csv)
        manifest_rows = seed_manifest(conn, args.monthly_csv, args.rdd_csv)

    print(f"Loaded {monthly_rows} monthly rows, {rdd_rows} RDD rows, {manifest_rows} manifest rows")
    print(f"Serving database: {args.db}")


if __name__ == "__main__":
    main()
