"""
build_posts_cqi_batches.py
==========================
Phase 2: Batch Spark ETL — S3 posts_batch_*.xml.gz → partitioned parquet

Usage:
  # Mode s3: run after Phase 1 finishes
  spark-submit scripts/build_posts_cqi_batches.py \
    --mode s3 --max-batches 1 \
    --posts-batches "s3://se-data-dump-2025-2/raw/stackoverflow/posts_batches/" \
    --users-xml     "s3://se-data-dump-2025-2/raw/stackoverflow/Users.xml" \
    --postlinks-xml "s3://se-data-dump-2025-2/raw/stackoverflow/PostLinks.xml" \
    --output        "s3://se-data-dump-2025-2/posts_cqi_with_tier/" \
    --checkpoint    "s3://se-data-dump-2025-2/checkpoints/posts_cqi_batches/"

  # Mode sqs: run concurrently with Phase 1 (full automated chain)
  spark-submit scripts/build_posts_cqi_batches.py \
    --mode sqs --sqs-url $SQS_URL --sns-topic-arn $SNS_TOPIC_ARN \
    --users-xml     "s3://se-data-dump-2025-2/raw/stackoverflow/Users.xml" \
    --postlinks-xml "s3://se-data-dump-2025-2/raw/stackoverflow/PostLinks.xml" \
    --output        "s3://se-data-dump-2025-2/posts_cqi_with_tier/" \
    --checkpoint    "s3://se-data-dump-2025-2/checkpoints/posts_cqi_batches/"

Prerequisites on EMR:
  pip install beautifulsoup4 lxml radon
"""

import argparse
import ast
import datetime
import json
import re
import sys
import warnings
from urllib.parse import urlparse

import boto3
import numpy as np
import pandas as pd
from botocore.exceptions import ClientError

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    FloatType, IntegerType, StringType, StructField, StructType
)
from pyspark.sql.functions import (
    col, coalesce, date_format, lit, log1p, pandas_udf, when
)

warnings.filterwarnings("ignore")


# ══════════════════════════════════════════════════════════════
# CQI feature functions — inlined from cqi_features_version_a.py
# (self-contained: no addPyFile needed on EMR)
# ══════════════════════════════════════════════════════════════

def _extract_code_blocks(html):
    from bs4 import BeautifulSoup
    if not isinstance(html, str) or len(html.strip()) < 5:
        return []
    try:
        soup = BeautifulSoup(html, "lxml")
        return [tag.get_text() for tag in soup.find_all("code")]
    except Exception:
        return []


def _compute_cyclomatic_complexity(code):
    from radon.complexity import cc_visit
    if not isinstance(code, str) or not code.strip():
        return None
    try:
        blocks = cc_visit(code)
        if not blocks:
            return 1.0
        return float(np.mean([b.complexity for b in blocks]))
    except Exception:
        return None


def _compute_halstead_volume(code):
    from radon.metrics import h_visit
    if not isinstance(code, str) or not code.strip():
        return None
    try:
        result = h_visit(code)
        volume = result.total.volume
        return float(volume) if volume is not None and not np.isnan(volume) else None
    except Exception:
        return None


def _compute_code_parseability(code):
    if not isinstance(code, str) or not code.strip():
        return 0.0
    try:
        ast.parse(code)
        return 1.0
    except Exception:
        return 0.0


def _compute_python_code_features(html):
    blocks  = _extract_code_blocks(html)
    primary = max(blocks, key=len) if blocks else None
    return {
        "code_block_count":      len(blocks),
        "primary_code_len":      len(primary) if primary else 0,
        "cyclomatic_complexity": _compute_cyclomatic_complexity(primary) if primary else None,
        "halstead_volume":       _compute_halstead_volume(primary)        if primary else None,
        "is_parseable":          _compute_code_parseability(primary)      if primary else 0.0,
        "has_code":              int(len(blocks) > 0),
    }


# ── pandas_udf ────────────────────────────────────────────────
CODE_FEATURE_SCHEMA = StructType([
    StructField("code_block_count",      IntegerType(), True),
    StructField("primary_code_len",      IntegerType(), True),
    StructField("cyclomatic_complexity", FloatType(),   True),
    StructField("halstead_volume",       FloatType(),   True),
    StructField("is_parseable",          FloatType(),   True),
    StructField("has_code",              IntegerType(), True),
])


@pandas_udf(CODE_FEATURE_SCHEMA)
def _extract_python_features_udf(body_series: pd.Series) -> pd.DataFrame:
    results = body_series.apply(_compute_python_code_features)
    return pd.DataFrame(results.tolist())


# ══════════════════════════════════════════════════════════════
# XML parsing helper — from 01_data_ingestion.ipynb
# ══════════════════════════════════════════════════════════════

def _parse_xml_row(line, target_cols):
    import xml.etree.ElementTree as ET
    line = line.strip()
    if not line.startswith("<row"):
        return None
    try:
        if not line.endswith("/>"):
            line = line + "/>"
        elem = ET.fromstring(line)
        return tuple(elem.attrib.get(c) for c in target_cols)
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════
# S3 / checkpoint helpers
# ══════════════════════════════════════════════════════════════

def _s3_parts(s3_uri):
    """Split 's3://bucket/key/prefix/' into (bucket, key_prefix)."""
    p = urlparse(s3_uri)
    return p.netloc, p.path.lstrip("/")


def list_s3_batches(prefix, s3_client):
    """Return list of {batch_id, s3_path} dicts sorted by batch_id."""
    bucket, key_prefix = _s3_parts(prefix)
    paginator = s3_client.get_paginator("list_objects_v2")
    batches = []
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            fname = key.split("/")[-1]
            if re.match(r"posts_batch_\d{5}\.xml\.gz$", fname):
                batches.append({
                    "batch_id": fname,
                    "s3_path":  f"s3://{bucket}/{key}"
                })
    return sorted(batches, key=lambda x: x["batch_id"])


def poll_sqs_batches(sqs_url, sqs_client, max_batches=None, empty_polls_limit=3):
    """
    Generator: yield one {batch_id, s3_path} per SQS message.
    Deletes the message after yielding so it won't be reprocessed.
    Stops after empty_polls_limit consecutive empty polls (Phase 1 done).
    """
    empty_polls = 0
    count = 0
    while empty_polls < empty_polls_limit:
        if max_batches is not None and count >= max_batches:
            break
        resp = sqs_client.receive_message(
            QueueUrl=sqs_url, MaxNumberOfMessages=1, WaitTimeSeconds=20
        )
        msgs = resp.get("Messages", [])
        if not msgs:
            empty_polls += 1
            print(f"  [SQS] No message ({empty_polls}/{empty_polls_limit} empty polls)",
                  flush=True)
            continue
        msg  = msgs[0]
        body = json.loads(msg["Body"])
        yield {
            "batch_id":       body["batch_id"],
            "s3_path":        body["s3_path"],
            "receipt_handle": msg["ReceiptHandle"],
        }
        sqs_client.delete_message(QueueUrl=sqs_url,
                                  ReceiptHandle=msg["ReceiptHandle"])
        empty_polls = 0
        count += 1


def checkpoint_exists(checkpoint_prefix, batch_id, s3_client):
    bucket, key_prefix = _s3_parts(checkpoint_prefix)
    try:
        s3_client.head_object(Bucket=bucket,
                              Key=f"{key_prefix}{batch_id}.done")
        return True
    except ClientError:
        return False


def write_checkpoint(checkpoint_prefix, batch_id, s3_client):
    bucket, key_prefix = _s3_parts(checkpoint_prefix)
    s3_client.put_object(Bucket=bucket,
                         Key=f"{key_prefix}{batch_id}.done",
                         Body=b"")


def update_dynamodb(batch_id, status, table_name, dynamodb):
    """Update DynamoDB batch status. Silent no-op if table missing (Phase 3 not set up)."""
    try:
        dynamodb.Table(table_name).update_item(
            Key={"batch_id": batch_id},
            UpdateExpression="SET #s = :s, processed_at = :t",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":s": status,
                ":t": datetime.datetime.utcnow().isoformat(),
            }
        )
    except Exception as e:
        print(f"  [WARN] DynamoDB update skipped ({batch_id}): {e}", flush=True)


# ══════════════════════════════════════════════════════════════
# Data loading — called once before the batch loop
# ══════════════════════════════════════════════════════════════

_USERS_COLS      = ["Id", "Reputation"]
_POSTLINKS_COLS  = ["PostId", "RelatedPostId", "LinkTypeId"]


def load_users_tier(spark, users_xml_path):
    """
    Parse Users.xml → DataFrame with (UserId, Reputation, experience_tier).
    Cached in Spark memory for reuse across all batches.
    Thresholds: novice <100, intermediate <1000, expert >=1000.
    """
    print(f"[INIT] Loading Users.xml from {users_xml_path} ...", flush=True)
    cols_bc = spark.sparkContext.broadcast(_USERS_COLS)
    raw = spark.sparkContext.textFile(users_xml_path)
    schema = StructType([StructField(c, StringType(), True) for c in _USERS_COLS])

    rows = raw.map(lambda line: _parse_xml_row(line, cols_bc.value)) \
              .filter(lambda r: r is not None)
    df = spark.createDataFrame(rows, schema=schema)

    users_tier = df.select(
        col("Id").alias("UserId"),
        col("Reputation").cast("int").alias("Reputation"),
        when(col("Reputation").cast("int") < 100,  "novice")
        .when(col("Reputation").cast("int") < 1000, "intermediate")
        .otherwise("expert").alias("experience_tier")
    ).cache()

    count = users_tier.count()
    print(f"  [OK] Users loaded: {count:,} rows", flush=True)
    return users_tier


def load_postlinks(spark, postlinks_xml_path):
    """
    Parse PostLinks.xml → DataFrame of PostIds marked as duplicates (LinkTypeId==3).
    Returns None if path not provided.
    """
    if not postlinks_xml_path:
        return None
    print(f"[INIT] Loading PostLinks.xml from {postlinks_xml_path} ...", flush=True)
    cols_bc = spark.sparkContext.broadcast(_POSTLINKS_COLS)
    schema  = StructType([StructField(c, StringType(), True) for c in _POSTLINKS_COLS])
    raw     = spark.sparkContext.textFile(postlinks_xml_path)

    rows = raw.map(lambda line: _parse_xml_row(line, cols_bc.value)) \
              .filter(lambda r: r is not None)
    df = spark.createDataFrame(rows, schema=schema)

    duplicates = (
        df.filter(col("LinkTypeId") == "3")
          .select(col("PostId").alias("Id"))
          .distinct()
          .withColumn("is_duplicate", lit(1))
          .cache()
    )
    count = duplicates.count()
    print(f"  [OK] Duplicate post IDs loaded: {count:,}", flush=True)
    return duplicates


# ══════════════════════════════════════════════════════════════
# Core ETL — one batch at a time
# ══════════════════════════════════════════════════════════════

_POSTS_COLS = [
    "Id", "PostTypeId", "CreationDate", "Score", "Body",
    "Tags", "OwnerUserId", "AnswerCount", "ClosedDate", "AcceptedAnswerId",
]

_FINAL_COLS = [
    "Id", "PostTypeId", "CreationDate", "Score", "Tags", "OwnerUserId",
    "AnswerCount", "ClosedDate", "AcceptedAnswerId", "year_month",
    "is_python", "is_duplicate", "Reputation", "experience_tier",
    "code_block_count", "primary_code_len", "cyclomatic_complexity",
    "halstead_volume", "is_parseable", "has_code", "cqi_version_a",
]


def process_batch(spark, batch_s3_path, users_tier_df, postlinks_df,
                  output_path, window_start, window_end):
    """
    Full ETL for one batch file:
      1. Parse xml.gz rows
      2. Filter questions (PostTypeId==1) and time window
      3. Add is_python flag
      4. Join Users for experience_tier
      5. Join PostLinks for is_duplicate
      6. Apply pandas_udf for CQI features (Python posts only)
      7. Compute CQI normalization formula
      8. Write partitioned parquet (mode=append)

    Returns row count written.
    """
    cols_bc = spark.sparkContext.broadcast(_POSTS_COLS)
    schema  = StructType([StructField(c, StringType(), True) for c in _POSTS_COLS])

    # ── 1. Parse XML rows ─────────────────────────────────────
    raw = spark.sparkContext.textFile(batch_s3_path)  # auto-decompresses .gz
    rows = raw.map(lambda line: _parse_xml_row(line, cols_bc.value)) \
              .filter(lambda r: r is not None)
    df = spark.createDataFrame(rows, schema=schema)

    # ── 2. Filter questions + add year_month ──────────────────
    df = (df
          .filter(col("PostTypeId") == "1")
          .withColumn("year_month",
                      date_format(col("CreationDate").cast("timestamp"), "yyyy-MM"))
          .filter(col("year_month").isNotNull())
         )
    if window_start:
        df = df.filter(col("year_month") >= window_start)
    if window_end:
        df = df.filter(col("year_month") <= window_end)

    n_raw = df.count()
    if n_raw == 0:
        print("  [SKIP] No posts in time window for this batch.", flush=True)
        return 0

    # ── 3. is_python flag ─────────────────────────────────────
    df = df.withColumn(
        "is_python",
        when(
            col("Tags").contains("<python>") |
            col("Tags").contains("<python-3") |
            col("Tags").contains("<python-2"),
            1
        ).otherwise(0)
    )

    # ── 4. Join Users → experience_tier ──────────────────────
    df = (df
          .join(users_tier_df,
                df["OwnerUserId"] == users_tier_df["UserId"],
                how="left")
          .drop("UserId")
         )

    # ── 5. Join PostLinks → is_duplicate ─────────────────────
    if postlinks_df is not None:
        df = (df
              .join(postlinks_df, on="Id", how="left")
              .fillna({"is_duplicate": 0})
             )
    else:
        df = df.withColumn("is_duplicate", lit(0))

    # ── 6. CQI feature extraction (Python posts only) ─────────
    python_df     = df.filter(col("is_python") == 1)
    non_python_df = df.filter(col("is_python") == 0)

    if python_df.count() > 0:
        features = python_df.withColumn(
            "_feat", _extract_python_features_udf(col("Body"))
        )
        for feat_col in CODE_FEATURE_SCHEMA.fieldNames():
            features = features.withColumn(feat_col, col(f"_feat.{feat_col}"))
        python_df = features.drop("_feat")
    else:
        for feat_col in CODE_FEATURE_SCHEMA.fieldNames():
            python_df = python_df.withColumn(feat_col, lit(None).cast(FloatType()))

    # Non-Python posts: null for all code features
    for feat_col in CODE_FEATURE_SCHEMA.fieldNames():
        non_python_df = non_python_df.withColumn(feat_col, lit(None).cast(FloatType()))

    df = python_df.unionByName(non_python_df)

    # ── 7. CQI normalization (Version A, 7 dimensions) ────────
    cqi_expr = (
        log1p(col("Score").cast("double"))                          * 0.25 +
        coalesce(col("cyclomatic_complexity").cast("double"), lit(0.0)) * 0.20 +
        log1p(coalesce(col("halstead_volume").cast("double"), lit(0.0))) * 0.15 +
        coalesce(col("is_parseable").cast("double"), lit(0.0))      * 0.10 +
        log1p(col("AnswerCount").cast("double"))                    * 0.10 +
        when(col("ClosedDate").isNull(), 1.0).otherwise(0.0)        * 0.10 +
        (lit(1.0) - col("is_duplicate").cast("double"))             * 0.10
    )
    df = df.withColumn(
        "cqi_version_a",
        when(col("is_python") == 1, cqi_expr).otherwise(lit(None).cast("double"))
    )

    # ── 8. Write parquet ─────────────────────────────────────
    out_cols = [c for c in _FINAL_COLS if c in df.columns]
    (df.select(out_cols)
       .write
       .mode("append")
       .partitionBy("year_month")
       .parquet(output_path))

    n_python = df.filter(col("is_python") == 1).count()
    print(f"  [OK] Written {n_raw:,} posts ({n_python:,} Python) to {output_path}",
          flush=True)
    return n_raw


# ══════════════════════════════════════════════════════════════
# main
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Phase 2: Batch Spark ETL — S3 batches → CQI parquet"
    )
    parser.add_argument("--mode",            default="s3", choices=["s3", "sqs"])
    parser.add_argument("--posts-batches",   help="S3 prefix for batch files (--mode s3)")
    parser.add_argument("--sqs-url",         help="SQS queue URL (--mode sqs)")
    parser.add_argument("--users-xml",       required=True)
    parser.add_argument("--postlinks-xml",   default=None,
                        help="S3 path to PostLinks.xml (optional, enables is_duplicate)")
    parser.add_argument("--output",          required=True)
    parser.add_argument("--checkpoint",      required=True)
    parser.add_argument("--dynamodb-table",  default="posts_batch_manifest")
    parser.add_argument("--sns-topic-arn",   default=None)
    parser.add_argument("--max-batches",     type=int, default=None)
    parser.add_argument("--window-start",    default="2019-01")
    parser.add_argument("--window-end",      default="2025-03")
    args = parser.parse_args()

    if args.mode == "s3" and not args.posts_batches:
        parser.error("--posts-batches is required for --mode s3")
    if args.mode == "sqs" and not args.sqs_url:
        parser.error("--sqs-url is required for --mode sqs")

    # ── Spark session ─────────────────────────────────────────
    spark = SparkSession.builder \
        .appName("PostsCQIBatches") \
        .getOrCreate()
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    spark.sparkContext.setLogLevel("WARN")

    # ── AWS clients ───────────────────────────────────────────
    s3       = boto3.client("s3")
    dynamodb = boto3.resource("dynamodb")
    sqs      = boto3.client("sqs") if args.mode == "sqs" else None

    print("=" * 60)
    print(" Phase 2: Batch ETL")
    print(f" Mode     : {args.mode}")
    print(f" Output   : {args.output}")
    print(f" Window   : {args.window_start} → {args.window_end}")
    print("=" * 60)

    # ── Load reference data once ──────────────────────────────
    users_tier_df = load_users_tier(spark, args.users_xml)
    postlinks_df  = load_postlinks(spark, args.postlinks_xml)

    # ── Get batch list ────────────────────────────────────────
    if args.mode == "s3":
        all_batches = list_s3_batches(args.posts_batches, s3)
        if args.max_batches:
            all_batches = all_batches[:args.max_batches]
        batch_iter = iter(all_batches)
        print(f"\n[INFO] {len(all_batches)} batches to process")
    else:
        batch_iter = poll_sqs_batches(
            args.sqs_url, sqs, max_batches=args.max_batches
        )
        print("\n[INFO] Polling SQS for batches ...")

    # ── Main batch loop ───────────────────────────────────────
    done_count = 0
    skip_count = 0
    start_time = datetime.datetime.utcnow()

    for batch in batch_iter:
        batch_id = batch["batch_id"]
        s3_path  = batch["s3_path"]

        if checkpoint_exists(args.checkpoint, batch_id, s3):
            print(f"[SKIP] {batch_id} (checkpoint exists)", flush=True)
            skip_count += 1
            continue

        print(f"\n[PROCESS] {batch_id}", flush=True)
        t0 = datetime.datetime.utcnow()

        try:
            n_rows = process_batch(
                spark, s3_path,
                users_tier_df, postlinks_df,
                args.output,
                args.window_start, args.window_end,
            )
            elapsed = (datetime.datetime.utcnow() - t0).total_seconds()
            print(f"  [DONE] {batch_id}  {n_rows:,} rows  {elapsed:.1f}s", flush=True)

            write_checkpoint(args.checkpoint, batch_id, s3)
            update_dynamodb(batch_id, "done", args.dynamodb_table, dynamodb)
            done_count += 1

        except Exception as e:
            print(f"  [ERROR] {batch_id}: {e}", flush=True)
            update_dynamodb(batch_id, "failed", args.dynamodb_table, dynamodb)
            raise

    # ── Summary ───────────────────────────────────────────────
    total_elapsed = (datetime.datetime.utcnow() - start_time).total_seconds()
    print("\n" + "=" * 60)
    print(" Phase 2 Complete")
    print(f" Processed : {done_count} batches")
    print(f" Skipped   : {skip_count} batches (already checkpointed)")
    print(f" Total time: {total_elapsed/60:.1f} min")
    print(f" Output    : {args.output}")
    print("=" * 60)

    # ── SNS completion email (--mode sqs only) ────────────────
    if args.mode == "sqs" and args.sns_topic_arn and done_count > 0:
        try:
            boto3.client("sns").publish(
                TopicArn=args.sns_topic_arn,
                Subject="[Stack Exchange ETL] All batches complete",
                Message=(
                    f"Phase 2 complete.\n"
                    f"Processed: {done_count} batches\n"
                    f"Skipped:   {skip_count} batches\n"
                    f"Output:    {args.output}\n"
                    f"Duration:  {total_elapsed/60:.1f} min"
                )
            )
            print("[OK] SNS completion email sent.", flush=True)
        except Exception as e:
            print(f"[WARN] SNS publish failed: {e}", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()
