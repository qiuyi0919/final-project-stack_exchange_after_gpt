"""
launch_parallel_etl.py
======================
Launch N spark-submit workers in parallel, each processing a disjoint round-robin
slice of the S3 batch list.  Analogous to A2's parallel EC2 crawlers — every worker
runs build_posts_cqi_batches.py with --worker-index i --num-workers N.

YARN (EMR's resource manager) divides cluster resources across the concurrent jobs.
The existing S3 checkpoint mechanism prevents any batch from being processed twice.

Usage (on EMR master node):
  python launch_parallel_etl.py \\
    --num-workers 4 \\
    --posts-batches "s3://se-data-dump-2025-2/raw/stackoverflow/posts_batches/" \\
    --users-xml     "s3://se-data-dump-2025-2/raw/stackoverflow/Users.xml" \\
    --postlinks-xml "s3://se-data-dump-2025-2/raw/stackoverflow/PostLinks.xml" \\
    --output        "s3://se-data-dump-2025-2/posts_cqi_with_tier/" \\
    --checkpoint    "s3://se-data-dump-2025-2/checkpoints/posts_cqi_batches/"

Optional:
  --script PATH          path to build_posts_cqi_batches.py (default: same dir as this file)
  --spark-submit-args    extra flags forwarded verbatim to spark-submit
                         e.g. "--executor-memory 8g --executor-cores 4"
  --dynamodb-table NAME  DynamoDB table name (default: posts_batch_manifest)
  --sns-topic-arn ARN    SNS topic for completion email
  --window-start YYYY-MM  (default 2019-01)
  --window-end   YYYY-MM  (default 2025-03)
  --max-batches N        Limit total batches across all workers (smoke test)
  --evidence-dir PATH    Directory to save per-worker JSON summaries + merged summary
"""

import argparse
import concurrent.futures
import json
import math
import os
import re
import subprocess
import sys
import threading
from urllib.parse import urlparse

import boto3


# ── helpers ──────────────────────────────────────────────────────────────────

def list_s3_batches(prefix):
    """List batch files under an S3 prefix; return sorted list of s3:// URIs."""
    p = urlparse(prefix)
    bucket, key_prefix = p.netloc, p.path.lstrip("/")
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    batches = []
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            fname = obj["Key"].split("/")[-1]
            if re.match(r"posts_batch_\d{5}\.xml\.gz$", fname):
                batches.append(f"s3://{bucket}/{obj['Key']}")
    return sorted(batches)


def stream_output(pipe, prefix, lock):
    """Read lines from pipe and print them prefixed with [Wi]."""
    for raw in pipe:
        line = raw.decode(errors="replace").rstrip()
        with lock:
            print(f"{prefix} {line}", flush=True)


# ── worker ───────────────────────────────────────────────────────────────────

def run_worker(worker_index, num_workers, base_cmd, print_lock, evidence_dir=None):
    """Run one spark-submit worker; return (worker_index, returncode)."""
    cmd = base_cmd + [
        f"--worker-index={worker_index}",
        f"--num-workers={num_workers}",
    ]
    if evidence_dir:
        os.makedirs(evidence_dir, exist_ok=True)
        cmd += [f"--evidence-file={evidence_dir}/run_summary_w{worker_index}.json"]
    prefix = f"[W{worker_index}]"
    with print_lock:
        print(f"{prefix} Starting: {' '.join(cmd)}", flush=True)

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    # Stream output in a thread so workers don't block each other
    t = threading.Thread(target=stream_output, args=(proc.stdout, prefix, print_lock),
                         daemon=True)
    t.start()
    proc.wait()
    t.join()

    with print_lock:
        status = "OK" if proc.returncode == 0 else f"FAILED (exit {proc.returncode})"
        print(f"{prefix} {status}", flush=True)

    return worker_index, proc.returncode


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    here = os.path.dirname(os.path.abspath(__file__))

    parser = argparse.ArgumentParser(
        description="Launch N parallel spark-submit ETL workers on this EMR cluster."
    )
    parser.add_argument("--num-workers",       type=int, default=4,
                        help="Number of parallel spark-submit workers (default 4)")
    parser.add_argument("--posts-batches",     required=True,
                        help="S3 prefix for posts_batch_*.xml.gz files")
    parser.add_argument("--users-xml",         required=True)
    parser.add_argument("--postlinks-xml",     default=None)
    parser.add_argument("--output",            required=True)
    parser.add_argument("--checkpoint",        required=True)
    parser.add_argument("--dynamodb-table",    default="posts_batch_manifest")
    parser.add_argument("--sns-topic-arn",     default=None)
    parser.add_argument("--window-start",      default="2019-01")
    parser.add_argument("--window-end",        default="2025-03")
    parser.add_argument("--script",            default=os.path.join(here, "build_posts_cqi_batches.py"),
                        help="Path to build_posts_cqi_batches.py")
    parser.add_argument("--spark-submit-args", default="",
                        help="Extra flags forwarded verbatim to spark-submit")
    parser.add_argument("--max-batches",        type=int, default=None,
                        help="Limit total batches across all workers (smoke test)")
    parser.add_argument("--evidence-dir",       default=None,
                        help="Directory to save per-worker JSON summaries + merged summary")
    args = parser.parse_args()

    # ── validate batch prefix before spending time on cluster work ───────────
    print(f"[INFO] Listing batches under {args.posts_batches} …", flush=True)
    batches = list_s3_batches(args.posts_batches)
    if not batches:
        sys.exit(f"[ERROR] No posts_batch_*.xml.gz files found under {args.posts_batches}")
    print(f"[INFO] Found {len(batches)} batches — "
          f"splitting across {args.num_workers} workers "
          f"(~{len(batches) // args.num_workers}–{(len(batches) + args.num_workers - 1) // args.num_workers} batches each)",
          flush=True)

    # ── build the spark-submit base command (no --worker-index yet) ──────────
    extra = args.spark_submit_args.split() if args.spark_submit_args.strip() else []
    base_cmd = (
        ["spark-submit"] + extra + [args.script,
        "--mode",         "s3",
        "--posts-batches", args.posts_batches,
        "--users-xml",     args.users_xml,
        "--output",        args.output,
        "--checkpoint",    args.checkpoint,
        "--dynamodb-table", args.dynamodb_table,
        "--window-start",  args.window_start,
        "--window-end",    args.window_end,
        ]
    )
    if args.postlinks_xml:
        base_cmd += ["--postlinks-xml", args.postlinks_xml]
    if args.sns_topic_arn:
        base_cmd += ["--sns-topic-arn", args.sns_topic_arn]
    if args.max_batches:
        per_worker = math.ceil(args.max_batches / args.num_workers)
        base_cmd += ["--max-batches", str(per_worker)]

    # ── launch all workers in parallel ───────────────────────────────────────
    print(f"\n[INFO] Launching {args.num_workers} workers …\n", flush=True)
    print_lock = threading.Lock()
    results = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.num_workers) as pool:
        futures = {
            pool.submit(run_worker, i, args.num_workers, base_cmd, print_lock,
                        args.evidence_dir): i
            for i in range(args.num_workers)
        }
        for future in concurrent.futures.as_completed(futures):
            idx, rc = future.result()
            results[idx] = rc

    # ── summary ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(" Parallel ETL complete")
    print("=" * 60)
    succeeded = [i for i, rc in sorted(results.items()) if rc == 0]
    failed    = [i for i, rc in sorted(results.items()) if rc != 0]
    print(f" Succeeded: workers {succeeded}")
    if failed:
        print(f" FAILED:    workers {failed}")
    print("=" * 60)

    # ── merge per-worker evidence JSONs ──────────────────────────────────────
    if args.evidence_dir and not failed:
        summaries = []
        for i in range(args.num_workers):
            path = os.path.join(args.evidence_dir, f"run_summary_w{i}.json")
            if os.path.exists(path):
                with open(path) as f:
                    summaries.append(json.load(f))
        if summaries:
            merged = {
                "run_at":                summaries[0].get("run_at"),
                "num_workers":           args.num_workers,
                "total_batches_done":    sum(s["batches_done"]    for s in summaries),
                "total_batches_skipped": sum(s["batches_skipped"] for s in summaries),
                "total_rows":            sum(s["total_rows"]       for s in summaries),
                "workers":               summaries,
            }
            out = os.path.join(args.evidence_dir, "run_summary_merged.json")
            with open(out, "w") as f:
                json.dump(merged, f, indent=2)
            print(f"[OK] Merged evidence → {out}", flush=True)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
