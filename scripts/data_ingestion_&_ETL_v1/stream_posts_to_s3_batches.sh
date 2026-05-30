#!/bin/bash
# stream_posts_to_s3_batches.sh
# Phase 1: Download 7z archive → stream Posts.xml → compressed S3 batch files + manifest
#
# Usage (on EMR master node, inside tmux):
#   export BUCKET="se-data-dump-2025-2"
#   export ROWS_PER_BATCH=1000000        # optional, default 1000000
#   bash stream_posts_to_s3_batches.sh 2>&1 | tee phase1.log
#
# Smoke test — 2 real batches (requires archive already downloaded):
#   export MAX_BATCHES=2 ROWS_PER_BATCH=1000000
#   bash stream_posts_to_s3_batches.sh 2>&1 | tee phase1_smoke.log

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
BUCKET="${BUCKET:?'ERROR: BUCKET env var is required. Run: export BUCKET=se-data-dump-2025-2'}"
ROWS_PER_BATCH="${ROWS_PER_BATCH:-1000000}"
MAX_BATCHES="${MAX_BATCHES:-0}"          # 0 = unlimited (full run); N = stop after N batches
ARCHIVE="${ARCHIVE:-/home/hadoop/stackoverflow.com.7z}"
ARCHIVE_URL="https://archive.org/download/stackexchange_20250331/stackexchange_20250331%2Fstackoverflow.com.7z"
MANIFEST_LOCAL="posts_batch_manifest.txt"
S3_RAW="raw/stackoverflow"
S3_BATCHES="${S3_RAW}/posts_batches"
S3_MANIFESTS="manifests"
PYHELPER="/tmp/batch_splitter_$$.py"

echo "========================================================"
echo " Phase 1: Stream Posts.xml → S3 Batches"
echo "========================================================"
echo " Bucket       : s3://${BUCKET}"
echo " Archive      : ${ARCHIVE}"
echo " Rows/batch   : ${ROWS_PER_BATCH}"
echo " Max batches  : ${MAX_BATCHES:-unlimited}"
echo " Output prefix: s3://${BUCKET}/${S3_BATCHES}/"
echo "========================================================"
echo ""

# ── [1/6] Install dependencies ────────────────────────────────────────────────
echo "[1/6] Installing dependencies..."
if command -v 7za &>/dev/null; then
    echo "  [SKIP] p7zip already installed ($(7za i 2>/dev/null | grep 'p7zip' | head -1 || echo 'ok'))"
else
    sudo yum install -y p7zip p7zip-plugins
    echo "  [OK]  p7zip installed"
fi

# ── [2/6] Download archive (resumable) ────────────────────────────────────────
echo ""
echo "[2/6] Downloading stackoverflow.com.7z (resumable with -c)..."
if [[ -f "$ARCHIVE" ]]; then
    SIZE_BYTES=$(stat -c%s "$ARCHIVE" 2>/dev/null || stat -f%z "$ARCHIVE")
    SIZE_GB=$(echo "scale=1; $SIZE_BYTES/1073741824" | bc)
    echo "  Archive exists: ${ARCHIVE} (${SIZE_GB} GB)"
    echo "  Running wget -c to verify / resume if incomplete..."
fi
wget -c --progress=dot:giga \
    "$ARCHIVE_URL" \
    -O "$ARCHIVE"
echo "  [OK]  ${ARCHIVE}"

# ── [3/6] Verify archive contents ─────────────────────────────────────────────
echo ""
echo "[3/6] Verifying archive contents..."
7za l "$ARCHIVE" | grep "\.xml" | awk '{print "  ", $NF, $3}'
echo "  [OK]  Archive verified"

# ── [4/6] Create S3 bucket ────────────────────────────────────────────────────
echo ""
echo "[4/6] Creating S3 bucket (idempotent)..."
aws s3 mb "s3://${BUCKET}" 2>/dev/null && echo "  Created s3://${BUCKET}" \
  || echo "  s3://${BUCKET} already exists — OK"

# ── [5/6] Upload small reference files ────────────────────────────────────────
echo ""
echo "[5/6] Uploading reference XML files..."
for xml_name in Tags.xml Users.xml PostLinks.xml; do
    s3_dest="s3://${BUCKET}/${S3_RAW}/${xml_name}"
    if aws s3 ls "$s3_dest" &>/dev/null; then
        echo "  [SKIP] ${xml_name} already in S3"
    else
        echo "  Extracting ${xml_name}..."
        7za e "$ARCHIVE" "$xml_name" -o/tmp/ -y -bd >/dev/null 2>&1
        aws s3 cp "/tmp/${xml_name}" "$s3_dest"
        rm -f "/tmp/${xml_name}"
        echo "  [OK]   ${xml_name} → ${s3_dest}"
    fi
done

# ── [6/6] Stream Posts.xml → S3 batches + manifest ────────────────────────────
echo ""
echo "[6/6] Streaming Posts.xml → S3 batches (this takes ~1-2 hours)..."
echo ""

# Write the Python batch-splitter to a temp file.
# We do this because stdin is already used by the pipe from 7za,
# so we cannot use a heredoc (<<) to feed Python at the same time.
cat > "$PYHELPER" << 'PYEOF'
import sys, os, gzip, subprocess

BUCKET         = os.environ["BUCKET"]
ROWS_PER_BATCH = int(os.environ.get("ROWS_PER_BATCH", "1000000"))
MAX_BATCHES    = int(os.environ.get("MAX_BATCHES", "0"))   # 0 = unlimited
PREFIX         = "raw/stackoverflow/posts_batches"

batch_num = 0
buffer    = []
manifest  = []

def flush_batch(n, buf):
    name   = f"posts_batch_{n:05d}.xml.gz"
    s3_key = f"s3://{BUCKET}/{PREFIX}/{name}"
    data   = gzip.compress(b"".join(buf), compresslevel=6)
    result = subprocess.run(
        ["aws", "s3", "cp", "-", s3_key],
        input=data,
        capture_output=True
    )
    if result.returncode != 0:
        sys.exit(f"\n[ERROR] S3 upload failed for {name}:\n{result.stderr.decode()}")
    size_kb = len(data) // 1024
    print(f"  [DONE] {name}  rows={len(buf):,}  compressed={size_kb:,} KB", flush=True)
    return name

for line in sys.stdin.buffer:
    buffer.append(line)
    if len(buffer) >= ROWS_PER_BATCH:
        manifest.append(flush_batch(batch_num, buffer))
        batch_num += 1
        buffer = []
        if MAX_BATCHES and batch_num >= MAX_BATCHES:
            print(f"  [STOP] MAX_BATCHES={MAX_BATCHES} reached, stopping early.", flush=True)
            break

if buffer and not (MAX_BATCHES and batch_num >= MAX_BATCHES):
    manifest.append(flush_batch(batch_num, buffer))

with open("posts_batch_manifest.txt", "w") as f:
    f.write("\n".join(manifest) + "\n")

print(f"\n  Total batches written: {len(manifest)}", flush=True)
PYEOF

# set +o pipefail: Python exiting early (MAX_BATCHES) sends SIGPIPE to upstream grep/7za,
# which exits non-zero. We only care about Python's own exit code.
set +o pipefail
7za e -so "$ARCHIVE" Posts.xml 2>/dev/null \
  | grep -a '<row ' \
  | python3 "$PYHELPER"
PYEXIT=$?
set -o pipefail

rm -f "$PYHELPER"

if [[ $PYEXIT -ne 0 ]]; then
    echo "[ERROR] Batch splitter failed (exit ${PYEXIT})"
    exit $PYEXIT
fi

aws s3 cp "$MANIFEST_LOCAL" "s3://${BUCKET}/${S3_MANIFESTS}/posts_batch_manifest.txt"
echo "  [OK]  Manifest → s3://${BUCKET}/${S3_MANIFESTS}/posts_batch_manifest.txt"

# ── Summary ────────────────────────────────────────────────────────────────────
echo ""
echo "========================================================"
echo " Phase 1 Complete"
echo "========================================================"
echo " Batches : s3://${BUCKET}/${S3_BATCHES}/"
echo " Manifest: s3://${BUCKET}/${S3_MANIFESTS}/posts_batch_manifest.txt"
echo ""
echo " Verify with:"
echo "   aws s3 ls s3://${BUCKET}/${S3_BATCHES}/ | wc -l"
echo "   aws s3 cp s3://${BUCKET}/${S3_MANIFESTS}/posts_batch_manifest.txt -"
echo "   aws s3 cp s3://${BUCKET}/${S3_BATCHES}/posts_batch_00000.xml.gz - | zcat | head -3"
echo "========================================================"
