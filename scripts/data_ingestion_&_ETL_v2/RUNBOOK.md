# Pipeline 运行手册

> 本手册说明如何在 AWS Academy 账号上运行 Stack Exchange ETL Pipeline，
> 包含完整的 AWS 环境准备、Smoke Test（小样本）和全量运行步骤。

---

## 目录

1. [AWS 环境准备（12 步）](#1-aws-环境准备)
2. [Smoke Test：小样本验证](#2-smoke-test小样本验证)
3. [全量运行](#3-全量运行)
4. [证据收集与验证](#4-证据收集与验证)
5. [常见问题](#5-常见问题)

---

## 1. AWS 环境准备

> **前提**：同学的 AWS Academy Lab 必须保持开着，直到第九步 EMR 集群启动完毕。
> EMR 启动后，集群通过 IAM Role 运行，不再依赖你的临时凭证。

### 第一步：获取 AWS 凭证

请同学：
1. 登录 AWS Academy → 进入课程 → 点击 **Launch AWS Learner Lab**
2. 点击 **Start Lab**（等绿灯亮起）
3. 点击 **AWS Details** → 复制以下三行发给你：

```
aws_access_key_id = ASIA...
aws_secret_access_key = ...
aws_session_token = IQoJ...（很长）
```

> ⚠️ 凭证每次开 Lab 都会变，几小时后过期。

---

### 第二步：修复 PEM 文件权限

```bash
chmod 400 "/Users/yuqiuyi/Desktop/2026 Spring/30123/vockey.pem"
```

---

### 第三步：配置 AWS 凭证

```bash
cat > ~/.aws/credentials << 'EOF'
[default]
aws_access_key_id     = 同学给你的 access key
aws_secret_access_key = 同学给你的 secret key
aws_session_token     = 同学给你的 session token（很长，注意不要截断）
EOF

cat > ~/.aws/config << 'EOF'
[default]
region = us-east-1
EOF
```

---

### 第四步：验证凭证有效

```bash
aws sts get-caller-identity
```

成功输出：

```json
{
    "UserId": "AROA...",
    "Account": "054507xxxxxx",
    "Arn": "arn:aws:sts::054507xxxxxx:assumed-role/..."
}
```

如果报错，回第三步检查 session token 是否完整粘贴。

---

### 第五步：确定 S3 Bucket 名

S3 bucket 名全局唯一，旧账号的 `se-data-dump-2025-2` 无法在新账号复用。

```bash
# 尝试创建，若显示 make_bucket: ... 则可用
aws s3 mb s3://se-data-dump-2025-3 2>&1
```

- 显示 `make_bucket: se-data-dump-2025-3` → 使用这个名字
- 报 `BucketAlreadyExists` → 换一个（如 `se-data-dump-2026`）

```bash
# 设置环境变量，后续所有命令直接引用 $BUCKET
export BUCKET="se-data-dump-2025-3"   # ← 改成你刚确认的名字
```

---

### 第六步：更新 notebook 里的 bucket 名

打开 `scripts/data_ingestion_&_ETL/setup_phase3.ipynb`，
找第一个 Cell，将 `BUCKET` 改为你刚确认的名字：

```python
BUCKET = "se-data-dump-2025-3"   # ← 改成你的
```

---

### 第七步：运行 setup_phase3.ipynb 创建 AWS 资源

必须从 `scripts/data_ingestion_&_ETL/` 目录运行（需要读取同目录的 `lambda_function.py`）：

```bash
cd "/Users/yuqiuyi/Desktop/30123 PJ/final-project-stack_exchange_after_gpt/scripts/data_ingestion_&_ETL"
jupyter notebook setup_phase3.ipynb
```

逐 Cell 运行，会自动创建：
- S3 bucket
- DynamoDB 表 `posts_batch_manifest`
- SQS 队列 `posts-batch-queue`
- Lambda 函数 `batch-manifest-writer`（S3 上传触发器）
- SNS topic `batch-etl-complete`

运行完后，**记下最后 Cell 输出的两个变量**（后续要用）：

```bash
export SQS_URL='https://sqs.us-east-1.amazonaws.com/同学账号ID/posts-batch-queue'
export SNS_TOPIC_ARN='arn:aws:sns:us-east-1:同学账号ID:batch-etl-complete'
```

---

### 第八步：确认 SNS 邮件订阅

去邮箱 `graceeehgq@gmail.com`，找一封来自 **AWS Notifications** 的邮件，
点击 **Confirm subscription**。不确认则 Phase 2 完成后收不到邮件通知。

---

### 第九步：启动 EMR 集群（本地执行）

```bash
cd "/Users/yuqiuyi/Desktop/30123 PJ/final-project-stack_exchange_after_gpt/scripts/data_ingestion_&_ETL"

python launch_spark_cluster.py \
    --pem_path "/Users/yuqiuyi/Desktop/2026 Spring/30123/vockey.pem"
```

默认配置：`emr-7.13.0`，`r8g.xlarge` × 5 节点（1 primary + 4 core）= 20 vCPU，在 AWS Academy 配额范围内。

等待约 5–8 分钟，脚本自动打印 SSH 命令：

```
Cluster is ready.
SSH into the primary node:
  ssh -i "…/vockey.pem" hadoop@ec2-xx-xx-xx-xx.compute-1.amazonaws.com
```

**把这行 SSH 命令存下来，设为变量：**

```bash
# ⚠️ 每次重新启动集群 DNS 会变，以实际输出为准
export CLUSTER_DNS="ec2-54-234-49-13.compute-1.amazonaws.com"
```

---

### 第十步：SSH 进入 master 节点，安装依赖

```bash
ssh -i "/Users/yuqiuyi/Desktop/2026 Spring/30123/vockey.pem" hadoop@$CLUSTER_DNS

# 登录后在 EMR 上执行：
pip install beautifulsoup4 lxml radon
```

---

### 第十一步：上传代码到 EMR（通过 S3 中转）

> scp 在 AWS Academy 环境中会因 `.bashrc` 输出干扰协议而卡住，改用 S3 中转。

**本地执行（上传到 S3）：**

```bash
export BUCKET="se-data-dump-2025-3"
cd "/Users/yuqiuyi/Desktop/30123 PJ/final-project-stack_exchange_after_gpt/scripts/data_ingestion_&_ETL"

aws s3 cp stream_posts_to_s3_batches.sh s3://$BUCKET/scripts/
aws s3 cp build_posts_cqi_batches.py     s3://$BUCKET/scripts/
aws s3 cp launch_parallel_etl.py         s3://$BUCKET/scripts/
```

**SSH 进 EMR 后，从 S3 下载：**

```bash
mkdir -p ~/etl
aws s3 cp s3://se-data-dump-2025-3/scripts/ ~/etl/ --recursive
ls ~/etl/   # 确认三个文件都在
```

---

### 第十二步：在 EMR 上设置环境变量

登录 EMR 后，先设好后续所有命令都会用到的变量：

```bash
export BUCKET="se-data-dump-2025-3"
export SQS_URL='https://sqs.us-east-1.amazonaws.com/650720751298/posts-batch-queue'
export SNS_TOPIC_ARN='arn:aws:sns:us-east-1:650720751298:batch-etl-complete'

cd ~/etl
```

---

## 2. Smoke Test：小样本验证

> **Phase 1 没有 smoke test**：无论处理多少个 batch，都必须先完整下载 22 GB 压缩包，
> 省不了时间，直接全量跑（见第 3 节）。
>
> **Smoke test 只针对 Phase 2**：Phase 1 全量完成后，用 `--max-batches 2` 验证
> ETL 逻辑是否正常，再决定是否全量跑 Phase 2。
>
> **所有命令都在 EMR master 上执行**（在 tmux 里）。

---

### Smoke Test — Phase 2（串行）：处理 2 个 batch，生成 Parquet + 证据 JSON

```bash
# 在 EMR master 上，新建 tmux session
tmux new -s smoke_phase2

export BUCKET="se-data-dump-2025-3"
export SNS_TOPIC_ARN="arn:aws:sns:..."   # ← 第七步记下的值

spark-submit ~/etl/build_posts_cqi_batches.py \
  --mode          s3 \
  --max-batches   2 \
  --posts-batches "s3://$BUCKET/raw/stackoverflow/posts_batches/" \
  --users-xml     "s3://$BUCKET/raw/stackoverflow/Users.xml" \
  --postlinks-xml "s3://$BUCKET/raw/stackoverflow/PostLinks.xml" \
  --output        "s3://$BUCKET/smoke_test/posts_cqi_with_tier/" \
  --checkpoint    "s3://$BUCKET/smoke_test/checkpoints/" \
  --sns-topic-arn $SNS_TOPIC_ARN \
  --evidence-file ~/smoke_phase2_summary.json \
  2>&1 | tee smoke_phase2.log
```

**预期输出**（约 5–15 分钟）：

```
[INFO] 2 batches to process
[PROCESS] posts_batch_00000.xml.gz
  [DONE] posts_batch_00000.xml.gz  ...,... rows  ...s
[PROCESS] posts_batch_00001.xml.gz
  [DONE] posts_batch_00001.xml.gz  ...,... rows  ...s
 Phase 2 Complete
 Processed : 2 batches
 Output    : s3://se-data-dump-2025-3/smoke_test/posts_cqi_with_tier/
[OK] Evidence saved → /home/hadoop/smoke_phase2_summary.json
```

**查看证据 JSON**：

```bash
cat ~/smoke_phase2_summary.json
```

输出示例：

```json
{
  "run_at": "2026-05-29T10:23:45Z",
  "mode": "s3",
  "worker_index": 0,
  "num_workers": 1,
  "batches_done": 2,
  "batches_skipped": 0,
  "total_rows": 840392,
  "duration_min": 8.3,
  "output": "s3://se-data-dump-2025-3/smoke_test/posts_cqi_with_tier/",
  "checkpoint": "s3://se-data-dump-2025-3/smoke_test/checkpoints/",
  "window": "2019-01 → 2025-03"
}
```

**验证 S3 输出**：

```bash
# 确认 checkpoint 文件存在（应有 2 个 .done 文件）
aws s3 ls s3://$BUCKET/smoke_test/checkpoints/

# 确认 Parquet 分区目录已创建
aws s3 ls s3://$BUCKET/smoke_test/posts_cqi_with_tier/
```

---

### Smoke Test — Phase 2（并行）：4 个 worker 各处理 1 个 batch

> 这一步测试并行 launcher `launch_parallel_etl.py` 是否正常工作。
> 需要 Phase 1 smoke test 已上传至少 4 个 batch；如只有 2 个，把 `--max-batches` 改为 2。

```bash
tmux new -s smoke_parallel

export BUCKET="se-data-dump-2025-3"

python ~/etl/launch_parallel_etl.py \
  --num-workers   4 \
  --max-batches   4 \
  --posts-batches "s3://$BUCKET/raw/stackoverflow/posts_batches/" \
  --users-xml     "s3://$BUCKET/raw/stackoverflow/Users.xml" \
  --postlinks-xml "s3://$BUCKET/raw/stackoverflow/PostLinks.xml" \
  --output        "s3://$BUCKET/smoke_parallel/posts_cqi_with_tier/" \
  --checkpoint    "s3://$BUCKET/smoke_parallel/checkpoints/" \
  --evidence-dir  ~/smoke_parallel_evidence/ \
  2>&1 | tee smoke_parallel.log
```

**预期输出**：

```
[INFO] Found 4 batches — splitting across 4 workers (~1 batches each)
[INFO] Launching 4 workers …
[W0] Starting: spark-submit ...
[W1] Starting: spark-submit ...
[W2] Starting: spark-submit ...
[W3] Starting: spark-submit ...
[W0] OK
[W1] OK
[W2] OK
[W3] OK
 Succeeded: workers [0, 1, 2, 3]
[OK] Merged evidence → /home/hadoop/smoke_parallel_evidence/run_summary_merged.json
```

**查看合并证据**：

```bash
cat ~/smoke_parallel_evidence/run_summary_merged.json
```

---

### 将所有 Smoke Test 证据下载到本地（通过 S3 中转）

**在 EMR 上，先把证据上传到 S3：**

```bash
export BUCKET="se-data-dump-2025-3"
aws s3 cp ~/smoke_phase2_summary.json          s3://$BUCKET/evidence/
aws s3 cp ~/smoke_phase2.log                   s3://$BUCKET/evidence/
aws s3 cp ~/smoke_parallel_evidence/run_summary_merged.json s3://$BUCKET/evidence/smoke_parallel_merged.json
```

**在本地下载：**

```bash
export BUCKET="se-data-dump-2025-3"
LOCAL_OUT="/Users/yuqiuyi/Desktop/30123 PJ/final-project-stack_exchange_after_gpt/outputs/data_ingestion_&_ETL"

aws s3 cp s3://$BUCKET/evidence/ "$LOCAL_OUT/" --recursive
```

---

## 3. 全量运行

> 全量运行处理所有约 61 个 batch（约 6100 万行原始数据），耗时约 2–4 小时。
> **建议优先使用并行方式**（Phase 2 并行）节省时间。

---

### 全量 Phase 1：下载并上传全部 batch

> 脚本已改用 **aria2c 16 线程**下载（原来是 wget 单线程）。
> 先确认磁盘空间：archive 文件约 **64 GB**，根卷 100 GB，需要剩余 ≥ 70 GB。

```bash
# 检查磁盘（需要 ≥ 70 GB 可用）
df -h /
```

磁盘充足后，在 EMR master 上运行：

```bash
tmux new -s phase1

export BUCKET="se-data-dump-2025-3"
cd ~/etl

bash stream_posts_to_s3_batches.sh 2>&1 | tee phase1_full.log
```

脚本会自动安装 aria2c，下载速度约 **20–50 MB/s**，64 GB 预计 **30–60 分钟**；
之后流式解压 + 上传全部 batch 约再需 **1–2 小时**。

**实时查看进度**（另开 tmux 窗口）：

```bash
tmux attach -t phase1

# 或查看当前已上传的 batch 数量
aws s3 ls s3://$BUCKET/raw/stackoverflow/posts_batches/ | wc -l
```

Phase 1 完成标志：

```
 Phase 1 Complete
 Batches : s3://se-data-dump-2025-3/raw/stackoverflow/posts_batches/
 Manifest: s3://se-data-dump-2025-3/manifests/posts_batch_manifest.txt
```

**收集 Phase 1 全量证据**：

```bash
# 本地 manifest（Phase 1 自动生成）
wc -l posts_batch_manifest.txt   # 应为 61

# S3 上的 manifest
aws s3 cp s3://$BUCKET/manifests/posts_batch_manifest.txt - | wc -l

# 保存 S3 batch 列表到本地
aws s3 ls s3://$BUCKET/raw/stackoverflow/posts_batches/ > phase1_full_s3_listing.txt
wc -l phase1_full_s3_listing.txt   # 应为 61
```

---

### 全量 Phase 2（推荐：并行，4 个 worker）

```bash
tmux new -s phase2

export BUCKET="se-data-dump-2025-3"
export SNS_TOPIC_ARN='arn:aws:sns:us-east-1:650720751298:batch-etl-complete'

python ~/etl/launch_parallel_etl.py \
  --num-workers   4 \
  --posts-batches "s3://$BUCKET/raw/stackoverflow/posts_batches/" \
  --users-xml     "s3://$BUCKET/raw/stackoverflow/Users.xml" \
  --postlinks-xml "s3://$BUCKET/raw/stackoverflow/PostLinks.xml" \
  --output        "s3://$BUCKET/posts_cqi_with_tier/" \
  --checkpoint    "s3://$BUCKET/checkpoints/posts_cqi_batches/" \
  --sns-topic-arn $SNS_TOPIC_ARN \
  --evidence-dir  ~/full_run_evidence/ \
  2>&1 | tee phase2_full_parallel.log
```

Phase 2 并行耗时约 **45–90 分钟**（4 个 worker 各处理约 15 个 batch）。

完成后你会收到 SNS 邮件通知，同时 `~/full_run_evidence/run_summary_merged.json` 自动生成。

---

### 全量 Phase 2（备选：串行，单 worker）

> 如果并行遇到问题，用串行兜底，耗时约 3–4 小时。

```bash
tmux new -s phase2_serial

export BUCKET="se-data-dump-2025-3"
export SNS_TOPIC_ARN='arn:aws:sns:us-east-1:650720751298:batch-etl-complete'

spark-submit ~/etl/build_posts_cqi_batches.py \
  --mode          s3 \
  --posts-batches "s3://$BUCKET/raw/stackoverflow/posts_batches/" \
  --users-xml     "s3://$BUCKET/raw/stackoverflow/Users.xml" \
  --postlinks-xml "s3://$BUCKET/raw/stackoverflow/PostLinks.xml" \
  --output        "s3://$BUCKET/posts_cqi_with_tier/" \
  --checkpoint    "s3://$BUCKET/checkpoints/posts_cqi_batches/" \
  --sns-topic-arn $SNS_TOPIC_ARN \
  --evidence-file ~/full_run_evidence/run_summary_serial.json \
  2>&1 | tee phase2_full_serial.log
```

> **断点续跑**：中途失败后直接重新执行同一命令。
> `--checkpoint` 机制会自动跳过已完成的 batch（打印 `[SKIP]`），从断点继续。

---

## 4. 证据收集与验证

### 在 EMR 上收集全量证据

```bash
mkdir -p ~/full_run_evidence

# Phase 1：batch 文件清单
aws s3 ls s3://$BUCKET/raw/stackoverflow/posts_batches/ \
  > ~/full_run_evidence/phase1_s3_batches.txt
echo "Phase 1 batch 数：$(wc -l < ~/full_run_evidence/phase1_s3_batches.txt)"

# Phase 2：checkpoint 清单（每个 .done 文件 = 一个成功处理的 batch）
aws s3 ls s3://$BUCKET/checkpoints/posts_cqi_batches/ \
  > ~/full_run_evidence/phase2_checkpoints.txt
echo "Phase 2 已完成 batch 数：$(wc -l < ~/full_run_evidence/phase2_checkpoints.txt)"

# Phase 2：Parquet 分区列表
aws s3 ls s3://$BUCKET/posts_cqi_with_tier/ \
  > ~/full_run_evidence/phase2_parquet_partitions.txt
echo "Parquet year_month 分区数：$(wc -l < ~/full_run_evidence/phase2_parquet_partitions.txt)"

# Phase 2：完整 Parquet 文件树（较大，约 18 万行）
aws s3 ls --recursive s3://$BUCKET/posts_cqi_with_tier/ \
  > ~/full_run_evidence/phase2_parquet_full_listing.txt
echo "Parquet 文件总数：$(wc -l < ~/full_run_evidence/phase2_parquet_full_listing.txt)"

# 查看合并运行摘要（--evidence-dir 自动生成）
cat ~/full_run_evidence/run_summary_merged.json
```

---

### 下载证据到本地（通过 S3 中转）

**在 EMR 上，先把证据上传到 S3：**

```bash
export BUCKET="se-data-dump-2025-3"
aws s3 cp ~/full_run_evidence/  s3://$BUCKET/evidence/full_run/ --recursive
aws s3 cp ~/phase1_full.log     s3://$BUCKET/evidence/
aws s3 cp ~/phase2_full_parallel.log s3://$BUCKET/evidence/
```

**在本地下载：**

```bash
export BUCKET="se-data-dump-2025-3"
LOCAL_OUT="/Users/yuqiuyi/Desktop/30123 PJ/final-project-stack_exchange_after_gpt/outputs/data_ingestion_&_ETL"

aws s3 cp s3://$BUCKET/evidence/ "$LOCAL_OUT/" --recursive
```

---

### 验证全量运行完整性

```bash
# 1. Phase 1：应有 61 个 batch
aws s3 ls s3://$BUCKET/raw/stackoverflow/posts_batches/ \
  | grep "posts_batch_" | wc -l
# 期望：61

# 2. Phase 2：应有 61 个 checkpoint
aws s3 ls s3://$BUCKET/checkpoints/posts_cqi_batches/ \
  | grep ".done" | wc -l
# 期望：61

# 3. Parquet 分区：应覆盖 2019-01 到 2025-03
aws s3 ls s3://$BUCKET/posts_cqi_with_tier/ | wc -l
# 期望：约 75 个 year_month 分区

# 4. 从 evidence JSON 确认总行数
cat ~/full_run_evidence/run_summary_merged.json | python3 -c \
  "import json,sys; d=json.load(sys.stdin); \
   print(f'总行数: {d[\"total_rows\"]:,}  完成 batch: {d[\"total_batches_done\"]}')"
# 期望：总行数约 2,600,000  完成 batch: 61
```

---

## 5. 常见问题

### 凭证过期怎么办？

AWS Academy 凭证几小时后过期，但**只影响本地操作**。
EMR 集群通过 IAM Role（`EMR_EC2_DefaultRole`）运行，Phase 1/2 在 EMR 上跑中途不会失败。

需要在本地做 AWS 操作时（如 `aws s3 ls`），让同学重新给凭证，重复第三步即可。

---

### Phase 2 中途失败怎么办？

直接重新执行同一条 `spark-submit` / `launch_parallel_etl.py` 命令。
checkpoint 机制会自动跳过已完成的 batch（打印 `[SKIP] xxx (checkpoint exists)`），从失败的地方继续。

---

### tmux 常用命令

| 操作 | 命令 |
|------|------|
| 新建 session | `tmux new -s 名字` |
| 挂起（保持运行，断开 SSH） | `Ctrl+B` 然后 `D` |
| 重新连接 | `tmux attach -t 名字` |
| 列出所有 session | `tmux ls` |
| 在同一 session 新开窗口 | `Ctrl+B` 然后 `C` |
| 切换窗口 | `Ctrl+B` 然后 `N`（下一个） |

---

### 文件路径速查

**EMR master 上的路径**

| 内容 | 路径 |
|------|------|
| Phase 1 脚本 | `~/etl/stream_posts_to_s3_batches.sh` |
| Phase 2 脚本 | `~/etl/build_posts_cqi_batches.py` |
| 并行 launcher | `~/etl/launch_parallel_etl.py` |
| Phase 1 日志（全量） | `~/phase1_full.log` |
| Phase 2 日志（全量） | `~/phase2_full_parallel.log` |
| 证据目录 | `~/full_run_evidence/` |
| 合并运行摘要 | `~/full_run_evidence/run_summary_merged.json` |

**S3 路径**

| 内容 | S3 路径 |
|------|---------|
| 原始 batch 文件 | `s3://$BUCKET/raw/stackoverflow/posts_batches/` |
| 参考 XML 文件 | `s3://$BUCKET/raw/stackoverflow/{Users,Tags,PostLinks}.xml` |
| Parquet 输出 | `s3://$BUCKET/posts_cqi_with_tier/year_month=YYYY-MM/` |
| Checkpoint 文件 | `s3://$BUCKET/checkpoints/posts_cqi_batches/*.done` |
| Batch manifest | `s3://$BUCKET/manifests/posts_batch_manifest.txt` |
