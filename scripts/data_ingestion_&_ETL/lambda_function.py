import boto3
import json
import os
import re

BUCKET         = os.environ["BUCKET"]
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]
SQS_URL        = os.environ["SQS_URL"]

dynamodb = boto3.resource("dynamodb")
sqs      = boto3.client("sqs")

BATCH_KEY_RE = re.compile(
    r"raw/stackoverflow/posts_batches/posts_batch_\d{5}\.xml\.gz$"
)


def lambda_handler(event, context):
    for record in event["Records"]:
        key         = record["s3"]["object"]["key"]
        size        = record["s3"]["object"]["size"]
        uploaded_at = record["eventTime"]   # ISO 8601 string from S3 event

        if not BATCH_KEY_RE.match(key):
            print(f"[SKIP] unrecognized key: {key}")
            continue

        batch_id = key.split("/")[-1]   # posts_batch_XXXXX.xml.gz

        # 1. Write DynamoDB manifest entry (status="uploaded")
        dynamodb.Table(DYNAMODB_TABLE).put_item(Item={
            "batch_id":    batch_id,
            "s3_path":     f"s3://{BUCKET}/{key}",
            "size_bytes":  size,
            "uploaded_at": uploaded_at,
            "status":      "uploaded",
        })

        # 2. Enqueue SQS message for Phase 2 (--mode sqs) to consume
        sqs.send_message(
            QueueUrl=SQS_URL,
            MessageBody=json.dumps({
                "batch_id": batch_id,
                "s3_path":  f"s3://{BUCKET}/{key}",
            }),
        )

        print(f"[OK] {batch_id}  size={size}  uploaded_at={uploaded_at}")

    return {"statusCode": 200}
