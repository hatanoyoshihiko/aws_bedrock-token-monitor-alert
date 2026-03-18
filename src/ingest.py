"""
Bedrock Model Invocation Log を受信し、
identity (IAMユーザー/ロール/APIキー) ごとのトークン使用量を
DynamoDB に記録する Lambda 関数。
"""

import base64
import gzip
import json
import os
import time

import boto3

TABLE_NAME = os.environ["TABLE_NAME"]
WINDOW_MINUTES = int(os.environ.get("WINDOW_MINUTES", "10"))

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)


def handler(event, context):
    # CloudWatch Logs Subscription Filter からのデータをデコード
    payload = base64.b64decode(event["awslogs"]["data"])
    log_data = json.loads(gzip.decompress(payload))

    with table.batch_writer() as batch:
        for log_event in log_data.get("logEvents", []):
            try:
                record = json.loads(log_event["message"])
            except (json.JSONDecodeError, KeyError):
                continue

            identity_arn = record.get("identity", {}).get("arn", "")
            if not identity_arn:
                continue

            input_tokens = record.get("input", {}).get("inputTokenCount", 0) or 0
            output_tokens = record.get("output", {}).get("outputTokenCount", 0) or 0
            total_tokens = input_tokens + output_tokens

            if total_tokens == 0:
                continue

            timestamp = record.get("timestamp", "")
            request_id = record.get("requestId", "")
            model_id = record.get("modelId", "")
            now_epoch = int(time.time())
            ttl_value = now_epoch + (WINDOW_MINUTES * 60) + 300  # ウィンドウ + 5分バッファ

            batch.put_item(
                Item={
                    "pk": identity_arn,
                    "sk": f"{timestamp}#{request_id}",
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "total_tokens": total_tokens,
                    "model_id": model_id,
                    "timestamp": timestamp,
                    "epoch": now_epoch,
                    "ttl": ttl_value,
                }
            )
