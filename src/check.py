"""
定期実行で DynamoDB を走査し、identity ごとの直近ウィンドウ内
トークン合計が閾値を超えていれば SNS 通知する Lambda 関数。
"""

import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3

TABLE_NAME = os.environ["TABLE_NAME"]
TOKEN_THRESHOLD = int(os.environ["TOKEN_THRESHOLD"])
WINDOW_MINUTES = int(os.environ.get("WINDOW_MINUTES", "10"))
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)
sns = boto3.client("sns")


def handler(event, context):
    cutoff_epoch = int(time.time()) - (WINDOW_MINUTES * 60)

    # DynamoDB 全件スキャンし、ウィンドウ内のレコードを identity ごとに集計
    identity_usage = defaultdict(lambda: {"input": 0, "output": 0, "total": 0, "count": 0})

    scan_kwargs = {}
    while True:
        response = table.scan(**scan_kwargs)
        for item in response.get("Items", []):
            if int(item.get("epoch", 0)) < cutoff_epoch:
                continue
            arn = item["pk"]
            identity_usage[arn]["input"] += int(item.get("input_tokens", 0))
            identity_usage[arn]["output"] += int(item.get("output_tokens", 0))
            identity_usage[arn]["total"] += int(item.get("total_tokens", 0))
            identity_usage[arn]["count"] += 1

        if "LastEvaluatedKey" not in response:
            break
        scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    # 閾値超過チェック & 通知
    breaching = {arn: usage for arn, usage in identity_usage.items() if usage["total"] > TOKEN_THRESHOLD}

    if not breaching:
        print(f"No identity exceeded threshold ({TOKEN_THRESHOLD} tokens) in the last {WINDOW_MINUTES} minutes.")
        return {"breaching_count": 0}

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [
        f"[Bedrock Token Alert] {now_str}",
        f"直近 {WINDOW_MINUTES} 分間でトークン閾値 ({TOKEN_THRESHOLD:,}) を超過した identity:\n",
    ]
    for arn, usage in breaching.items():
        lines.append(
            f"  Identity: {arn}\n"
            f"    入力トークン: {usage['input']:,}\n"
            f"    出力トークン: {usage['output']:,}\n"
            f"    合計トークン: {usage['total']:,}\n"
            f"    リクエスト数: {usage['count']}\n"
        )

    message = "\n".join(lines)
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"Bedrock Token Alert: {len(breaching)} identity(s) exceeded threshold",
        Message=message,
    )
    print(f"Alert sent for {len(breaching)} identity(s).")
    return {"breaching_count": len(breaching), "identities": list(breaching.keys())}
