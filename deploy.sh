#!/bin/bash
set -euo pipefail

STACK_NAME="${STACK_NAME:-bedrock-token-monitor}"
REGION="${AWS_REGION:-ap-northeast-1}"
SNS_EMAIL="${SNS_EMAIL:?SetYourEmailAddress SNS_EMAIL (example: alert@example.com)}"
SNS_TOPIC_NAME="${SNS_TOPIC_NAME:-bedrock-token-alert}"
BEDROCK_LOG_GROUP="${BEDROCK_LOG_GROUP:-/aws/bedrock/}"

sam build --template-file template.yaml

sam deploy \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --resolve-s3 \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides \
    SnsEmail="$SNS_EMAIL" \
    SnsTopicName="$SNS_TOPIC_NAME" \
    BedrockLogGroupName="$BEDROCK_LOG_GROUP" \
  --no-fail-on-empty-changeset

echo "Deployed."
