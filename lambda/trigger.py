"""
Lambda: S3 upload triggers Step Function pipeline.

Flow: S3 raw/ upload → this Lambda → Step Function → Glue jobs
"""

import json
import os
from datetime import datetime, timezone

import boto3

sfn = boto3.client("stepfunctions")
STATE_MACHINE_ARN = os.environ["STEP_FUNCTION_ARN"]


def lambda_handler(event, context):
    executions = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        if not key.endswith(".csv"):
            continue

        execution_input = {
            "bucket": bucket,
            "key": key,
            "triggered_at": datetime.now(timezone.utc).isoformat(),
        }

        name = f"sales-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

        response = sfn.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=name,
            input=json.dumps(execution_input),
        )
        executions.append(response["executionArn"])

    return {
        "statusCode": 200,
        "body": json.dumps({"executions_started": len(executions)}),
    }
