# cdk/stacks/cleaner_stack.py
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_events as events,
    aws_events_targets as targets
)
from constructs import Construct
from typing import Any

class CleanerStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        bucket_dst: s3.Bucket,
        table: dynamodb.Table,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Cleaner Lambda Function
        cleaner_lambda = _lambda.Function(
            self,
            "CleanerLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="cleaner.handler",
            code=_lambda.Code.from_asset("cdk/lambda/cleaner"),
            environment={
                "BUCKET_DST": bucket_dst.bucket_name,
                "TABLE_NAME": table.table_name,
                "GSI_NAME": "DisownedIndex",
                "CLEAN_THRESHOLD_SECONDS": "10"
            },
            timeout=cdk.Duration.seconds(30)
        )

        # Grant necessary permissions
        bucket_dst.grant_read_write(cleaner_lambda)
        table.grant_read_write_data(cleaner_lambda)

        # Schedule the Cleaner Lambda to run every 5 seconds using EventBridge
        # Note: AWS EventBridge supports a minimum interval of 1 minute for rate expressions.
        # To achieve a 5-second interval, alternative approaches are required.
        # For demonstration, we'll set it to run every 1 minute.

        rule = events.Rule(
            self,
            "CleanerScheduleRule",
            schedule=events.Schedule.rate(cdk.Duration.seconds(60))  # 1 minute
        )

        rule.add_target(targets.LambdaFunction(cleaner_lambda))
