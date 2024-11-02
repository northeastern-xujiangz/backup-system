# cdk/stacks/replicator_stack.py
from aws_cdk import (
    Stack,
    Duration,  # Direct import of Duration
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_s3_notifications as s3n
)
from constructs import Construct
from typing import Any

class ReplicatorStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        bucket_src: s3.Bucket,
        bucket_dst: s3.Bucket,
        table: dynamodb.Table,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Replicator Lambda Function
        replicator_lambda = _lambda.Function(
            self,
            "ReplicatorLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="replicator.handler",
            code=_lambda.Code.from_asset("cdk/lambda/replicator"),
            environment={
                "BUCKET_DST": bucket_dst.bucket_name,
                "TABLE_NAME": table.table_name
            },
            timeout=Duration.seconds(30)  # Use Duration directly
        )

        # Grant necessary permissions
        bucket_dst.grant_read_write(replicator_lambda)
        bucket_src.grant_read(replicator_lambda)
        table.grant_read_write_data(replicator_lambda)

        # Add S3 event notifications to trigger Replicator Lambda
        notification_filter = s3.NotificationKeyFilter(prefix="")

        replicator_lambda.add_event_source(
            s3n.LambdaDestination(replicator_lambda),
            events=[s3.EventType.OBJECT_CREATED_PUT, s3.EventType.OBJECT_REMOVED_DELETE]
        )

        # Allow Lambda to be invoked by S3
        replicator_lambda.add_permission(
            "AllowS3Invoke",
            principal=iam.ServicePrincipal("s3.amazonaws.com"),
            source_arn=bucket_src.bucket_arn
        )
