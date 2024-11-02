# cdk/stacks/storage_stack.py
from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_s3 as s3,
    aws_dynamodb as dynamodb
)
from constructs import Construct

class StorageStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Source S3 Bucket
        self.bucket_src = s3.Bucket(
            self,
            "BucketSrc",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # Destination S3 Bucket
        self.bucket_dst = s3.Bucket(
            self,
            "BucketDst",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # DynamoDB Table
        self.backup_table = dynamodb.Table(
            self,
            "BackupTable",
            partition_key=dynamodb.Attribute(
                name="OriginalObjectName",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="CopyTimestamp",
                type=dynamodb.AttributeType.NUMBER
            ),
            removal_policy=RemovalPolicy.DESTROY,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        # Global Secondary Index for Disowned Copies
        self.backup_table.add_global_secondary_index(
            index_name="DisownedIndex",
            partition_key=dynamodb.Attribute(
                name="Disowned",
                type=dynamodb.AttributeType.STRING  # Changed from BOOLEAN to STRING
            ),
            sort_key=dynamodb.Attribute(
                name="DisownTimestamp",
                type=dynamodb.AttributeType.NUMBER
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )
