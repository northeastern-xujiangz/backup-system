# cdk/app.py
import aws_cdk as cdk
from stacks.storage_stack import StorageStack
from stacks.replicator_stack import ReplicatorStack
from stacks.cleaner_stack import CleanerStack

app = cdk.App()

storage_stack = StorageStack(app, "StorageStack")

replicator_stack = ReplicatorStack(
    app,
    "ReplicatorStack",
    bucket_src=storage_stack.bucket_src,
    bucket_dst=storage_stack.bucket_dst,
    table=storage_stack.backup_table
)

cleaner_stack = CleanerStack(
    app,
    "CleanerStack",
    bucket_dst=storage_stack.bucket_dst,
    table=storage_stack.backup_table
)

app.synth()
