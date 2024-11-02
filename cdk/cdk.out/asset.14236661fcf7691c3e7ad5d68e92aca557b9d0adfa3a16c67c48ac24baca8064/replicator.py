# cdk/lambda/replicator/replicator.py

import json
import os
import boto3
import time
from urllib.parse import unquote_plus

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

BUCKET_DST = os.environ['BUCKET_DST']
TABLE_NAME = os.environ['TABLE_NAME']
table = dynamodb.Table(TABLE_NAME)

def handler(event, context):
    print("Received event:", json.dumps(event, indent=2))
    
    for record in event['Records']:
        event_name = record['eventName']
        bucket_src = record['s3']['bucket']['name']
        object_key = unquote_plus(record['s3']['object']['key'])
        
        if event_name.startswith('ObjectCreated:Put'):
            handle_put_event(object_key)
        elif event_name.startswith('ObjectRemoved:Delete'):
            handle_delete_event(object_key)

def handle_put_event(object_key):
    timestamp = int(time.time() * 1000)
    copy_object_name = f"{object_key}-{timestamp}"
    
    # Copy the object to BucketDst
    copy_source = {'Bucket': bucket_src, 'Key': object_key}
    s3_client.copy_object(
        Bucket=BUCKET_DST,
        Key=copy_object_name,
        CopySource=copy_source
    )
    print(f"Copied {object_key} to {copy_object_name} in {BUCKET_DST}")
    
    # Query existing copies
    response = table.query(
        KeyConditionExpression='OriginalObjectName = :original',
        ExpressionAttributeValues={
            ':original': object_key
        },
        ScanIndexForward=True  # Ascending order
    )
    
    items = response.get('Items', [])
    if len(items) > 0:
        # Delete the oldest copy
        oldest = items[0]
        s3_client.delete_object(Bucket=BUCKET_DST, Key=oldest['CopyObjectName'])
        print(f"Deleted oldest copy: {oldest['CopyObjectName']}")
        
        # Remove the oldest entry from DynamoDB
        table.delete_item(
            Key={
                'OriginalObjectName': object_key,
                'CopyTimestamp': oldest['CopyTimestamp']
            }
        )
    
    # Insert the new copy into DynamoDB
    table.put_item(
        Item={
            'OriginalObjectName': object_key,
            'CopyTimestamp': timestamp,
            'CopyObjectName': copy_object_name,
            'Disowned': "false",  # Corrected to string
            'DisownTimestamp': None
        }
    )
    print(f"Inserted new copy record into DynamoDB: {copy_object_name}")

def handle_delete_event(object_key):
    # Query all copies of the deleted object
    response = table.query(
        KeyConditionExpression='OriginalObjectName = :original',
        ExpressionAttributeValues={
            ':original': object_key
        }
    )
    
    items = response.get('Items', [])
    
    # Mark each copy as disowned
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(
                Item={
                    'OriginalObjectName': item['OriginalObjectName'],
                    'CopyTimestamp': item['CopyTimestamp'],
                    'CopyObjectName': item['CopyObjectName'],
                    'Disowned': "true",  # Corrected to string
                    'DisownTimestamp': int(time.time() * 1000)
                }
            )
    
    print(f"Marked {len(items)} copies as disowned for {object_key}")
