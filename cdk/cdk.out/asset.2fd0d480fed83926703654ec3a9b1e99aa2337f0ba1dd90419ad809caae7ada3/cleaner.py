# cdk/lambda/cleaner/cleaner.py

import os
import boto3
import time

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

BUCKET_DST = os.environ['BUCKET_DST']
TABLE_NAME = os.environ['TABLE_NAME']
GSI_NAME = os.environ['GSI_NAME']
CLEAN_THRESHOLD_SECONDS = int(os.environ.get('CLEAN_THRESHOLD_SECONDS', '10'))
table = dynamodb.Table(TABLE_NAME)

def handler(event, context):
    now = int(time.time() * 1000)
    threshold = now - (CLEAN_THRESHOLD_SECONDS * 1000)
    
    # Query disowned copies older than threshold
    response = table.query(
        IndexName=GSI_NAME,
        KeyConditionExpression='Disowned = :disowned AND DisownTimestamp <= :threshold',
        ExpressionAttributeValues={
            ':disowned': "true",  # Corrected to string
            ':threshold': threshold
        }
    )
    
    items = response.get('Items', [])
    print(f"Found {len(items)} disowned copies to clean.")
    
    for item in items:
        # Delete the copy from S3
        s3_client.delete_object(Bucket=BUCKET_DST, Key=item['CopyObjectName'])
        print(f"Deleted disowned copy: {item['CopyObjectName']}")
        
        # Remove the item from DynamoDB
        table.delete_item(
            Key={
                'OriginalObjectName': item['OriginalObjectName'],
                'CopyTimestamp': item['CopyTimestamp']
            }
        )
        print(f"Removed copy record from DynamoDB: {item['CopyObjectName']}")
    
    return f"Cleaned {len(items)} disowned copies."
