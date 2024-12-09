import boto3
import json

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('UsersTable') # Replace with your table name
sqs = boto3.client('sqs')

def lambda_handler(event, context):
    try:
        # Fetch data from DynamoDB
        response = table.scan(
            FilterExpression="emailEnabled = :enabled",
            ExpressionAttributeValues={":enabled": True}
        )
        users = response['Items']

        # Validate and batch data
        batches = []
        for i in range(0, len(users), 100):
            batch = [user for user in users[i:i + 100] if 'email' in user]
            if batch:
                batches.append(batch)

        # Send batches to SQS
        for batch in batches:
            sqs.send_message(
                QueueUrl="https://sqs.ap-southeast-1.amazonaws.com/user-id/UserProcessedQueue", # Replace with your SQS URL
                MessageBody=json.dumps(batch)
            )

        return {"statusCode": 200, "body": "Users batched and sent to SQS!"}
    except Exception as e:
        print(e)
        return {"statusCode": 500, "body": "Error processing users."}
