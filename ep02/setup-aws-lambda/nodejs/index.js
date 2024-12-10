const AWS = require('aws-sdk');
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const sqs = new AWS.SQS();
const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

exports.handler = async () => {
    try {
        // Fetch data from DynamoDB
        let params = {
            TableName: "UsersTable", // Replace with your DynamoDB table name
            FilterExpression: "emailEnabled = :enabled",
            ExpressionAttributeValues: { ":enabled": true }
        };

        let users = [];
        let data;
        do {
            data = await dynamoDB.scan(params).promise();
            users = users.concat(data.Items);
            params.ExclusiveStartKey = data.LastEvaluatedKey;
        } while (params.ExclusiveStartKey);

        // Validate and batch data
        const batches = [];
        for (let i = 0; i < users.length; i += 100) {
            const batch = users.slice(i, i + 100).filter(user => user.email && emailRegex.test(user.email)); // Validate email
            if (batch.length > 0) {
                batches.push(batch);
            }
        }

        // Send batches to SQS
        for (const batch of batches) {
            const sqsParams = {
                QueueUrl: "https://sqs.ap-southeast-1.amazonaws.com/account-id/UserProcessedQueue", // Replace with your SQS URL
                MessageBody: JSON.stringify(batch)
            };
            await sqs.sendMessage(sqsParams).promise();
        }

        return { statusCode: 200, body: "Users batched and sent to SQS!" };
    } catch (error) {
        console.error(error);
        return { statusCode: 500, body: "Error processing users." };
    }
};
