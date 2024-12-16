const AWS = require("aws-sdk");
const DynamoDB = new AWS.DynamoDB.DocumentClient();
const SNS = new AWS.SNS();

const SNS_TOPIC_ARN = "arn:aws:sns:region:account-id:DemoFileProcessingNotifications";

exports.handler = async (event) => {
    console.log("Event Received:", JSON.stringify(event, null, 2));

    try {
        if (event.Records[0].eventSource === "aws:s3") {
            // Process S3 Trigger
            for (const record of event.Records) {
                const bucketName = record.s3.bucket.name;
                const objectKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));
                console.log(`File uploaded: ${bucketName}/${objectKey}`);

                // Save metadata to DynamoDB
                const timestamp = new Date().toISOString();
                await DynamoDB.put({
                    TableName: "DemoFileMetadata",
                    Item: {
                        FileName: objectKey,
                        UploadTimestamp: timestamp,
                        Status: "Processed",
                    },
                }).promise();
                console.log(`Metadata saved for file: ${objectKey}`);
            }
        } else if (event.Records[0].eventSource === "aws:dynamodb") {
            // Process DynamoDB Streams Trigger
            for (const record of event.Records) {
                if (record.eventName === "INSERT") {
                    const newItem = record.dynamodb.NewImage;

                    // Construct notification message
                    const message = `File ${newItem.FileName.S} uploaded at ${newItem.UploadTimestamp.S} has been processed.`;
                    console.log("Sending notification:", message);

                    // Send notification via SNS
                    await SNS.publish({
                        TopicArn: SNS_TOPIC_ARN,
                        Message: message,
                    }).promise();
                    console.log("Notification sent successfully.");
                }
            }
        }

        return {
            statusCode: 200,
            body: "Event processed successfully!",
        };
    } catch (error) {
        console.error("Error processing event:", error);
        throw error;
    }
};