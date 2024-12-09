package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type User struct {
	UserId       string `json:"userId"`
	Email        string `json:"email"`
	EmailEnabled bool   `json:"emailEnabled"`
}

func handler(ctx context.Context) (string, error) {
	sess := session.Must(session.NewSession())
	dynamo := dynamodb.New(sess)
	sqsSvc := sqs.New(sess)

	// Fetch users from DynamoDB
	params := &dynamodb.ScanInput{
		TableName:        aws.String("UsersTable"), // Replace with your DynamoDB table name
		FilterExpression: aws.String("emailEnabled = :enabled"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":enabled": {BOOL: aws.Bool(true)},
		},
	}

	var users []User
	err := dynamo.ScanPages(params, func(page *dynamodb.ScanOutput, lastPage bool) bool {
		for _, item := range page.Items {
			var user User
			err := dynamodbattribute.UnmarshalMap(item, &user)
			if err == nil && user.Email != "" {
				users = append(users, user)
			}
		}
		return !lastPage
	})
	if err != nil {
		return "", err
	}

	// Batch users and send to SQS
	for i := 0; i < len(users); i += 100 {
		end := i + 100
		if end > len(users) {
			end = len(users)
		}
		batch := users[i:end]

		message, _ := json.Marshal(batch)
		_, err := sqsSvc.SendMessage(&sqs.SendMessageInput{
			QueueUrl:    aws.String("https://sqs.ap-southeast-1.amazonaws.com/account-id/UserProcessedQueue"), // Replace with your SQS URL
			MessageBody: aws.String(string(message)),
		})
		if err != nil {
			log.Println(err)
		}
	}

	return "Users batched and sent to SQS!", nil
}

func main() {
	lambda.Start(handler)
}
