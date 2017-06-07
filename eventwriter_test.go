package main

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func TestWriteFirstEvent(t *testing.T) {
	assert := assert.New(t)
	db := setupDynamoDBLocal(t)
	ew := dynamoDBEventWriter{db}
	m := make(map[string]string)
	m["startTime"] = time.Now().String()

	err := ew.WriteEvent("transID", "UUID", "CREATE", m)
	assert.NoError(err, "Should succeed")
}

func setupDynamoDBLocal(t *testing.T) *dynamodb.DynamoDB {
	assert := assert.New(t)
	sess, err := session.NewSession(&aws.Config{
		Region:   aws.String("us-west-2"),
		Endpoint: aws.String("http://localhost:8000")})
	assert.NoError(err, "Should be able to create a session talking to local DynamoDB. Make sure this is running")
	db := dynamodb.New(sess)

	params := &dynamodb.DescribeTableInput{
		TableName: aws.String("Transactions"),
	}

	_, err = db.DescribeTable(params)

	if err != nil { // the table doesn't exist
		if awsErr, ok := err.(awserr.Error); ok {
			log.Println("Error found:", awsErr.Code(), awsErr.Message())

			if awsErr.Code() == dynamodb.ErrCodeResourceNotFoundException {
				params := &dynamodb.CreateTableInput{
					AttributeDefinitions: []*dynamodb.AttributeDefinition{ // Required
						{ // Required
							AttributeName: aws.String("transactionID"),               // Required
							AttributeType: aws.String(dynamodb.ScalarAttributeTypeS), // Required
						},
						// More values...
					},
					KeySchema: []*dynamodb.KeySchemaElement{ // Required
						{ // Required
							AttributeName: aws.String("transactionID"),      // Required
							KeyType:       aws.String(dynamodb.KeyTypeHash), // Required
						},
						// More values...
					},
					ProvisionedThroughput: &dynamodb.ProvisionedThroughput{ // Required
						ReadCapacityUnits:  aws.Int64(10), // Required
						WriteCapacityUnits: aws.Int64(10), // Required
					},
					TableName: aws.String("Transactions"), // Required
				}
				_, err = db.CreateTable(params)

				assert.NoError(err, "Unexpected error")
			}
		} else {
			fmt.Println(err.Error())
		}
	}

	return db
}
