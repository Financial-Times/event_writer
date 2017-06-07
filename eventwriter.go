package main

import (
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type eventWriter interface {
	WriteEvent(transactionID string, eventType string, timestamp time.Time) (err error)
}

type dynamoDBEventWriter struct {
	dynamodb *dynamodb.DynamoDB
}

func (ew dynamoDBEventWriter) WriteEvent(transactionID string, thingUUID string, eventType string, values map[string]string) (err error) {

	db := ew.dynamodb

	updateExpression := generateUpdateExpression(transactionID, thingUUID, values)
	log.Printf("updateExpression= %v", updateExpression)

	expressionAttributeValues, err := generateExpressionAttributeValues(transactionID, thingUUID, values)
	if err != nil {
		log.Printf("Failed to generate expression attribute values %v", err)
		return err
	}
	log.Printf("expressionAttributeValues= %v", expressionAttributeValues)

	out, err := db.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: aws.String("Transactions"),
		Key: map[string]*dynamodb.AttributeValue{
			"transactionID": {S: aws.String(transactionID)},
		},
		ExpressionAttributeValues: expressionAttributeValues,
		UpdateExpression:          &updateExpression,
		ReturnValues:              aws.String("ALL_NEW"),
	})

	if err != nil {
		log.Printf("Failed to update item %v", err)
		return err
	}
	log.Printf("Got output %v", out)

	return nil

}

func generateUpdateExpression(transactionID string, thingUUID string, values map[string]string) string {
	coreUpdateExpression := "SET thingUUID=:thingUUID, "

	updateValues := []string{}

	for name := range values {
		updateValues = append(updateValues, name+"=:"+name)
	}

	return coreUpdateExpression + strings.Join(updateValues, ", ")
}

func generateExpressionAttributeValues(transactionID string, thingUUID string, values map[string]string) (eav map[string]*dynamodb.AttributeValue, err error) {
	// create a new map with the required colon at the start of each name. Also avoid the
	// reserved word 'UUID'
	modifiedMap := make(map[string]string)

	for name, value := range values {
		modifiedMap[":"+name] = value
	}

	modifiedMap[":thingUUID"] = thingUUID

	return dynamodbattribute.MarshalMap(modifiedMap)

}
