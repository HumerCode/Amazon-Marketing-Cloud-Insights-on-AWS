#!/usr/bin/env python3
# File Name: WorkflowTableTrigger.py
# Author: Joshua Witt jwittaws@amazon.com
# Description:
# The Lambda function invokes the AMC API lambda to perform CRUD operations on workflows based upon the workflow table.

import boto3
import json
import os
import logging
from aws_lambda_powertools import Logger
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

# POWERTOOLS_SERVICE_NAME defined
logger = Logger(service="WorkFlowManagement", level="INFO")

def deserializeDyanmoDBItem(item):
    return {k: TypeDeserializer().deserialize(value=v) for k, v in item.items()}

def pushToSNSTopic(snsTopicArn, subject, message):
    client = boto3.client('sns')
    response = client.publish(
        TargetArn=snsTopicArn, 
        Message=json.dumps(message),
        Subject=subject[:100],
         MessageStructure='string',
         MessageGroupId='WorkflowLibrary'
    )
    return(response)

    
def lambda_handler(event, context):
    logger.info('event: {}'.format(event))
    for record in event['Records']:
        logger.info('dynamoDB Record: {}'.format(record))
        dynamodbTable = record['eventSourceARN'].split('/')[1]
        snsTopicArn = os.environ['SNS_TOPIC_ARN']
        DynamoDBRecords = {'eventSourceARN': record['eventSourceARN'], 'eventName': record['eventName'], 'dynamodbTable': dynamodbTable }
        if 'dynamodb' in record and 'NewImage' in record['dynamodb'] :
            DynamoDBRecords['NewImage'] = deserializeDyanmoDBItem(record['dynamodb']['NewImage'])
        if 'dynamodb' in record and 'OldImage' in record['dynamodb'] :
            DynamoDBRecords['OldImage'] = deserializeDyanmoDBItem(record['dynamodb']['OldImage'])
        logger.info('Customer Record {} for table {} : {}'.format(record['eventName'],dynamodbTable,dynamodbTable))
        
        PushToSNSTopicResponse = pushToSNSTopic(snsTopicArn, 'Customer Record {} for table {}'.format(record['eventName'],dynamodbTable), DynamoDBRecords )
        logger.info('Push To SNS Topic {} Response {}'.format(snsTopicArn, PushToSNSTopicResponse))
        return (PushToSNSTopicResponse)