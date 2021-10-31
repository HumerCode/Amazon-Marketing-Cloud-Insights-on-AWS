#!/usr/bin/env python3
# File Name: AddAMCWorkflowLibraryWorkflowmgnt/src/lambda_function.py
# Author: Tejal Gohil gohiteja@amazon.com
# Description:
# The Lambda function invokes the AMC API lambda to perform CRUD operations on workflows based upon the workflow table.

import boto3
import json
import os
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

# POWERTOOLS_SERVICE_NAME defined
logger = Logger(service="TPSAMCWorkflowLibrary", level="INFO")
dynamoDB_client = boto3.client('dynamodb')
#Use SSM Parameter for target table
workflow_service_table = os.getenv("AMC_WORKFLOW_LIBRARY_WORKFLOWMGNT_DYNAMODB_TABLE")

def put_item(table, item, key):
    try:
        logger.info('dynamoDB calling put_item api')
        response = dynamoDB_client.put_item(
            TableName=table,
            Item=item,
            ConditionExpression=f"attribute_not_exists({key})",
        )
        logger.info('dynamoDB response: {}'.format(response))
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            logger.info(e.response['Error']['Message'])
        else:
            raise
    else:
        return response

def update_item(table, item, key):
    try:
        logger.info('dynamoDB calling update_item api')
        response = dynamoDB_client.update_item(
            TableName=table,
            Item=item,
            # ConditionExpression=f"attribute_exists({key})",
        )
        logger.info('dynamoDB response: {}'.format(response))
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            logger.info(e.response['Error']['Message'])
        else:
            logger.info(e.response['Error']['Code'])
            logger.info(e.response['Error']['Message'])
            raise
    else:
        return response

def lambda_handler(event, context):
    try:
        logger.info('event: {}'.format(event))
        logger.info('Workflow Service AMC Workflow Library Table is: {}'.format(workflow_service_table))

        for record in event['Records']:
            logger.info('dynamoDB Record: {}'.format(record))
            dynamodbTable = record['eventSourceARN'].split('/')[1]

            operation = record['eventName']
            # key = list(record['dynamodb']['Keys'].keys())[0]
            key = "customerId"
            old_key = list(record['dynamodb']['Keys'].keys())[0]
            logger.info('dynamoDB record key is: {}'.format(key))
            logger.info('dynamoDB record old_key is: {}'.format(old_key))
            if operation == 'INSERT':
                logger.info('dynamoDB record operation: {}'.format(operation))
                #Workflow Service Key is different than TPS service
                item = record['dynamodb']['NewImage']
                item[key] = item.pop(old_key)
                put_item(workflow_service_table, item, key)
            if operation == 'MODIFY':
                logger.info('dynamoDB record operation: {}'.format(operation))
                #Workflow Service Key is different than TPS service
                item = record['dynamodb']['NewImage']
                item[key] = item.pop(old_key)
                update_item(workflow_service_table, item, key)
    except Exception as e:
        logger.error('Fatal error', exc_info=True)
        raise e
    return 'Successfully processed {} records.'.format(len(event['Records']))
