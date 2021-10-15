#!/usr/bin/env python3
# File Name: GetAMCWorkflowLibrary/src/lambda_function.py
# Author: Tejal Gohil gohiteja@amazon.com
# Description:
# The Lambda function to perform get operations on tps-AMC Workflow Library table.
import boto3
import json
import os
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# POWERTOOLS_SERVICE_NAME defined
logger = Logger(service="TPSAMCWorkflowLibrary", level="INFO")
dynamo = boto3.resource('dynamodb')
ssm = boto3.client('ssm')
table_path = os.environ['AMC_WORKFLOW_LIBRARY_DYNAMODB_TABLE']
table_ssm_param = ssm.get_parameter(
    Name=table_path
)
table_name = table_ssm_param['Parameter']['Value']
table = dynamo.Table(table_name)
logger.info('AMCWorkflowLibraryTable Name: {}'.format(table))

def get_item(table, key):
    try:
        response = table.query(
                KeyConditionExpression=Key('tenantId').eq(key)
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            logger.info(e.response['Error']['Message'])
        else:
            logger.info(e.response['Error']['Code'])
            logger.info(e.response['Error']['Message'])
            raise
    else:
        return response

def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': err.message if err else json.dumps(res),
        'headers': {
            'Content-Type': 'application/json',
        },
    }

def lambda_handler(event, context):
    logger.info('event: {}'.format(json.dumps(event, indent=2)))
    operation = event['httpMethod']
    #Process only GET Http method
    if operation == 'GET':
        payload = event['queryStringParameters']
        logger.info('Payload: {}'.format(payload))
        key = payload['tenantId']
        # logger.info('TenantTable key: {}'.format(key))
        return respond(None, get_item(table, key))
    else:
        return respond(ValueError('Unsupported method "{}"'.format(operation)))