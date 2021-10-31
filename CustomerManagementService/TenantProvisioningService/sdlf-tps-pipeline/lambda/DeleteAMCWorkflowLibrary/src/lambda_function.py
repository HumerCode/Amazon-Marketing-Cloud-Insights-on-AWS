#!/usr/bin/env python3
# File Name: DeleteAMCWorkflowLibrary/src/lambda_function.py
# Author: Tejal Gohil gohiteja@amazon.com
# Description:
# The Lambda function to perform delete operations on tps-AMC Workflow Library table.


import boto3
import json
import os
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr

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
logger.info('TenantTable Name: {}'.format(table))
def delete_item(table, key):
    try:
        response = table.delete_item(
            Key=key,
            ConditionExpression=Attr('tenantId').eq(key['tenantId']),
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            logger.info(e.response['Error']['Message'])
        else:
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
    #Process only DELETE Http method
    if operation == 'DELETE':
        key = json.loads(event['body'])
        logger.info('AMCWorkflowLibraryTable key: {}'.format(key))
        return respond(None, delete_item(table, key))
    else:
        return respond(ValueError('Unsupported method "{}"'.format(operation)))