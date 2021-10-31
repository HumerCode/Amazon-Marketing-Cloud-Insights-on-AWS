#!/usr/bin/env python3
# File Name: AddTenant/src/lambda_function.py
# Author: Tejal Gohil gohiteja@amazon.com
# Description:
# The Lambda function to perform add operations on tps-tenant table.

import boto3
import json
import os
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

# POWERTOOLS_SERVICE_NAME defined
logger = Logger(service="TPSTenant", level="INFO")
dynamo = boto3.resource('dynamodb')
ssm = boto3.client('ssm')
table_path = os.environ['TENANT_DYNAMODB_TABLE']
table_ssm_param = ssm.get_parameter(
        Name=table_path
    )
table_name = table_ssm_param['Parameter']['Value']
table = dynamo.Table(table_name)

def put_item(table, item, key):
    try:
        response = table.put_item(
            Item=item,
            ConditionExpression=f"attribute_not_exists({key})",
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
    table_ssm_param = ssm.get_parameter(
        Name=table_path
    )
    table_name = table_ssm_param['Parameter']['Value']
    logger.info('TenantTable Name: {}'.format(table_name))
    operation = event['httpMethod']
    #Process only POST Http method
    if operation == 'POST':
        logger.info('event: {}'.format(json.dumps(event, indent=2)))
        logger.info('TenantTable Name: {}'.format(table))
        operation = event['httpMethod']
        # Process only POST Http method
        if operation == 'POST':
            payload = json.loads(event['body'])
            logger.info('Payload: {}'.format(payload))
            key = "tenantId"
            logger.info('TenantTable key: {}'.format(key))
            return respond(None, put_item(table, payload, key))
    else:
        return respond(ValueError('Unsupported method "{}"'.format(operation)))