#!/usr/bin/env python3
# File Name: WorkflowTableTrigger.py
# Author: Joshua Witt jwittaws@amazon.com
# Description:
# The Lambda function invokes the AMC API lambda to perform CRUD operations on workflows based upon the workflow table.

import boto3
import json
import os
from aws_lambda_powertools import Logger
from wfm import wfm_utils

logger = Logger(service="WorkFlowManagement", level="INFO")
wfmutils = wfm_utils.Utils(logger)


def invoke_amc_api(customer_id, payload, method='createWorkflow'):
    # create an event that will be sent to the email-s3-file lambda
    client = boto3.client('lambda')
    logger.info('function name: {}'.format(os.environ['AMC_API_INTERFACE_FUNCTION_NAME']))
    lambda_invoke_response = client.invoke(
        FunctionName=os.environ['AMC_API_INTERFACE_FUNCTION_NAME'],
        InvocationType='Event',
        Payload=bytes(json.dumps(
            {
                'customerId': customer_id,
                'method': method,
                'payload': payload
            }, default=wfmutils.json_encoder_default
        ), 'utf-8')
    )

    logger.info('response payload: {}'.format(lambda_invoke_response))
    return lambda_invoke_response


def lambda_handler(event, context):

    logger.info('event: {}'.format(event))
    for record in event['Records']:
        logger.info('dynamoDB Record: {}'.format(record))

        if 'dynamodb' in record and 'NewImage' in record['dynamodb']:
            new_record = wfmutils.deseralize_dynamodb_item(record['dynamodb']['NewImage'])
            logger.info('NewRecord:{}'.format(new_record))
        if 'dynamodb' in record and 'OldImage' in record['dynamodb']:
            old_record = wfmutils.deseralize_dynamodb_item(record['dynamodb']['OldImage'])

        if record['eventName'] == 'INSERT':
            invoke_lambda_response = invoke_amc_api(new_record['customerId'], new_record, 'createWorkflow')

        if record['eventName'] == 'MODIFY':
            invoke_lambda_response = invoke_amc_api(new_record['customerId'], new_record, 'updateWorkflow')

        if record['eventName'] == 'REMOVE':
            invoke_lambda_response = invoke_amc_api(old_record['customerId'], old_record, 'deleteWorkflow')

        return invoke_lambda_response['ResponseMetadata']['HTTPStatusCode']
