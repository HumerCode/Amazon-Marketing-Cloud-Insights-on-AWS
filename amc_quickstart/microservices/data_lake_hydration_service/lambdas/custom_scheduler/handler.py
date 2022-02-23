# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

## Custom Scheduler
import json
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from decimal import Decimal
import os
import boto3
import datetime
import calendar
from datetime import datetime, timedelta, date, timedelta
import time

# POWERTOOLS_SERVICE_NAME defined
logger = Logger(service="WorkflowManagerService", level="INFO")

dynamo = boto3.resource('dynamodb')

workflow_schedule_table_name = os.environ['WORKFLOW_SCHEDULE_TABLE']
workflow_schedule_table = dynamo.Table(workflow_schedule_table_name)
logger.info('Workflow Schedule table Name: {}'.format(workflow_schedule_table))


def deseralize_dynamodb_item(item):
    return {k: TypeDeserializer().deserialize(value=v) for k, v in item.items()}


def default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError("Object of type '%s' is not JSON serializable" % type(obj).__name__)


def dynamodb_get_wf_schedule_records(dynamodb_table_name, frequency=None):
    workflow_schedules = []
    # Set up a DynamoDB Connection
    dynamodb = boto3.client('dynamodb')

    if frequency is None:
        # We want to get the whole table for all configs so we pagniate in case there is large number of items
        paginator = dynamodb.get_paginator('scan')
        response_iterator = paginator.paginate(
            TableName=dynamodb_table_name, PaginationConfig={'PageSize': 100}
        )
    else:
        logger.info("frequency is not empty!")
        logger.info("frequency is : {}".format(frequency))
        # paginate in case there is large number of items
        paginator = dynamodb.get_paginator('query')
        response_iterator = paginator.paginate(
            TableName=dynamodb_table_name,
            IndexName='custom-schdl-index',
            Select='ALL_ATTRIBUTES',
            Limit=1,
            ConsistentRead=False,
            ReturnConsumedCapacity='TOTAL',
            KeyConditions={
                'ScheduleExpression': {
                    'AttributeValueList': [
                        {
                            'S': frequency
                        },
                    ],
                    'ComparisonOperator': 'EQ'
                }
            }, PaginationConfig={'PageSize': 100})
    # Iterate over each page from the iterator
    for page in response_iterator:
        # deserialize each "item" (or record) into a dictionary
        if 'Items' in page:
            for item in page['Items']:
                wf_schedule = deseralize_dynamodb_item(item)
                # Run only "ENABLED" workflows
                if (wf_schedule['State'] == 'ENABLED'):
                    workflow_schedules.append(wf_schedule)
                    logger.info(wf_schedule)
    return workflow_schedules


def lambda_handler(event, context):
    frequency = ''
    utcdt = datetime.utcnow()
    hour = str(int(utcdt.hour))
    query = event.get('query')
    if 'custom(H' in query:
        frequency = query
    elif 'custom(D' in query:
        frequency = 'custom(D * {H})'.format(H=hour)
    elif 'custom(W' in query:
        weekday = str(int(utcdt.weekday()))
        frequency = 'custom(W {D} {H})'.format(D=weekday, H=hour)
    elif 'custom(M' in query:
        day_month = str(int(utcdt.day))
        frequency = 'custom(M {D} {H})'.format(D=day_month, H=hour)
    print('Frequency is {}'.format(frequency))
    
    logger.info(frequency)
        
    workflow_schedules = []
    try:
        workflow_schedules = dynamodb_get_wf_schedule_records(workflow_schedule_table_name, frequency)

    except ClientError as e:
        logger.info(e.response['Error']['Code'])
        logger.info(e.response['Error']['Message'])
    logger.info("workflow_schedules")
    logger.info(workflow_schedules)
    
    client = boto3.client('lambda')
    
    if len(workflow_schedules) > 0:
        for item in workflow_schedules:
            # Each item is dict
            payload = {
                'customerId': item['customerId'],
                'payload': item['Input']['payload']
            }
            print(payload)
            
            response = client.invoke(
                FunctionName=os.environ['EXECUTION_QUEUE_PRODUCER_LAMBA_ARN'].split(':')[-1],
                InvocationType='Event',
                Payload=json.dumps(payload)
                )
            print(response)

    return {
        'statusCode': 200,
        'payload': json.dumps(workflow_schedules, default=default),
        'messageId': "Success"
    }