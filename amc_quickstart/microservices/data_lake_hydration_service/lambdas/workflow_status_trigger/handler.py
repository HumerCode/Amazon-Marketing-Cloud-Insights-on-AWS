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


#!/usr/bin/env python3
# File Name: WorkflowStatusTableTrigger.py
# Author: Joshua Witt jwittaws@amazon.com
# Description:
# This lambda is a trigger for the workflow status table to send sns notifications when certain statuses appear on workflow execution records

import boto3
import json
import os
from aws_lambda_powertools import Logger
from wfm import wfm_utils

logger = Logger(service="WorkFlowManagement", level="INFO")
wfmutils = wfm_utils.Utils(logger)

def lambda_handler(event, context):

    logger.info('event: {}'.format(event))
    snsResultMessage = ''
    result=''

    for record in event['Records']:
        dynamodbTable = record['eventSourceARN'].split('/')[1]

        if 'dynamodb' in record and 'NewImage' in record['dynamodb'] and 'executionStatus' in record['dynamodb'][
            'NewImage'] and 'S' in record['dynamodb']['NewImage']['executionStatus']:
            newRecord = wfmutils.deseralize_dynamodb_item(record['dynamodb']['NewImage'])

            if newRecord['executionStatus'] not in os.environ['IGNORE_STATUS_LIST'].split(','):
                customerConfigs = wfmutils.dynamodb_get_customer_config_records(
                    os.environ['CUSTOMERS_DYNAMODB_TABLE'])

                if newRecord['customerId'] in customerConfigs:
                    config = customerConfigs[newRecord['customerId']]
                    subject = '{} Execution {}'.format(newRecord['workflowId'], newRecord['executionStatus'])
                    message = 'Execution {} for workflow {} created on {} for time window {} to {} status is {} event info: {}'.format(
                        newRecord['workflowExecutionId'], newRecord['workflowId'], newRecord['createTime'],
                        newRecord['timeWindowStart'], newRecord['timeWindowEnd'], newRecord['executionStatus'],
                        json.dumps(record))
                    result = wfmutils.sns_publish_message( config['AMC']['WFM']['snsTopicArn'], subject, message)
                    print(result)
                    if result['ResponseMetadata']['HTTPStatusCode'] == 200:
                        snsResultMessage = "Successfully sent subject {} and message {} to SNS topic {} \n sns Response: {}".format(
                            subject, message, config['AMC']['WFM']['snsTopicArn'], result)
                        logger.info(snsResultMessage)
                    else:
                        snsResultMessage = "Failed to sent subject {} and message {} to SNS topic {} \n sns Response: {}".format(
                            subject, message, config['AMC']['WFM']['snsTopicArn'], result)
                        logger.error(snsResultMessage)
    return {"message": snsResultMessage, "result": result}
