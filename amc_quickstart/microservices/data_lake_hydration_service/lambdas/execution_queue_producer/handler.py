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


# This function was created to queue AMC Workflow requests in order to prevent execution timeouts as AMC will only run a certin number of workflows at a time
# Requests that are received will be put into an SQS queue that will be consumed by conuser Lambda function that ensures only the configured number of current
# executions are submitted

import json
import boto3
import uuid
import os

from aws_lambda_powertools import Logger

logger = Logger(service="WorkFlowManagement", level="INFO")

from wfm import wfm_utils

wfmutils = wfm_utils.Utils(logger)


def lambda_handler(event, context):
    customers_dynamodb_table_name = os.environ['CUSTOMERS_DYNAMODB_TABLE']
    logger.info('received event {}'.format(event))
    # Check to make sure a run request payload exists in the event
    if 'payload' not in event or 'customerId' not in event:
        message = 'The request must have a customerID, and a payload with a workflowId'
        logger.info(message)
        return {
            "message": message,
            "statusCode": 500
        }
    customerConfigs = {}
    if type(event['customerId']) != list:
        customerConfigs = wfmutils.dynamodb_get_customer_config_records(customers_dynamodb_table_name,
                                                                        event['customerId'])

    if type(event['customerId']) == list:
        customer_config_result = wfmutils.dynamodb_get_customer_config_records(customers_dynamodb_table_name)
        for customer_id in event['customerId']:
            customerConfigs[customer_id] = customer_config_result[customer_id]

    sqsResponses = []
    all_response_codes = [200]
    for customerId in customerConfigs:
        response_codes = []
        responses = []
        customerConfig = customerConfigs[customerId]
        messages_sent_successfully = 0
        messages_failed_to_send = 0

        # Connect to the SQS Queue
        sqs = boto3.resource('sqs')
        queue = boto3.resource('sqs').get_queue_by_name(
            QueueName=customerConfig['AMC']['WFM']['amcWorkflowExecutionSQSQueueName'])
        if type(event['payload']) == dict:
            event['payload'] = [event['payload']]

        if type(event['payload']) == list:
            payloadFullList = event['payload']
            payloadGrouped = [payloadFullList[i * 10:(i + 1) * 10] for i in
                              range((len(payloadFullList) + 10 - 1) // 10)]
            for payloadGroup in payloadGrouped:
                messagesToSend = []
                for payloadItem in payloadGroup:

                    # Process the parameters to enable now() and today() functions
                    if "parameterValues" in payloadItem:
                        for parameter in payloadItem['parameterValues']:
                            payloadItem['parameterValues'][parameter] = wfmutils.process_parameter_functions(
                                payloadItem['parameterValues'][parameter])
                            logger.info("updated parameter {} to {}".format(parameter,
                                                                            payloadItem['parameterValues'][parameter]))
                    if 'timeWindowStart' in payloadItem:
                        payloadItem['timeWindowStart'] = wfmutils.process_parameter_functions(
                            payloadItem['timeWindowStart'])
                        logger.info("updated parameter timeWindowStart to {}".format(payloadItem['timeWindowStart']))
                    if 'timeWindowEnd' in payloadItem:
                        payloadItem['timeWindowEnd'] = wfmutils.process_parameter_functions(
                            payloadItem['timeWindowEnd'])
                        logger.info("updated parameter timeWindowEnd to {}".format(payloadItem['timeWindowEnd']))

                    if 'MessageGroupId' in payloadItem:
                        MessageGroupId = payloadItem['MessageGroupId']
                    else:
                        MessageGroupId = 'amcworkflows'
                    messagesToSend.append({
                        'Id': str(uuid.uuid1()),
                        'MessageBody': json.dumps({"customerId": customerId, "payload": payloadItem}),
                        'MessageGroupId': MessageGroupId,
                        'MessageAttributes': {
                            'customerId': {
                                'StringValue': customerId,
                                'DataType': 'String'
                            },
                            'workflowId': {
                                'StringValue': payloadItem['workflowId'],
                                'DataType': 'String'
                            }
                        }
                    })
                send_messages_responses = queue.send_messages(Entries=messagesToSend)
                if 'Successful' in send_messages_responses:
                    for response in send_messages_responses['Successful']:
                        messages_sent_successfully = messages_sent_successfully + 1
                        responses.append({
                            "HTTPStatusCode": 200,
                            "MessageId": response['MessageId']
                        })
                        response_codes.append(200)
                if 'Failed' in send_messages_responses:
                    for response in send_messages_responses['Failed']:
                        messages_failed_to_send = messages_failed_to_send + 1
                        responses.append({
                            "HTTPStatusCode": response['Code'],
                            "MessageId": ""
                        })
                        response_codes.append(response['Code'])

        all_response_codes.append(max(response_codes))
        sqsResponses.append(
            {"customerId": customerId, "messages_failed_to_send": messages_failed_to_send,
             "messages_sent_successfully": messages_sent_successfully, "statusCode": max(response_codes),
             "responses": responses.copy()})
    logger.info(sqsResponses)

    return {
        'statusCode': max(all_response_codes),
        'body': sqsResponses
    }
