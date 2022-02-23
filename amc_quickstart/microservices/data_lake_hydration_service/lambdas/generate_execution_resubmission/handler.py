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


import json
import os
import boto3
from datetime import datetime, timedelta, timezone
from aws_lambda_powertools import Logger
from wfm import wfm_utils

logger = Logger(service="WorkFlowManagement", level="INFO")
wfmutils = wfm_utils.Utils(logger)


def remove_dictionary_items(dictionary, itemslist):
    for item in itemslist:
        if item in dictionary:
            del dictionary[item]
    return dictionary


def filter_dictionary_items(dictionary, itemslist):
    new_item = {}
    for item in itemslist:
        if item in dictionary:
            new_item[item] = dictionary[item]
    return new_item


def check_if_duplicate(potential_duplicate_item, list_of_items, list_of_properties):
    for existing_item in list_of_items:
        item_match_count = 0
        items_in_both_records = 0
        for item_to_check in list_of_properties:

            if item_to_check in potential_duplicate_item and item_to_check in existing_item:
                items_in_both_records = items_in_both_records + 1
                if potential_duplicate_item[item_to_check] == existing_item[item_to_check]:
                    item_match_count = item_match_count + 1
        if item_match_count == items_in_both_records:
            return (True)
    return False


def consume_dead_letter_queue(customer_config_record):
    log_messages = []
    workflow_execution_responses = []
    workflow_execution_response_codes = [200]
    executions_available = 0
    executions_submitted_count = 0
    executions_submitted = []
    messages_deleted = 0
    messages_received = []
    dlq_queue_name = ''
    # Get the queue
    logger.info('attemtping to consume from customer dead letter queue')
    if 'amcWorkflowExecutionDLQSQSQueueName' in config['AMC']['WFM']:
        dlq_queue_name = customer_config_record['AMC']['WFM']['amcWorkflowExecutionDLQSQSQueueName']
    elif 'amcWorkflowExecutionSQSQueueName' in config['AMC']['WFM']:
        dlq_queue_name = '{}-DLQ.fifo'.format(
            customer_config_record['AMC']['WFM']['amcWorkflowExecutionSQSQueueName'].split('.fifo')[0])

    queue = boto3.resource('sqs').get_queue_by_name(
        QueueName=dlq_queue_name)
    logger.info('customerId: {} executionsAvailable: {}'.format(customer_config_record['customerId'],
                                                                executions_available_result['executionsAvailable']))

    messagesToReceive = 3

    messages_received = queue.receive_messages(MessageAttributeNames=['customerId', 'workflowId'],
                                               MaxNumberOfMessages=messagesToReceive)

    messages_received_count = len(messages_received)
    logger.info('customerId: {} sqs queue: {} messages received: {}'.format(customer_config_record['customerId'],
                                                                            dlq_queue_name,
                                                                            messages_received_count))
    for message in messages_received:
        customerId = ''
        workflowId = ''
        runWorkflowResponse = {}
        runWorkflowRequest = {}
        if message.message_attributes is not None:
            customerId = message.message_attributes.get('customerId').get('StringValue')
            workflowId = message.message_attributes.get('workflowId').get('StringValue')
            logger.info(
                'Recevied Run request from Dead letter queue for customerId: {} workflowId: {} Body: {}'.format(
                    customerId, workflowId,
                    message.body))
            messageBody = json.loads(message.body)

            runWorkflowRequest = {
                'workflowId': workflowId,
                'customerConfig': customer_config_record,
                'amcApiEndpoint': customer_config_record['AMC']['amcApiEndpoint'],
                'payload': messageBody['payload']
            }

            runWorkflowResponse = executeWorkflow(customer_config_record, runWorkflowRequest.copy())
            logger.info('runWorkflowResponse:{}'.format(runWorkflowResponse))

            workflow_execution_responses.append(runWorkflowResponse.copy())
            workflow_execution_response_codes.append(runWorkflowResponse['statusCode'])

            if runWorkflowResponse['statusCode'] == 200:
                workflowExecutionId = runWorkflowResponse['body']['workflowExecutionId']
                executions_submitted.append(
                    {"customerId": customerId, 'workflowId:': workflowId, "executionId": workflowExecutionId,
                     "amcApiEndpoint": customer_config_record['AMC']['amcApiEndpoint'],
                     "statusCode": runWorkflowResponse['statusCode']})

                logmessage = " Successfully submitted workflow execution for workflowId: {} workflowExecutionId: {}".format(
                    workflowId, workflowExecutionId)
                updateExeuctionTrackingTable(customer_config_record, runWorkflowResponse['body'])

                logger.info(logmessage)
                log_messages.append(logmessage)
                # Let the queue know that the message is processed
                try:
                    message.delete()
                    logger.info('Deleted Message for customerid {} workflowid{}'.format(customerId, workflowId))
                    messages_deleted += 1

                except Exception as ex:
                    message = "Error occured when trying to submit workflow execution url: {} error message: {}".format(
                        customer_config_record['AMC']['amcApiEndpoint'], ex)
                    log_messages.append(message)
                    logger.error(message)


def lambda_handler(event, context):
    executionTable = os.environ['CUSTOMERS_DYNAMODB_TABLE']

    configs = wfmutils.dynamodb_get_customer_config_records(os.environ['CUSTOMERS_DYNAMODB_TABLE'], event['customerId'])
    for configKey in configs:
        config = configs[configKey]

        if 'lookbackHours' in event:
            lookback_hours = int(event['lookbackHours'])
        else:
            try:
                lookback_hours = (-1 * int(
                    config['AMC']['WFM']['syncWorkflowStatuses']['workflowExeuctionStatusLookBackHours']))
            except:
                lookback_hours = -300

        # calculate the minimum execution created date
        minimum_create_date_string = (datetime.today() + timedelta(hours=lookback_hours)).strftime('%Y-%m-%dT00:00:00')

        if 'workflowId' in event:
            workflow_id = event['workflowId']
            if workflow_id == '':
                workflow_id = None
        else:
            workflow_id = None

        if 'maxItems' in event:
            max_items = int(event['maxItems'])
        else:
            max_items = 1000

        if 'workflowIdToExclude' in event:
            workflow_id_to_exclude = event['workflowIdToExclude']
        else:
            workflow_id_to_exclude = None

        deleted_executions = wfmutils.dynamodb_get_workflow_executions(config, execution_status='DELETED',
                                                                       workflow_id=workflow_id,
                                                                       minimum_create_date_string=minimum_create_date_string,
                                                                       max_items=max_items,
                                                                       workflow_id_to_exclude=workflow_id_to_exclude)

        rejected_executions = wfmutils.dynamodb_get_workflow_executions(config, execution_status='REJECTED',
                                                                        workflow_id=workflow_id,
                                                                        minimum_create_date_string=minimum_create_date_string,
                                                                        max_items=max_items,
                                                                        workflow_id_to_exclude=workflow_id_to_exclude)

        failed_executions = wfmutils.dynamodb_get_workflow_executions(config, execution_status='FAILED',
                                                                      workflow_id=workflow_id,
                                                                      minimum_create_date_string=minimum_create_date_string,
                                                                      max_items=max_items,
                                                                      workflow_id_to_exclude=workflow_id_to_exclude)

        failed_rejected_deleted_executions = failed_executions + rejected_executions + deleted_executions
        logger.info('failed_rejected_deleted_executions count {}'.format(len(failed_rejected_deleted_executions)))
        failed_rejected_deleted_executions_deduplicated = []
        # removed invalidationOffsetSecs
        items_to_keep = ['customerId', 'workflowId', 'timeWindowStart', 'timeWindowEnd', 'timeWindowTimeZone',
                         'timeWindowType',
                         'parameterValues']

        for execution in failed_rejected_deleted_executions:
            new_execution = filter_dictionary_items(execution, items_to_keep)
            if 'timeWindowType' not in new_execution:
                new_execution['timeWindowType'] = 'EXPLICIT'

            if check_if_duplicate(new_execution, failed_rejected_deleted_executions_deduplicated,
                                  items_to_keep) == False:
                failed_rejected_deleted_executions_deduplicated.append(new_execution)

        executions_failed_rejected_deleted_not_yet_succeeded = []
        executions_running_pending_succeeded = []
        if len(failed_rejected_deleted_executions_deduplicated) > 0:
            succeeded_executions = wfmutils.dynamodb_get_workflow_executions(config, execution_status='SUCCEEDED',
                                                                             workflow_id=workflow_id,
                                                                             minimum_create_date_string=minimum_create_date_string,
                                                                             max_items=max_items,
                                                                             workflow_id_to_exclude=workflow_id_to_exclude)
            running_executions = wfmutils.dynamodb_get_workflow_executions(config, execution_status='RUNNING',
                                                                           workflow_id=workflow_id,
                                                                           minimum_create_date_string=minimum_create_date_string,
                                                                           max_items=max_items,
                                                                           workflow_id_to_exclude=workflow_id_to_exclude)
            pending_executions = wfmutils.dynamodb_get_workflow_executions(config, execution_status='PENDING',
                                                                           workflow_id=workflow_id,
                                                                           minimum_create_date_string=minimum_create_date_string,
                                                                           max_items=max_items,
                                                                           workflow_id_to_exclude=workflow_id_to_exclude)
            executions_running_pending_succeeded = succeeded_executions + running_executions + pending_executions

            for execution_to_resubmit_record in failed_rejected_deleted_executions_deduplicated:
                if check_if_duplicate(execution_to_resubmit_record, executions_running_pending_succeeded,
                                      items_to_keep):
                    logger.info('duplicated item in executions_running_pending_succeeded list {}'.format(
                        execution_to_resubmit_record))
                else:
                    executions_failed_rejected_deleted_not_yet_succeeded.append(execution_to_resubmit_record)

    return {
        'failed_rejected_deleted_executions_count ': len(failed_rejected_deleted_executions),
        'failed_rejected_deleted_executions_deduplicated_count': len(failed_rejected_deleted_executions_deduplicated),
        'executions_running_pending_succeeded_count': len(executions_running_pending_succeeded),
        'executions_failed_rejected_deleted_not_yet_succeeded_count': len(
            executions_failed_rejected_deleted_not_yet_succeeded),
        'executions_failed_rejected_deleted_not_yet_succeeded': executions_failed_rejected_deleted_not_yet_succeeded

    }
