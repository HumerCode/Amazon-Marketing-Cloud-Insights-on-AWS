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
# File Name: sync-workflow-statuses.py
# Author: Joshua Witt jwittaws@amazon.com
# Description:
# The Lambda function will synchronize the Workflow Status DynamoDB Table with the AMC API

import boto3
import json
from boto3 import Session
import os
import urllib3
from urllib.parse import urlparse, urlencode, parse_qs, quote
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse
from aws_lambda_powertools import Logger
from wfm import wfm_utils
import math
import time

logger = Logger(service="WorkFlowManagement", level="INFO")
wfmutils = wfm_utils.Utils(logger)


def getSignedHeaders(config, request_method, request_endpoint_url, request_body):
    # Generate signed http headers for Sigv4
    AWS_REGION = config['AMC']['amcInstanceRegion']
    request = AWSRequest(method=request_method.upper(), url=request_endpoint_url, data=request_body)
    SigV4Auth(Session().get_credentials(), "execute-api", AWS_REGION).add_auth(request)
    return dict(request.headers.items())


def getExecutionStatusesByMinCreationTime(config, minCreationTime):
    receivedExecutionStatus = True
    message = []
    request_method = 'GET'
    request_body = ''

    executions = []
    statuses = {}
    AMC_API_RESPONSE_DICTIONARY = {}
    AMC_API_RESPONSE_DICTIONARY['nextToken'] = ''

    # Loop over responses if there is a nextToken in the response to get all executions
    while 'nextToken' in AMC_API_RESPONSE_DICTIONARY and receivedExecutionStatus:
        receivedExecutionStatus = False
        url = "{}/workflowExecutions/?{}".format(config['AMC']['amcApiEndpoint'], urlencode(
            {'minCreationTime': minCreationTime, "nextToken": AMC_API_RESPONSE_DICTIONARY['nextToken']}))
        AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                         headers=getSignedHeaders(config, request_method, url,
                                                                                  request_body), body=request_body)
        AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))
        statuses[url] = AMC_API_RESPONSE.status

        if (AMC_API_RESPONSE.status == 200 and 'executions' in AMC_API_RESPONSE_DICTIONARY):
            receivedExecutionStatus = True
            executions += AMC_API_RESPONSE_DICTIONARY['executions'].copy()
        else:
            receivedExecutionStatus = False
            message.append("Failed to Receive Execution Status: {} {}".format(AMC_API_RESPONSE.status,
                                                                              AMC_API_RESPONSE_DICTIONARY))
            logger.error(message)

    returnValue = {
        'statusCode': max(statuses.values()),
        'statuses': statuses,
        'executions': executions
    }

    if not receivedExecutionStatus:
        wfmutils.sns_publish_message(config, ' '.join(message), returnValue)

    return returnValue


def getExecutionStatusByWorkFlowExecutionId(config, workflowExecutionId):
    receivedExecutionStatus = False
    message = []
    request_method = 'GET'
    request_body = ''
    url = "{}/workflowExecutions/{}".format(config['AMC']['amcApiEndpoint'], workflowExecutionId)
    AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                     headers=getSignedHeaders(config, request_method, url,
                                                                              request_body), body=request_body)
    workflow_status_response = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

    if (AMC_API_RESPONSE.status == 200):
        receivedExecutionStatus = True
        # if 'workflowId' in workflow_status_response:
        #    message.append(
        #        "Received Execution Status for Workflow Execution ID {}, updating dynamoDB Tracking Table".format(
        #            workflowExecutionId))
        #    update_tracking_table_with_statuses(config, workflow_status_response.copy())
        #    logger.info(message)

    else:
        receivedExecutionStatus = False
        message.append("Failed to Receive Execution Status for Workflow Execution ID {}".format(workflowExecutionId))
        logger.error(message)

    returnValue = {
        'statusCode': AMC_API_RESPONSE.status,
        'endpointUrl': url,
        'message': message,
        'body': workflow_status_response
    }

    if not receivedExecutionStatus:
        wfmutils.sns_publish_message(config, ' '.join(message), returnValue)

    return returnValue


def update_tracking_table_with_statuses(config, execution_statuses):
    status_update_record_batch_size = int(
        config['AMC']['WFM']['syncWorkflowStatuses']['WorkflowStatusRecordUpdateBatchSize'])
    status_update_batch_delay_seconds = int(
        config['AMC']['WFM']['syncWorkflowStatuses']['WorkflowStatusRecordUpdateBatchDelaySeconds'])

    if type(execution_statuses) != list:
        execution_statuses = [execution_statuses]

    # Get the name of the tracking table from an environment variable
    # Create a table object
    table = boto3.resource('dynamodb').Table(
        config['AMC']['WFM']['syncWorkflowStatuses']['amcWorkflowExecutionTrackingDynamoDBTableName'])
    messages = []

    records_to_write = len(execution_statuses)
    message = 'records to write {}'.format(records_to_write)
    logger.info(message)
    messages.append(message)
    batches_to_run = math.ceil(records_to_write / status_update_record_batch_size)
    batch_counter = 1
    last_index = 0
    results = []
    number_of_records_updated = 0
    status_codes = [200]
    last_updated_times = ['']
    while batch_counter <= batches_to_run:
        if batch_counter > 1:
            time.sleep(status_update_batch_delay_seconds)

        logger.info(
            'last index{} {} , batch_counter: {} {}, status_update_record_batch_size: {} {}'.format(type(last_index),
                                                                                                    last_index,
                                                                                                    type(batch_counter),
                                                                                                    batch_counter, type(
                    status_update_record_batch_size), status_update_record_batch_size))
        statuses = execution_statuses[last_index: (batch_counter * status_update_record_batch_size)]
        last_index = batch_counter * status_update_record_batch_size
        message = 'batch_counter {}, lenth of batch {}, last_index: {}'.format(batch_counter, len(statuses), last_index)
        logger.info(message)
        messages.append(message)
        statusCode = 200
        try:
            with table.batch_writer() as batch:
                for record in statuses:
                    if 'outputS3URI' not in record:
                        record['outputS3URI'] = ''
                    last_updated_times.append(record['lastUpdatedTime'])
                    record['executionStatus'] = record['status']
                    # remove the status item as it will cause a DynamoDB error as it is a reserved word
                    del record['status']
                    record['customerId'] = config['customerId']
                    record['expireTimestamp'] = round((parse(record['lastUpdatedTime']) + timedelta(days=int(
                        config['AMC']['WFM']['syncWorkflowStatuses'][
                            'WorkflowStatusRecordRetentionDays']))).timestamp())
                    batch.put_item(Item=record)
            message = 'put {} records for table {}'.format(len(statuses), config['AMC']['WFM']['syncWorkflowStatuses'][
                'amcWorkflowExecutionTrackingDynamoDBTableName'])
            logger.info(message)
            results.append(
                {'statusCode': 200, 'batchNumber': batch_counter, 'batchSize': len(statuses), 'message': message})
            number_of_records_updated = number_of_records_updated + len(statuses)
        except Exception as e:
            message = 'Updating record batch number {} batch size {} record {} failed with error {}'.format(
                batch_counter, len(statuses), json.dumps(record, default=wfmutils.json_encoder_default), e)
            logger.error(message)
            results.append(
                {'statusCode': 500, 'batchNumber': batch_counter, 'batchSize': len(statuses), 'message': message})
            status_codes.append(500)

        batch_counter = batch_counter + 1
    return {
        'statusCode': max(status_codes),
        'totalRecordsUpdated': number_of_records_updated,
        'latestLastUpdatedTime': max(last_updated_times),
        'results': results
    }


def lambda_handler(event, context):
    default_batch_size = int(os.environ['DEFAULT_DYNAMODB_RECORD_UPDATE_BATCH_SIZE'])
    default_batch_delay_seconds = int(os.environ['DEFAULT_DYNAMODB_BATCH_DELAY_SECONDS'])

    if 'customerId' not in event:
        message = 'no customerId found in the request {}'.format(event)
        logger.error(message)
        return {"statusCode": 500, "message": message}

    configs = wfmutils.dynamodb_get_customer_config_records(os.environ['CUSTOMERS_DYNAMODB_TABLE'], event['customerId'])

    for configKey in configs:
        config = configs[configKey]

        if 'WorkflowStatusRecordUpdateBatchSize' not in config['AMC']['WFM']['syncWorkflowStatuses']:
            config['AMC']['WFM']['syncWorkflowStatuses']['WorkflowStatusRecordUpdateBatchSize'] = default_batch_size
        if 'WorkflowStatusRecordUpdateBatchDelaySeconds' not in config['AMC']['WFM']['syncWorkflowStatuses']:
            config['AMC']['WFM']['syncWorkflowStatuses'][
                'WorkflowStatusRecordUpdateBatchDelaySeconds'] = default_batch_delay_seconds
        return (sync_workflow_statuses(config))


def update_last_synced_time_customer_record_Dynamodb(table_name, config, last_synced_time, latest_last_updated_time=''):
    table = boto3.resource('dynamodb').Table(table_name)
    if latest_last_updated_time != '':
        response = table.update_item(
            Key={
                'customerId': config['customerId']
            },
            UpdateExpression="set AMC.WFM.syncWorkflowStatuses.lastSyncedTime=:t, AMC.WFM.syncWorkflowStatuses.latestLastUpdatedTime=:u",
            ExpressionAttributeValues={
                ':t': last_synced_time,
                ':u': latest_last_updated_time
            },
            ReturnValues="UPDATED_NEW"
        )

    else:
        response = table.update_item(
            Key={
                'customerId': config['customerId']
            },
            UpdateExpression="set AMC.WFM.syncWorkflowStatuses.lastSyncedTime=:t",
            ExpressionAttributeValues={
                ':t': last_synced_time
            },
            ReturnValues="UPDATED_NEW"
        )
    return response


def sync_workflow_statuses(config):
    logger.info('Syncing Status Table for customerId:{}'.format(config['customerId']))

    # create a dictionary for execution records so we can do a quick lookup later based on workflowExecutionId as the key for each record
    executionRecordsDictionary = {}

    try:
        lookbackHours = (-1 * int(config['AMC']['WFM']['syncWorkflowStatuses']['workflowExeuctionStatusLookBackHours']))
    except:
        lookbackHours = -72

    # calculate the minimum execution created date
    minimum_create_date_string = (datetime.today() + timedelta(hours=lookbackHours)).strftime('%Y-%m-%dT00:00:00')
    # get a date with UTC by adding Z to the parsed string
    minimum_create_date = parse(minimum_create_date_string + "Z")
    outdated_executions = []
    updates_for_outdated_executions = []
    running_executions = wfmutils.dynamodb_get_workflow_executions(config, execution_status="RUNNING")
    pending_executions = wfmutils.dynamodb_get_workflow_executions(config, execution_status="PENDING")

    running_and_pending_executions = running_executions + pending_executions
    for execution in running_and_pending_executions:
        if 'createTime' in execution:
            logger.info(
                'createTime: {} minimum_create_date:{} '.format(parse(execution['createTime']), minimum_create_date))
            if parse(execution['createTime']) < minimum_create_date:
                outdated_executions.append(execution)
                execution_status = getExecutionStatusByWorkFlowExecutionId(config, execution['workflowExecutionId'])
                logger.info(execution_status['body'])
                updates_for_outdated_executions.append(execution_status['body'])

    # get execution records from DynamoDB
    # execution_records_from_time_window =  getDynamoDBExecutionRecordsByminimum_create_date_string(config,minimum_create_date_string)

    execution_records_from_time_window = wfmutils.dynamodb_get_workflow_executions(config,
                                                                                   minimum_create_date_string=minimum_create_date_string)

    executionRecords = outdated_executions + execution_records_from_time_window

    logger.info('{} exeuction Records returned from DynamoDB'.format(len(executionRecords)))

    # fill the new dictionary with the values from the array
    for executionRecord in executionRecords:
        executionRecordsDictionary[executionRecord['workflowExecutionId']] = executionRecord

    # Get the current list of executions from the AMC API
    getExecutionStatusesResponse = getExecutionStatusesByMinCreationTime(config, minimum_create_date_string)
    if 'executions' in getExecutionStatusesResponse:
        executions = getExecutionStatusesResponse['executions'] + updates_for_outdated_executions
        logger.info('{} exeuctions returned from AMC API'.format(len(executions)))

    # create a dictionary for response records so we can do a quick lookup later based on workflowExecutionId as the key for each record
    executionResponsesDictionary = {}

    # Create arrays to hold records that need to be inserted or updated
    executionsToUpdate = []
    executionsToInsert = []

    for execution in executions:
        # fill the new dictionary
        executionResponsesDictionary[execution['workflowExecutionId']] = execution
        # Check to see if the record key exists in our records dictionary (if the record was in the results from DynamoDB)
        if execution['workflowExecutionId'] in executionRecordsDictionary.keys():
            # If the record was in DynamoDB with an older lastupdated time then add the response from AMC to our executions to update array
            if execution['lastUpdatedTime'] > executionRecordsDictionary[execution['workflowExecutionId']][
                'lastUpdatedTime']:
                executionsToUpdate.append(execution.copy())
        else:
            # If the record was not in our dictionary of DyanmoDB responses then add it to our records to insert array
            executionsToInsert.append(execution.copy())

    # Create an array for records that need to be marked as deleted
    executionsToMarkAsDeleted = []

    # iterate over the execution records that we received from DyanmoDB to find records that were not returned from the AMC API (because they were deleted)
    for executionRecord in executionRecords:
        # Skip over any records in our Dictionary that are already marked as Deleted We should exclude these from the original request to DynamoDB but
        # the executionstatus field is a key field for a GSI (and we are querying the table) so we can't filter on it, we are querying on the main table not the GSI
        # so we can't use it in the key conditions either to filter them out)
        # status in the records gets moved to executionStatus by the update method because status is a reserved word in Dynamodb and cannot be an attribute name
        if executionRecord['workflowExecutionId'] not in executionResponsesDictionary.keys() and executionRecord[
            'executionStatus'] != 'DELETED':
            updatedexecutionRecord = executionRecord.copy()
            updatedexecutionRecord['status'] = 'DELETED'
            executionsToMarkAsDeleted.append(updatedexecutionRecord)

    AllRecordsToUpdate = executionsToUpdate + executionsToInsert + executionsToMarkAsDeleted

    update_results = update_tracking_table_with_statuses(config, AllRecordsToUpdate)

    if update_results['statusCode'] == 200:
        update_last_synced_time_response = update_last_synced_time_customer_record_Dynamodb(
            os.environ['CUSTOMERS_DYNAMODB_TABLE'], config, datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
            update_results['latestLastUpdatedTime'])

        return_object = {
            'customerId': config['customerId'],
            'statusCode': update_results['statusCode'],
            'latestLastUpdatedTime': update_results['latestLastUpdatedTime'],
            'allexecutionsToModify': len(executionsToUpdate),
            'allexecutionsToInsert': len(executionsToInsert),
            'allexecutionsToMarkDeleted': len(executionsToMarkAsDeleted),
            'totalRecordsToUpdate': len(AllRecordsToUpdate),
            'totalRecordsUpdated': update_results['totalRecordsUpdated'],
            'lookbackHours': lookbackHours,
            'oldestCreateDateMonitored': minimum_create_date_string,
            'runningOrPendingExecutionsOutsideMonitoringWindow': len(outdated_executions),
            'updatesForExecutionsOutsideMonitoringWindow': updates_for_outdated_executions,
            "recordsUpdated": AllRecordsToUpdate,
            'updateResults': update_results,
            'updateLastSyncedTimeResponse': update_last_synced_time_response
        }

    logger.info(json.dumps(return_object, default=wfmutils.json_encoder_default))

    return (return_object)
