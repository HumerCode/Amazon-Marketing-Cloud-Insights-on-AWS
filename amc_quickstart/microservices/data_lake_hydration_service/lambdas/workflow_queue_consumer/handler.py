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
import boto3
import urllib3
import os
from boto3 import Session
from datetime import datetime, timedelta, timezone
from datetime import datetime
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from aws_lambda_powertools import Logger

logger = Logger(service="WorkFlowManagement", level="INFO")

from wfm import wfm_utils

wfmutils = wfm_utils.Utils(logger)


def updateExeuctionTrackingTable(customerConfig, executions):
    table = boto3.resource('dynamodb').Table(
        customerConfig['AMC']['WFM']['syncWorkflowStatuses']['amcWorkflowExecutionTrackingDynamoDBTableName'])
    if type(executions) == dict:
        executions = [executions]

    with table.batch_writer() as batch:
        if type(executions) == list:
            for execution in executions:
                execution['customerId'] = customerConfig['customerId']
                execution['executionStatus'] = execution['status']
                # remove the status attirubte since it was coped to executionStatus as 'status' is a reserved word in dynamodDB and will cause an error
                del execution['status']

                batch.put_item(
                    Item=execution
                )


def getSignedHeaders(request_method, request_url, region, request_body):
    # Generate signed http headers for Sigv4
    request = AWSRequest(method=request_method.upper(), url=request_url, data=request_body)
    SigV4Auth(Session().get_credentials(), "execute-api", region).add_auth(request)
    return dict(request.headers.items())


def getOffsetValue(offset_string):
    return (int(offset_string.split('(')[1].split(')')[0]))


def getCurrentDateWithOffset(offset_in_days):
    return ((datetime.today() + timedelta(days=offset_in_days)).strftime('%Y-%m-%dT00:00:00'))


def processParameterFunctions(parameter):
    if parameter.upper() == 'NOW()':
        return (datetime.today().strftime('%Y-%m-%dT%H:%M:%S'))

    if "TODAY(" in parameter.upper():
        return getCurrentDateWithOffset(getOffsetValue(parameter))
    return parameter


def executeWorkflow(customerConfig, event):
    executedWorkflow = False
    payload = event['payload']
    message = ''

    # Process the parameters to enable now() and today() functions
    if "parameterValues" in payload:
        for parameter in payload['parameterValues']:
            payload['parameterValues'][parameter] = processParameterFunctions(payload['parameterValues'][parameter])
            logger.info("updated parameter {} to {}".format(parameter, payload['parameterValues'][parameter]))

    payload['timeWindowStart'] = processParameterFunctions(payload['timeWindowStart'])
    logger.info("updated parameter timeWindowStart to {}".format(payload['timeWindowStart']))
    payload['timeWindowEnd'] = processParameterFunctions(payload['timeWindowEnd'])
    logger.info("updated parameter timeWindowEnd to {}".format(payload['timeWindowEnd']))

    url = "{}/workflowExecutions".format(customerConfig['AMC']['amcApiEndpoint'])
    request_method = 'POST'
    request_body = json.dumps(payload)
    try:
        AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                         headers=getSignedHeaders(request_method, url,
                                                                                  customerConfig[
                                                                                      'AMC'][
                                                                                      'amcInstanceRegion'],
                                                                                  request_body),
                                                         body=request_body)
        AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

        if (AMC_API_RESPONSE.status == 200):
            executedWorkflow = True
            message = 'Successfully created an exeuction for workflow {} response body: {}'.format(
                payload['workflowId'], AMC_API_RESPONSE_DICTIONARY)
            logger.info(message)

        else:
            executedWorkflow = False
            message = 'Failed to create an exeuction for workflow {} response status: {} response body: {}'.format(
                payload['workflowId'], AMC_API_RESPONSE.status, AMC_API_RESPONSE_DICTIONARY)
            logger.error(message)
        returnValue = {
            'statusCode': AMC_API_RESPONSE.status,
            'message': message,
            'endpointUrl': url,
            'body': AMC_API_RESPONSE_DICTIONARY
        }

    except Exception as ex:
        message = "Error occured when trying to submit workflow execution url: {} error message: {}".format(url, ex)
        logger.error(message)
        executedWorkflow = False
        returnValue = {
            'statusCode': 500,
            'message': message,
            'endpointUrl': url,
            'body': {}
        }

    if not executedWorkflow:
        wfmutils.sns_publish_message(customerConfig['AMC']['WFM']['snsTopicArn'], message, returnValue)

    return returnValue


def get_running_and_pending_executions(customer_config):
    executions_running = 0
    executions_pending = 0
    # Set up a DynamoDB Connection
    dynamodb = boto3.client('dynamodb')
    executionsResponse = dynamodb.query(
        TableName=customer_config['AMC']['WFM']['syncWorkflowStatuses'][
            'amcWorkflowExecutionTrackingDynamoDBTableName'],
        IndexName='executionStatus-workflowId-index',
        Select='SPECIFIC_ATTRIBUTES',
        AttributesToGet=[
            'workflowExecutionId', 'executionStatus', 'customerId', 'lastUpdatedTime', 'createTime', 'workflowId',
            'timeWindowStart', 'timeWindowEnd'
        ],
        ConsistentRead=False,
        ReturnConsumedCapacity='INDEXES',
        KeyConditions={
            'customerId': {
                'AttributeValueList': [
                    {
                        'S': customer_config['customerId']
                    },
                ],
                'ComparisonOperator': 'EQ'
            },

            'executionStatus': {
                'AttributeValueList': [
                    {
                        'S': 'RUNNING'
                    }
                ],
                'ComparisonOperator': 'EQ'
            }
        }
    )

    if executionsResponse['ResponseMetadata']['HTTPStatusCode'] == 200:
        executions_running = []
        for item in executionsResponse['Items']:
            executions_running.append(wfmutils.deseralize_dynamodb_item(item))

    executionsResponse = dynamodb.query(
        TableName=customer_config['AMC']['WFM']['syncWorkflowStatuses'][
            'amcWorkflowExecutionTrackingDynamoDBTableName'],
        IndexName='executionStatus-workflowId-index',
        Select='SPECIFIC_ATTRIBUTES',
        AttributesToGet=[
            'workflowExecutionId', 'executionStatus', 'customerId', 'lastUpdatedTime', 'createTime', 'workflowId',
            'timeWindowStart', 'timeWindowEnd'
        ],
        ConsistentRead=False,
        ReturnConsumedCapacity='INDEXES',
        KeyConditions={
            'customerId': {
                'AttributeValueList': [
                    {
                        'S': customer_config['customerId']
                    },
                ],
                'ComparisonOperator': 'EQ'
            },

            'executionStatus': {
                'AttributeValueList': [
                    {
                        'S': 'PENDING'
                    }
                ],
                'ComparisonOperator': 'EQ'
            }
        }
    )

    if executionsResponse['ResponseMetadata']['HTTPStatusCode'] == 200:
        executions_pending = []
        for item in executionsResponse['Items']:
            executions_pending.append(wfmutils.deseralize_dynamodb_item(item))

    return ({
        "customerId": customer_config['customerId'],
        "executionsRunning": executions_running,
        "executionsPending": executions_pending
    })


def get_number_of_executions_available(customer_config):
    executions_running = 0
    executions_pending = 0
    # Set up a DynamoDB Connection
    dynamodb = boto3.client('dynamodb')
    executionsResponse = dynamodb.query(
        TableName=customer_config['AMC']['WFM']['syncWorkflowStatuses'][
            'amcWorkflowExecutionTrackingDynamoDBTableName'],
        IndexName='executionStatus-workflowId-index',
        Select='COUNT',
        ConsistentRead=False,
        ReturnConsumedCapacity='INDEXES',
        KeyConditions={
            'customerId': {
                'AttributeValueList': [
                    {
                        'S': customer_config['customerId']
                    },
                ],
                'ComparisonOperator': 'EQ'
            },

            'executionStatus': {
                'AttributeValueList': [
                    {
                        'S': 'RUNNING'
                    }
                ],
                'ComparisonOperator': 'EQ'
            }
        }
    )

    if executionsResponse['ResponseMetadata']['HTTPStatusCode'] == 200:
        executions_running = int(executionsResponse['Count'])

    executionsResponse = dynamodb.query(
        TableName=customer_config['AMC']['WFM']['syncWorkflowStatuses'][
            'amcWorkflowExecutionTrackingDynamoDBTableName'],
        IndexName='executionStatus-workflowId-index',
        Select='COUNT',
        ConsistentRead=False,
        ReturnConsumedCapacity='INDEXES',
        KeyConditions={
            'customerId': {
                'AttributeValueList': [
                    {
                        'S': customer_config['customerId']
                    },
                ],
                'ComparisonOperator': 'EQ'
            },

            'executionStatus': {
                'AttributeValueList': [
                    {
                        'S': 'PENDING'
                    }
                ],
                'ComparisonOperator': 'EQ'
            }
        }
    )

    if executionsResponse['ResponseMetadata']['HTTPStatusCode'] == 200:
        executions_pending = int(executionsResponse['Count'])

        executionsAvailable = int(
            customer_config['AMC']['maximumConcurrentWorkflowExecutions']) - executions_pending - executions_running
        logmessage = 'customerId {} Currently has {} RUNNING executions and {} PENDING executions, maximumConcurrentWorkflowExecutions {}, Available executions: {}'.format(
            customer_config['customerId'], executions_running, executions_pending,
            customer_config['AMC']['maximumConcurrentWorkflowExecutions'], executionsAvailable)
        logger.info(logmessage)

    return ({
        "customerId": customer_config['customerId'],
        "executionsRunning": executions_running,
        "executionsPending": executions_pending,
        "executionsAvailable": executionsAvailable
    })


def invoke_consume_queue(customer_config):
    client = boto3.client('lambda')
    event = {
        "method": 'consumequeue',
        "customerId": customer_config['customerId'],
        "customerConfig": customer_config
    }

    logger.info('Invoking function name: {} for customerId{} '.format(os.environ['AWS_LAMBDA_FUNCTION_NAME'],
                                                                      customer_config['customerId']))
    # invoke the email-s3-file lambda passing the event in the payload
    lambda_invoke_response = client.invoke(
        FunctionName=os.environ['AWS_LAMBDA_FUNCTION_NAME'],
        InvocationType='Event',
        Payload=bytes(json.dumps(event, default=wfmutils.json_encoder_default), 'utf-8')
    )

    response = {
        "customerId": customer_config['customerId'],
        "statusCode": lambda_invoke_response['ResponseMetadata']['HTTPStatusCode']
    }

    logger.info('response {}'.format(response))
    return response


def process_queue(customer_config_record):
    log_messages = []
    workflow_execution_responses = []
    workflow_execution_response_codes = [200]
    executions_available = 0
    executions_submitted_count = 0
    executions_submitted = []
    messages_deleted = 0
    messages_received = []

    executions_available_result = get_number_of_executions_available(customer_config_record)

    # Get the queue
    queue = boto3.resource('sqs').get_queue_by_name(
        QueueName=customer_config_record['AMC']['WFM']['amcWorkflowExecutionSQSQueueName'])
    logger.info('customerId: {} executionsAvailable: {}'.format(customer_config_record['customerId'],
                                                                executions_available_result['executionsAvailable']))
    if executions_available_result['executionsAvailable'] > 0:
        messagesToReceive = executions_available_result['executionsAvailable']
        if executions_available_result['executionsAvailable'] > 10:
            messagesToReceive = 10

        messages_received = queue.receive_messages(MessageAttributeNames=['customerId', 'workflowId'],
                                                   MaxNumberOfMessages=messagesToReceive)
        messages_received_count = len(messages_received)
        logger.info('customerId: {} sqs queue: {} messages received: {}'.format(customer_config_record['customerId'],
                                                                                customer_config_record['AMC']['WFM'][
                                                                                    'amcWorkflowExecutionSQSQueueName'],
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
                    'Recevied Run request for customerId: {} workflowId: {} Body: {}'.format(customerId, workflowId,
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

    return ({
        'statusCode': max(workflow_execution_response_codes),
        'customerId': customer_config_record['customerId'],
        'amcApiEndpoint': customer_config_record['AMC']['amcApiEndpoint'],
        'messages': log_messages,
        'executionsAvailable': executions_available_result['executionsAvailable'],
        'executionsRunning': executions_available_result['executionsRunning'],
        'executionsPending': executions_available_result['executionsPending'],
        'messagesReceived': len(messages_received),
        'messagesDeleted': messages_deleted,
        'executionsSubmitted': executions_submitted,
        'executions': workflow_execution_responses

    })


def lambda_handler(event, context):
    logger.info('event received {}'.format(event))
    customers_dynamodb_table_name = os.environ['CUSTOMERS_DYNAMODB_TABLE']

    if 'method' in event:
        if event['method'].lower() == 'getexecutionsavailable':
            if 'customerId' in event:
                get_customer_config_records_results = wfmutils.dynamodb_get_customer_config_records(
                    customers_dynamodb_table_name, event['customerId'])
                customer_config = get_customer_config_records_results[event['customerId']]
                return (get_number_of_executions_available(customer_config))

    if 'method' in event:
        if event['method'].lower() == 'getrunningandpendingexecutions':
            if 'customerId' in event:
                get_customer_config_records_results = wfmutils.dynamodb_get_customer_config_records(
                    customers_dynamodb_table_name, event['customerId'])
                customer_config = get_customer_config_records_results[event['customerId']]
                return (get_running_and_pending_executions(customer_config))

    results = []
    if 'method' in event:
        if event['method'].lower() == 'consumequeue':
            if 'customerId' in event:

                if 'customerConfig' in event:
                    process_queue_results = process_queue(event['customerConfig'])
                    results.append(process_queue_results.copy())
                    logger.info(process_queue_results)
                    return process_queue_results
                else:
                    get_customer_config_records_results = wfmutils.dynamodb_get_customer_config_records(
                        customers_dynamodb_table_name, event['customerId'])
                    customer_config = get_customer_config_records_results[event['customerId']]
                    process_queue_results = process_queue(customer_config)
                    logger.info(process_queue_results)
                    return (process_queue_results)
            else:
                logger.error('A customerId must be specified for method consumeQueue')

    if 'method' in event:
        if event['method'].lower() == 'getallexecutionsavailable':
            results = []
            all_workflow_execution_response_codes = [200]
            logger.info('No method specified, Consuming All queues')
            customer_config_records = wfmutils.dynamodb_get_customer_config_records(customers_dynamodb_table_name)

            for customer_id in customer_config_records:
                results.append(get_number_of_executions_available(customer_config_records[customer_id]))
            return results

    results = []
    all_workflow_execution_response_codes = [200]
    logger.info('No method specified, Consuming All queues')
    customer_config_records = wfmutils.dynamodb_get_customer_config_records(customers_dynamodb_table_name)
    for customer_id in customer_config_records:
        invoke_process_queue_results = invoke_consume_queue(customer_config_records[customer_id])
        results.append(invoke_process_queue_results.copy())
        all_workflow_execution_response_codes.append(invoke_process_queue_results['statusCode'])

    logger.info(results)
    return {
        'statusCode': max(all_workflow_execution_response_codes),
        'responses': results
    }
