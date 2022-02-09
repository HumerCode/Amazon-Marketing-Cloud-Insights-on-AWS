#!/usr/bin/env python3
# File Name: amc-api-interface.py
# Author: Joshua Witt jwittaws@amazon.com
# Description:
# The Lambda function acts as an interface for interacting with the AMC API

import boto3
import json
from boto3 import Session
import os
import urllib3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse
from dateutil.tz import gettz
from dateutil.relativedelta import relativedelta
import calendar
from aws_lambda_powertools import Logger
from wfm import wfm_utils

logger = Logger(service="WorkFlowManagement", level="INFO")
wfmutils = wfm_utils.Utils(logger)


def sync_workflow_statuses(event):
    # create an event that will be sent to the email-s3-file lambda
    client = boto3.client('lambda')
    logger.info('function name: {}'.format(os.environ['SYNC_WORKFLOW_STATUSES_LAMBDA_FUNCTION_NAME']))
    # invoke the email-s3-file lambda passing the event in the payload
    lambda_invoke_response = client.invoke(
        FunctionName=os.environ['SYNC_WORKFLOW_STATUSES_LAMBDA_FUNCTION_NAME'],
        InvocationType='Event',
        Payload=bytes(json.dumps(event), 'utf-8')
    )

    logger.info('response payload: {}'.format(lambda_invoke_response))
    return lambda_invoke_response

def getNumberOfRunningExecutions(config):
    # Set up a DynamoDB Connection
    dynamodb = boto3.client('dynamodb')

    # Get the name of the tracking table from an environment variable
    AMC_EXEUCTION_TRACKING_DYNAMODB_TABLE = config['AMC']['WFM']['syncWorkflowStatuses'][
        'amcWorkflowExecutionTrackingDynamoDBTableName']

    response = dynamodb.query(
        TableName=AMC_EXEUCTION_TRACKING_DYNAMODB_TABLE,
        IndexName='executionStatus-workflowId-index',
        Select='COUNT',
        Limit=1000,
        ConsistentRead=False,
        ReturnConsumedCapacity='INDEXES',
        KeyConditions={
            'customerId': {
                'AttributeValueList': [
                    {
                        'S': config['customerId']
                    },
                ],
                'ComparisonOperator': 'EQ'
            },

            'executionStatus': {
                'AttributeValueList': [
                    {
                        'S': 'RUNNING'
                    },
                ],
                'ComparisonOperator': 'EQ'
            }
        }
    )
    return (response)


def getExecutionsByStatus(config, executionStatusFilterValue="RUNNING"):
    # Set up a DynamoDB Connection
    dynamodb = boto3.client('dynamodb')

    # Get the name of the tracking table from an environment variable
    AMC_EXEUCTION_TRACKING_DYNAMODB_TABLE = config['AMC']['WFM']['syncWorkflowStatuses'][
        'amcWorkflowExecutionTrackingDynamoDBTableName']
    # Create a table object
    # table = dynamodb.Table(AMC_EXEUCTION_TRACKING_DYNAMODB_TABLE)
    # ProjectionExpression='workflowId,workflowExecutionId,executionStatus',

    workflowExecutions = []
    table = os.environ['CUSTOMERS_DYNAMODB_TABLE']
    # Set up a DynamoDB Connection
    dynamodb = boto3.client('dynamodb')

    # We want to get the whole table for all configs so we pagniate in case there is large number of items
    paginator = dynamodb.get_paginator('query')
    response_iterator = paginator.paginate(
        TableName=AMC_EXEUCTION_TRACKING_DYNAMODB_TABLE,
        IndexName='executionStatus-workflowId-index',
        Select='ALL_PROJECTED_ATTRIBUTES',
        ConsistentRead=False,
        ReturnConsumedCapacity='INDEXES',
        KeyConditions={
            'customerId': {
                'AttributeValueList': [
                    {
                        'S': config['customerId']
                    },
                ],
                'ComparisonOperator': 'EQ'
            },

            'executionStatus': {
                'AttributeValueList': [
                    {
                        'S': executionStatusFilterValue
                    },
                ],
                'ComparisonOperator': 'EQ'
            }
        },

        PaginationConfig={'MaxItems': 1000, 'PageSize': 100, }
    )

    # Iterate over each page from the iterator
    for page in response_iterator:
        # deseralize each "item" (or record) into a client config dictionary
        if 'Items' in page:
            for item in page['Items']:
                workflowExecution = wfmutils.deseralize_dynamodb_item(item)
                # add the client config dictionary to our array
                workflowExecutions.append(workflowExecution.copy())

    return (workflowExecutions)


def getSignedHeaders(config, request_method, request_endpoint_url, request_body):
    # Generate signed http headers for Sigv4
    AWS_REGION = config['AMC']['amcInstanceRegion']
    request = AWSRequest(method=request_method.upper(), url=request_endpoint_url, data=request_body)
    SigV4Auth(Session().get_credentials(), "execute-api", AWS_REGION).add_auth(request)
    return dict(request.headers.items())

def executeWorkflow(config, event):
    payload = event['payload']
    executedWorkflow = False
    logger.info(payload)
    message = ''

    # Process the parameters to enable now() and today() functions
    if "parameterValues" in payload:
        for parameter in payload['parameterValues']:
            payload['parameterValues'][parameter] = wfmutils.process_parameter_functions(payload['parameterValues'][parameter])
            logger.info("updated parameter {} to {}".format(parameter, payload['parameterValues'][parameter]))

    payload['timeWindowStart'] = wfmutils.process_parameter_functions(payload['timeWindowStart'])
    logger.info("updated parameter timeWindowStart to {}".format(payload['timeWindowStart']))
    payload['timeWindowEnd'] = wfmutils.process_parameter_functions(payload['timeWindowEnd'])
    logger.info("updated parameter timeWindowEnd to {}".format(payload['timeWindowEnd']))

    url = "{}/workflowExecutions".format(config['AMC']['amcApiEndpoint'])
    request_method = 'POST'
    request_body = json.dumps(payload)
    AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                     headers=getSignedHeaders(config, request_method, url,
                                                                              request_body), body=request_body)
    AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

    if (AMC_API_RESPONSE.status == 200):
        executedWorkflow = True
        message = 'Successfully created an exeuction for workflow {}'.format(payload['workflowId'])
        logger.info(message)
        update_tracking_table_with_statuses(config, AMC_API_RESPONSE_DICTIONARY)

    else:
        executedWorkflow = False
        message = 'Failed to create an exeuction for workflow {}'.format(payload['workflowId'])
        logger.error(message)
    returnValue = {
        'statusCode': AMC_API_RESPONSE.status,
        'message': message,
        'endpointUrl': url,
        'body': AMC_API_RESPONSE_DICTIONARY
    }

    if not executedWorkflow:
        wfmutils.sns_publish_message(config['AMC']['WFM']['snsTopicArn'], message, returnValue)

    return returnValue


def getWorkflows(config):
    url = "{}/workflows".format(config['AMC']['amcApiEndpoint'])
    message = ''
    request_method = 'GET'
    request_body = ''
    receivedWorkFlows = False
    workflowIdList = []
    AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                     headers=getSignedHeaders(config, request_method, url,
                                                                              request_body), body=request_body)

    AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))
    if AMC_API_RESPONSE.status == 200:

        for index in range(len(AMC_API_RESPONSE_DICTIONARY['workflows'])):
            workflowIdList.append(AMC_API_RESPONSE_DICTIONARY['workflows'][index]['workflowId'])
            receivedWorkFlows = True
        message = 'Successfully received list of workflows'
        logger.info(message)

    else:
        receivedWorkFlows = False
        message = 'Failed to receive list of workflows'
        logger.error(message)

    returnValue = {
        'statusCode': AMC_API_RESPONSE.status,
        'message': message,
        'endpointUrl': config['AMC']['amcApiEndpoint'],
        'workflowIdList': ','.join(workflowIdList),
        'body': AMC_API_RESPONSE_DICTIONARY
    }

    if not receivedWorkFlows:
        wfmutils.sns_publish_message(config['AMC']['WFM']['snsTopicArn'], message, returnValue)
    return returnValue


def getExecutionStatusByWorkflowId(config, workflowId):
    message = []
    receivedWorkflowStatus = False
    request_method = 'GET'
    request_body = ''

    url = "{}/workflowExecutions/?workflowId={}".format(config['AMC']['amcApiEndpoint'], workflowId)

    AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                     headers=getSignedHeaders(config, request_method, url,
                                                                              request_body), body=request_body)
    AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

    if (AMC_API_RESPONSE.status == 200):
        receivedWorkflowStatus = True
        if 'executions' in AMC_API_RESPONSE_DICTIONARY:
            for exeuction in AMC_API_RESPONSE_DICTIONARY['executions']:
                if 'workflowId' in exeuction:
                    update_tracking_table_with_statuses(config , exeuction)
                    #updateResponse = update_tracking_table_with_statuses(config , exeuction)
                    #message.append(updateResponse['message'])
                    #logger.info(message)
    else:
        receivedWorkflowStatus = False
        message = 'Failed to receive workflow status for workflow {}'.format(workflowId)
        logger.error(message)

    returnValue = {
        'statusCode': AMC_API_RESPONSE.status,
        'endpointUrl': url,
        'message': message,
        'body': AMC_API_RESPONSE_DICTIONARY
    }

    if not receivedWorkflowStatus:
        wfmutils.sns_publish_message(config, message[-1], returnValue)
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
    AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

    if (AMC_API_RESPONSE.status == 200):
        receivedExecutionStatus = True
        if 'workflowId' in AMC_API_RESPONSE_DICTIONARY:
            message.append(
                "Received Execution Status for Workflow Execution ID {}, updating dynamoDB Tracking Table".format(
                    workflowExecutionId))
            update_tracking_table_with_statuses(config, AMC_API_RESPONSE_DICTIONARY)
            logger.info(message)

    else:
        receivedExecutionStatus = False
        message.append("Failed to Receive Execution Status for Workflow Execution ID {}".format(workflowExecutionId))
        logger.error(message)

    returnValue = {
        'statusCode': AMC_API_RESPONSE.status,
        'endpointUrl': url,
        'message': message,
        'body': AMC_API_RESPONSE_DICTIONARY
    }

    if not receivedExecutionStatus:
        wfmutils.sns_publish_message(config['AMC']['WFM']['snsTopicArn'], ' '.join(message), returnValue)

    return returnValue


def createWorkflow(config, payload):
    url = "{}/workflows".format(config['AMC']['amcApiEndpoint'])

    workflowCreated = False
    message = ''
    request_method = 'POST'
    request_body = json.dumps(payload)
    AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                     headers=getSignedHeaders(config, request_method, url,
                                                                              request_body), body=request_body)
    AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

    if (AMC_API_RESPONSE.status == 200):
        workflowCreated = True
        message = 'Successfully created workflow {}'.format(payload['workflowId'])
        logger.info(message)
    else:
        workflowCreated = False
        message = 'Failed to create workflow {}'.format(payload['workflowId'])
        logger.error(message)
        if 'message' in AMC_API_RESPONSE_DICTIONARY and AMC_API_RESPONSE_DICTIONARY[
            "message"] == "Workflow with ID {} already exists.".format(payload['workflowId']):
            # Insert failed due to workflow already exiting, running an update instead
            logger.info('Insert failed bacause the workflowId {} already exists, submitting an update request'.format(
                payload['workflowId']))
            updateResponse = updateWorkflow(config, payload)
            return (updateResponse)

    returnValue = {
        'statusCode': AMC_API_RESPONSE.status,
        'message': message,
        'endpointUrl': url,
        'body': AMC_API_RESPONSE_DICTIONARY
    }

    if not workflowCreated:
        wfmutils.sns_publish_message(config['AMC']['WFM']['snsTopicArn'], message, returnValue)

    return returnValue


def updateWorkflow(config, payload):
    workflowUpdated = False
    message = ''

    url = "{}/workflows/{}".format(config['AMC']['amcApiEndpoint'], payload['workflowId'])
    request_method = 'PUT'
    request_body = json.dumps(payload)
    AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                     headers=getSignedHeaders(config, request_method, url,
                                                                              request_body), body=request_body)
    AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

    if (AMC_API_RESPONSE.status == 200):
        workflowUpdated = True
        message = 'Successfully updated workflow {}'.format(payload['workflowId'])
        logger.info(message)
    else:
        workflowUpdated = False
        message = 'Failed to update workflow {}'.format(payload['workflowId'])
        logger.error(message)

    returnValue = {
        'statusCode': AMC_API_RESPONSE.status,
        'message': message,
        'endpointUrl': url,
        'body': AMC_API_RESPONSE_DICTIONARY
    }

    if not workflowUpdated:
        wfmutils.sns_publish_message(config['AMC']['WFM']['snsTopicArn'], message, returnValue)

    return returnValue


def deleteWorkflow(config, payload):
    workflowDeleted = False
    message = ''
    url = "{}/workflows/{}".format(config['AMC']['amcApiEndpoint'], payload['workflowId'])
    request_method = 'DELETE'
    request_body = json.dumps(payload)
    AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                     headers=getSignedHeaders(config, request_method, url,
                                                                              request_body), body=request_body)
    AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

    logger.info('Workflow delete response {}'.format(AMC_API_RESPONSE))
    if (AMC_API_RESPONSE.status == 200):
        workflowDeleted = True
        message = 'Successfully deleted workflow {} for customerId {}'.format(payload['workflowId'], config['customerId'])
        logger.info(message)
    else:
        workflowDeleted = False
        message = 'Failed to delete workflow {}'.format(payload['workflowId'])
        logger.error(message)

    returnValue = {
        'statusCode': AMC_API_RESPONSE.status,
        'message': message,
        'endpointUrl': url,
        'body': AMC_API_RESPONSE_DICTIONARY
    }

    if not workflowDeleted:
        wfmutils.sns_publish_message(config['AMC']['WFM']['snsTopicArn'], message, returnValue)

    return returnValue


def delete_schedules(config, payload):
    return_values = []
    for schedule_id in payload['scheduleIds']:

        schedule_deleted = False
        message = ''
        url = "{}/schedules/{}".format(config['AMC']['amcApiEndpoint'], schedule_id)
        request_method = 'DELETE'
        request_body = json.dumps(payload)
        AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                         headers=getSignedHeaders(config, request_method, url,
                                                                                  request_body), body=request_body)
        AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

        logger.info('schedule delete response {}'.format(AMC_API_RESPONSE))
        if (AMC_API_RESPONSE.status == 200):
            schedule_deleted = True
            message = 'Successfully deleted schedule {} for customerId {}'.format(schedule_id, config['customerId'])
            logger.info(message)
        else:
            schedule_deleted = False
            message = 'Failed to delete schedule {} for customerId {}'.format(schedule_id, config['customerId'])
            logger.error(message)

        returnValue = {
            'customerId': config['customerId'],
            'statusCode': AMC_API_RESPONSE.status,
            'message': message,
            'endpointUrl': url,
            'body': AMC_API_RESPONSE_DICTIONARY
        }

        if not schedule_deleted:
            wfmutils.sns_publish_message(config['AMC']['WFM']['snsTopicArn'], message, returnValue)
        return_values.append(returnValue.copy())
    return return_values


def get_schedules(config):
    message = ''
    url = "{}/schedules".format(config['AMC']['amcApiEndpoint'])
    request_method = 'GET'
    request_body = ''
    logger.info('get workflow request URL: {}'.format(url))
    AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                     headers=getSignedHeaders(config, request_method, url,
                                                                              request_body), body=request_body)
    logger.info('response data: {}'.format(AMC_API_RESPONSE.data))
    AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

    logger.info('get schedules response {}'.format(AMC_API_RESPONSE))
    if (AMC_API_RESPONSE.status == 200):
        schedules_received = True
        message = 'Successfully received schedules for customerId {}'.format(config['customerId'])
        logger.info(message)
    else:
        schedules_received = False
        message = 'Failed to get schedules for customerId {}'.format(config['customerId'])
        logger.error(message)

    returnValue = {
        'customerId': config['customerId'],
        'statusCode': AMC_API_RESPONSE.status,
        'message': message,
        'endpointUrl': url,
        'body': AMC_API_RESPONSE_DICTIONARY
    }

    if not schedules_received:
        wfmutils.sns_publish_message(config['AMC']['WFM']['snsTopicArn'], message, returnValue)

    return returnValue


def get_workflow(config, payload):
    message = ''
    url = "{}/workflows/{}".format(config['AMC']['amcApiEndpoint'], payload['workflowId'])
    request_method = 'GET'
    request_body = ''
    logger.info('get workflow request URL: {}'.format(url))
    AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                     headers=getSignedHeaders(config, request_method, url,
                                                                              request_body), body=request_body)
    logger.info('response data: {}'.format(AMC_API_RESPONSE.data))
    AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

    logger.info('get workflow response {}'.format(AMC_API_RESPONSE))
    if (AMC_API_RESPONSE.status == 200):
        workflowreceived = True
        message = 'Successfully recevied workflow {}'.format(payload['workflowId'])
        logger.info(message)
    else:
        workflowreceived = False
        message = 'Failed to get workflow {}'.format(payload['workflowId'])
        logger.error(message)

    returnValue = {
        'statusCode': AMC_API_RESPONSE.status,
        'message': message,
        'endpointUrl': url,
        'body': AMC_API_RESPONSE_DICTIONARY
    }

    if not workflowreceived:
        wfmutils.sns_publish_message(config['AMC']['WFM']['snsTopicArn'], message, returnValue)

    return returnValue


def deleteWorkflowExecution(config, payload):
    workflowExecutionDeleted = False
    message = ''
    url = "{}/workflowExecutions/{}".format(config['AMC']['amcApiEndpoint'], payload['workflowExecutionId'])

    request_method = 'DELETE'
    request_body = json.dumps(payload)
    AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                     headers=getSignedHeaders(config, request_method, url,
                                                                              request_body), body=request_body)
    AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

    logger.info(
        'Workflow delete response code: {} response : {}'.format(AMC_API_RESPONSE.status, AMC_API_RESPONSE_DICTIONARY))
    if (AMC_API_RESPONSE.status == 200):
        workflowExecutionDeleted = True
        message = 'Successfully deleted workflow execution {} for customerId {}'.format(payload['workflowExecutionId'], config['customerId'])
        logger.info(message)
    else:
        workflowExecutionDeleted = False
        message = 'Failed to delete workflow {}'.format(payload['workflowExecutionId'])
        logger.error(message)

    returnValue = {
        'statusCode': AMC_API_RESPONSE.status,
        'customerId' : config['customerId'],
        'message': message,
        'endpointUrl': url,
        'body': AMC_API_RESPONSE_DICTIONARY
    }

    if not workflowExecutionDeleted:
        wfmutils.sns_publish_message(config['AMC']['WFM']['snsTopicArn'], message, returnValue)

    return returnValue


def update_tracking_table_with_statuses(config, statuses):
    # Get the name of the tracking table from an environment variable
    AMC_EXEUCTION_TRACKING_DYNAMODB_TABLE = config['AMC']['WFM']['syncWorkflowStatuses'][
        'amcWorkflowExecutionTrackingDynamoDBTableName']
    # Create a table object
    table = boto3.resource('dynamodb').Table(AMC_EXEUCTION_TRACKING_DYNAMODB_TABLE)

    if type(statuses) != list:
        statuses = [statuses]

    with table.batch_writer() as batch:
        for record in statuses:
            if 'outputS3URI' not in record:
                record['outputS3URI'] = ''
            record['executionStatus'] = record['status']
            # remove the status item as it will cause a DynamoDB error as it is a reserved word
            del record['status']
            record['customerId'] = config['customerId']
            record['expireTimestamp'] = round((parse(record['lastUpdatedTime']) + timedelta(days=int(
                config['AMC']['WFM']['syncWorkflowStatuses']['WorkflowStatusRecordRetentionDays']))).timestamp())

            logger.info('record to update workflowExecutionId:{} workflowId:{}'.format(record['workflowExecutionId'],
                                                                                       record['workflowId']))
            batch.put_item(
                Item=record
            )


def lambda_handler(event, context):
    configs = wfmutils.dynamodb_get_customer_config_records(os.environ['CUSTOMERS_DYNAMODB_TABLE'])

    if 'customerId' not in event and event['method'] != 'syncExecutionStatuses':
        message = 'no customerId found in the request {}'.format(event)
        logger.error(message)
        return {"statusCode": 500, "message": message}

    if 'customerId' in event and event['customerId'] not in configs:
        message = 'customerId {} not found in customer config records'.format(event['customerId'])
        logger.error(message)
        return {"statusCode": 500, "message": message}
    # create a variable to hold our results
    result = ''

    if 'customerId' in event:
        config = configs[event['customerId']]

    # determine which function to execute based upon the method passed in the event
    if (event['method'] == 'createWorkflow'):
        result = createWorkflow(configs[event['customerId']], event['payload'])

    elif (event['method'] == 'deleteWorkflow'):
        result = deleteWorkflow(configs[event['customerId']], event['payload'])

    elif (event['method'] == 'executeWorkflow'):
        result = executeWorkflow(configs[event['customerId']], event)

    elif (event['method'] == 'deleteWorkflowExecution'):
        if 'workflowExecutionId' in event['payload']:
            if type(event['payload']['workflowExecutionId']) == list :
                result = []
                for executionId in event['payload']['workflowExecutionId']:
                    result.append(deleteWorkflowExecution(configs[event['customerId']], {"workflowExecutionId": executionId}))
            else:
                result = deleteWorkflowExecution(configs[event['customerId']], event['payload'])

    elif (event['method'] == 'deleteWorkflowExecutions'):
        result = []
        for executionId in event['payload']['workflowExecutionIds']:
            result.append(deleteWorkflowExecution(configs[event['customerId']], {"workflowExecutionId": executionId}))

    elif (event['method'] == 'getWorkflows'):
        result = getWorkflows(configs[event['customerId']])

    elif (event['method'] == 'getWorkflow'):
        result = get_workflow(configs[event['customerId']], event['payload'])

    elif (event['method'] == 'syncExecutionStatuses'):
        responses = {}
        for configkey in configs:
            logger.info('getting status for customerId:{}'.format(configkey))

            invokeLambdaResponse = sync_workflow_statuses({'customerId': configkey})
            responses[configkey] = invokeLambdaResponse['ResponseMetadata']['HTTPStatusCode']
        return (responses)

    elif (event['method'] == 'getExecutionStatus'):
        if 'workflowId' in event:
            if isinstance(event['workflowId'], (list)):
                result = []
                messages = []
                executions = []
                statusCode = []

                for workflowId in event['workflowId']:
                    current_result = (getExecutionStatusByWorkflowId(configs[event['customerId']], workflowId))
                    messages += current_result['message']
                    executions += current_result['body']['executions']
                    statusCode.append(current_result['statusCode'])
                body = {"executions": executions}
                result = {
                    "statusCode": max(statusCode),
                    "endpointUrl": config['AMC']['amcApiEndpoint'],
                    "message": messages,
                    "body": body

                }

            if isinstance(event['workflowId'], (str)):
                result = getExecutionStatusByWorkflowId(configs[event['customerId']], event['workflowId'])

        elif 'workflowExecutionId' in event:
            result = getExecutionStatusByWorkFlowExecutionId(configs[event['customerId']], event['workflowExecutionId'])

    elif (event['method'] == 'updateWorkflow'):
        result = updateWorkflow(configs[event['customerId']], event['payload'])

    elif (event['method'] == 'getNumberOfRunningExecutions'):
        result = getNumberOfRunningExecutions(configs[event['customerId']])

    elif (event['method'] == 'getRunningExecutions'):
        result = getExecutionsByStatus(configs[event['customerId']], "RUNNING")

    elif (event['method'] == 'getExecutionsByStatus'):
        result = getExecutionsByStatus(configs[event['customerId']], event['executionStatus'])

    elif (event['method'] == 'getSchedules'):
        result = get_schedules(configs[event['customerId']])

    elif (event['method'] == 'deleteSchedules'):
        result = delete_schedules(configs[event['customerId']], event['payload'])

    else:
        message = 'invalid method {}'.format(event['method'])
        logger.error(message)
        returnValue = {
            'statusCode': 500,
            'customerId': config['customerId'],
            'endpointUrl': config['AMC']['amcApiEndpoint'],
            'message': message
        }

        wfmutils.sns_publish_message(config['AMC']['WFM']['snsTopicArn'], message, returnValue)
        return returnValue

    logger.info(result)
    return (result)