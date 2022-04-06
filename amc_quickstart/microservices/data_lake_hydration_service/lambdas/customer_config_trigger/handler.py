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

import boto3
import json
import os
import re
from aws_lambda_powertools import Logger
from wfm import wfm_utils

logger = Logger(service="WorkFlowManagement", level="INFO")
wfmutils = wfm_utils.Utils(logger)


sqs = boto3.client('sqs')
ssm = boto3.client('ssm')
ddb = boto3.client('dynamodb')


# checks to see if the IAM policy to allow invoking AMC Endpoints contains the endpoint for the record that was modified.
def check_iam_policy(config):
    logger.info('inside check iam policy function')
    amc_endpoint_iam_policy_arn = os.environ['AMC_ENDPOINT_IAM_POLICY_ARN']

    endpoint_id = re.match('https://([^.]*).*', config['AMC']['amcApiEndpoint']).groups()[0]
    endpoint_resource = 'arn:aws:execute-api:{}:*:{}/*'.format(config['AMC']['amcInstanceRegion'], endpoint_id)

    client = boto3.client('iam')

    get_policy_versions_paginator = client.get_paginator('list_policy_versions')

    get_policy_versions_response_iterator = get_policy_versions_paginator.paginate(
        PolicyArn=amc_endpoint_iam_policy_arn,
        PaginationConfig={
            'PageSize': 10
        }
    )
    policy_versions = []
    for policyVersionsResponsePage in get_policy_versions_response_iterator:
        for policy_version in policyVersionsResponsePage['Versions']:
            policy_versions.append(policy_version)
            if policy_version['IsDefaultVersion'] == True:
                default_policy_version_id = VersionId = policy_version['VersionId']
                get_policy_version_response = client.get_policy_version(
                    PolicyArn=amc_endpoint_iam_policy_arn,
                    VersionId=policy_version['VersionId']
                )
    if 'ResponseMetadata' in get_policy_version_response and 'HTTPStatusCode' in get_policy_version_response[
        'ResponseMetadata'] and get_policy_version_response['ResponseMetadata'][
        'HTTPStatusCode'] == 200 and 'PolicyVersion' in get_policy_version_response:
        logger.info(
            'Received Default Policy, Document: {}'.format(get_policy_version_response['PolicyVersion']['Document']))
        for statement in get_policy_version_response['PolicyVersion']['Document']['Statement']:
            if 'execute-api:Invoke' in statement['Action']:
                logger.info('Found invoke statment, resources: {}'.format(statement['Resource']))
                if endpoint_resource in statement['Resource']:
                    return ()
                else:
                    logger.info('endpoint resource {} NOT found in statement'.format(endpoint_resource))
                    statement['Resource'] = list([statement['Resource']]) if type(statement['Resource']) == str else statement['Resource']
                    statement['Resource'].append(endpoint_resource)

                    if len(policy_versions) > 4:
                        logger.info(
                            '{} policy versions already exists, maximum policy version count of 5 exceeded will delete the oldest policies above 5'.format(
                                len(policy_versions)))
                        for policyToDelete in policy_versions[4:]:
                            logger.info("policy to delete: {}".format(policyToDelete))
                            delete_policy_version = client.delete_policy_version(
                                PolicyArn=amc_endpoint_iam_policy_arn,
                                VersionId=policyToDelete['VersionId']
                            )
                            logger.info('Delete policy request response {}'.format(delete_policy_version))

                    create_policy_version_response = client.create_policy_version(
                        PolicyArn=amc_endpoint_iam_policy_arn,
                        PolicyDocument=json.dumps(get_policy_version_response['PolicyVersion']['Document']),
                        SetAsDefault=True
                    )
                    logger.info('Create Policy Version Response: {}'.format(create_policy_version_response))


def get_sqs_queue_url(queue_name):
    logger.info('checking if queue exists: {}'.format(queue_name))
    try:
        response = sqs.get_queue_url(
            QueueName=queue_name
        )

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return response['QueueUrl']
    except Exception as e:
        logger.info(e)
        return ''


def get_sqs_queue_attributes(queue_url):
    logger.info('gettting attiributes for queue {}'.format(queue_url))
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                'All'
            ]
        )
        logger.info('queue url: {} attributes response recevied : {}'.format(queue_url, response))
        return response

    except Exception as e:
        logger.info(e)
        return ''



def set_sqs_queue_attributes(queue_url, queue_attributes):
    logger.info('trying to update queue url {} with attributes {}'.format(queue_url, queue_attributes))
    try:
        response = sqs.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes=queue_attributes
        )
        logger.info('queue url {} update response {}'.format(queue_url, response))
        return response

    except Exception as e:
        logger.info(e)
        return ''



def check_sqs_queues(customer_id, item):
    team = os.environ['TEAM']
    microservice = os.environ['MICROSERVICE']
    env = os.environ['ENV']

    responses = []

    # get the execution SQS queue name from the customer config
    queue_name = item['AMC']['WFM']['amcWorkflowExecutionSQSQueueName']

    # get the execution SQS Dead letter queue from th customer config, if it does not exist generate a name
    if 'WFM' in item['AMC'] and 'amcWorkflowExecutionDLQSQSQueueName' in item['AMC']['WFM']:
        dead_letter_queue_name = item['AMC']['WFM']['amcWorkflowExecutionDLQSQSQueueName']
    else:
        matches = re.match('(.*)(\.fifo)', queue_name)
        queue_base_name = matches.groups()[0]
        dead_letter_queue_name = '{}-DLQ.fifo'.format(queue_base_name)

    # get the SQS execution queue url (also a way to check if it exists)
    sqs_queue_url = get_sqs_queue_url(queue_name)

    # get the SQS execution dead letter queue url (also a way to check if it exists)
    sqs_dead_letter_queue_url = get_sqs_queue_url(dead_letter_queue_name)

    # generate the tag
    tag = "{}-{}-{}".format(microservice, team, env)

    # if the dead letter queue url is empty then it does not exist and should be created.
    if sqs_dead_letter_queue_url == '':
        create_sqs_dead_letter_queue_response =  create_sqs_queue(dead_letter_queue_name, tag)
        responses.append(create_sqs_dead_letter_queue_response)
        logger.info('create_dead_letter_queue_response {}'.format(create_sqs_dead_letter_queue_response))
    # get the queue url after the SQS execution dead letter queue is created so that we can get the details
    sqs_dead_letter_queue_url = get_sqs_queue_url(dead_letter_queue_name)
    # get the queue attributes for the execution dead letter queue
    sqs_dead_letter_queue_attributes = get_sqs_queue_attributes(sqs_dead_letter_queue_url)
    logger.info('sqs_dead_letter_queue_attributes: {}'.format(sqs_dead_letter_queue_attributes))
    # get the ARN of the dead letter queue so we can use it to create a redrive policy for the execution queue
    sqs_dead_letter_queue_arn = sqs_dead_letter_queue_attributes['Attributes']['QueueArn']

    # create a redrive policy object
    redrive_policy = {
        "deadLetterTargetArn": sqs_dead_letter_queue_arn,
        "maxReceiveCount": 5
    }

    # if we were able to get the queue url then the execution SQS queue does exist, check it's attributes
    if sqs_queue_url != '':
        sqs_queue_attributes = get_sqs_queue_attributes(sqs_queue_url)
        if sqs_queue_attributes != '':
            # get the ARN of the queue so we can update the queue if necessary
            sqs_queue_arn = sqs_queue_attributes['Attributes']['QueueArn']
            # check to see if the redrive policy exists and has the dead letter queue arn specified
            if 'RedrivePolicy' not in sqs_queue_attributes['Attributes'] or json.loads(sqs_queue_attributes['Attributes']['RedrivePolicy'])['deadLetterTargetArn'] != sqs_dead_letter_queue_arn:
                set_sqs_queue_attributes_response = set_sqs_queue_attributes(sqs_queue_url, {"RedrivePolicy": json.dumps(redrive_policy)})
                responses.append(set_sqs_queue_attributes_response)

    ##if the queue does not exist then create it
    if sqs_queue_url == '':
        create_sqs_dead_letter_queue_response = create_sqs_queue(queue_name, tag, json.dumps(redrive_policy))
        responses.append(create_sqs_dead_letter_queue_response)

    return responses


def create_sqs_queue(queue_name, tag, redrive_policy=''):
    logger.info('creating queue: {}'.format(queue_name))

    queue_attributes = {
        "FifoQueue": "true",
        "ContentBasedDeduplication": "true",
        "KmsMasterKeyId": os.environ['KMS_MASTER_KEY'],
        "VisibilityTimeout": "30",
        "DelaySeconds": "10"
    }

    if redrive_policy != '':
        queue_attributes['RedrivePolicy'] = redrive_policy

    response = sqs.create_queue(
        QueueName=queue_name,
        Attributes=queue_attributes,
        tags={
            'tagging-policy': tag
        }

    )

    logger.info('response payload: {}'.format(response))
    return response


def lambda_handler(event, context):


    logger.info('event received {}'.format(event))
    response = {}
    for record in event['Records']:
        logger.info('dynamoDB Record: {}'.format(record))

        if 'dynamodb' in record and 'NewImage' in record['dynamodb']:
            new_record = wfmutils.deseralize_dynamodb_item(record['dynamodb']['NewImage'])
            item = new_record


        if record['eventName'] == 'INSERT':
            logger.info('record inserted, checking IAM policy')
            response = check_iam_policy(item)

            if 'WFM' in item['AMC'] and 'amcWorkflowExecutionSQSQueueName' in item['AMC']['WFM']:
                response = check_sqs_queues(new_record['customerId'], item)

                # Deploy the workflow library workflows for the new customer:
                client = boto3.client('lambda')
                workflow_library_event = {'customerId': item['customerId'], 'deployForNewCustomer': True}
                lambda_invoke_response = client.invoke(
                    FunctionName=os.environ['WORKFLOW_LIBRARY_TRIGGER_LAMBDA_FUNCTION_NAME'],
                    InvocationType='Event',
                    Payload=bytes(json.dumps(workflow_library_event, default=wfmutils.json_encoder_default),
                                  'utf-8')
                )
                logger.info('Deploy workflows for new customer invoke response: {}'.format(lambda_invoke_response))

        if record['eventName'] == 'MODIFY':
            logger.info('record modified, going to check iam policy')
            response = check_iam_policy(item)
            if 'WFM' in item['AMC'] and 'amcWorkflowExecutionSQSQueueName' in item['AMC']['WFM']:
                response = check_sqs_queues(new_record['customerId'], item)

    return response
