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
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger
from wfm import wfm_utils

logger = Logger(service="WorkFlowManagement", level="INFO")
wfmutils = wfm_utils.Utils(logger)

client = boto3.client('events')
lambdaClient = boto3.client('lambda')


def get_targets_for_rule(rule):
    paginator = client.get_paginator('list_targets_by_rule')

    response_iterator = paginator.paginate(
        Rule=rule['Name'],
        PaginationConfig={
            'PageSize': 100
        }
    )
    pages = []
    targets = []
    for page in response_iterator:
        pages.append(page)
        logger.info('Rules page {}'.format(page))
        if 'Targets' in page:
            for target in page['Targets']:
                targets.append(target)
    return targets


def get_rules(name_prefix):
    paginator = client.get_paginator('list_rules')
    response_iterator = paginator.paginate(
        NamePrefix=name_prefix,
        PaginationConfig={
            'PageSize': 100
        }
    )
    rules = {}
    for page in response_iterator:
        if 'Rules' in page:
            for rule in page['Rules']:
                rule_name = rule['Name']
                rules[rule_name] = rule
                rules[rule_name]['Targets'] = get_targets_for_rule(rule)
    return rules


def update_rule(rule):
    response = client.put_rule(
        # EventPattern=rule['EventPattern'],
        Name=rule['Name'],
        ScheduleExpression=rule['ScheduleExpression'],

        State=rule['State'],
        Description=rule['Description'],
        Tags=[
            {
                'Key': 'customerId',
                'Value': rule['customerId']
            },
        ],
        EventBusName=rule['EventBusName']

    )
    logger.info(response)
    return response


def delete_rule(rule):
    remove_targets(rule)
    response = client.delete_rule(
        Name=rule['Name'],
        Force=False
    )
    logger.info(response)
    return response


def remove_targets(rule):
    target_for_rule_response = get_targets_for_rule(rule)
    logger.info('target_for_rule_response {}'.format(target_for_rule_response))
    target_ids_to_remove = []
    for target in target_for_rule_response:
        target_ids_to_remove.append(target['Id'])

    if len(target_ids_to_remove) > 0:
        remove_targets_response = client.remove_targets(
            Rule=rule['Name'],
            Ids=target_ids_to_remove,
            Force=True | False
        )
        logger.info('remove_targets_response : {}'.format(remove_targets_response))
    else:
        logger.info('No targets to remove')


def update_target(rule, target):
    response = client.put_targets(
        Rule=rule['Name'],
        Targets=[target]
    )
    return response


def remove_all_lambda_permissions(function_name):
    try:
        get_policy_response = lambdaClient.get_policy(
            FunctionName=function_name
        )

    except lambdaClient.exceptions.ResourceNotFoundException:
        logger.info('policy for function {} does not exist, nothing to remove'.format(function_name))
        return

    logger.info('policy: {}'.format(get_policy_response))
    if get_policy_response['ResponseMetadata']['HTTPStatusCode'] == 200:
        policy = json.loads(get_policy_response['Policy'])
        for statement in policy['Statement']:
            remove_permission_response = lambdaClient.remove_permission(
                FunctionName=function_name,
                StatementId=statement['Sid']
            )
            logger.info('Remove permissions response: {}'.format(remove_permission_response))
            if remove_permission_response['ResponseMetadata']['HTTPStatusCode'] == 204:
                logger.info('Successfully removed existing policy sid {}'.format(statement['Sid']))


def remove_lambda_permissions_if_exist(function_name, statement_id):
    try:
        get_policy_response = lambdaClient.get_policy(
            FunctionName=function_name
        )

    except lambdaClient.exceptions.ResourceNotFoundException:
        logger.info('policy for function {} does not exist, nothing to remove'.format(function_name))
        return

    logger.info('policy: {}'.format(get_policy_response))
    if get_policy_response['ResponseMetadata']['HTTPStatusCode'] == 200:
        policy = json.loads(get_policy_response['Policy'])
        for statement in policy['Statement']:
            if statement['Sid'] == statement_id:
                logger.info('Policy already exists with Sid {}, attempting to remove'.format(statement_id))
                remove_permission_response = lambdaClient.remove_permission(
                    FunctionName=function_name,
                    StatementId=statement_id
                )
                logger.info('Remove permissions response: {}'.format(remove_permission_response))
                if remove_permission_response['ResponseMetadata']['HTTPStatusCode'] == 204:
                    logger.info('Successfully removed existing policy sid {}'.format(statement_id))


def add_lambda_permission(rule_arn, function_name, statement_id):
    try:
        response = lambdaClient.add_permission(
            Action='lambda:InvokeFunction',
            FunctionName=function_name,
            Principal='events.amazonaws.com',
            SourceArn=rule_arn,
            StatementId=statement_id)

        logger.info('Added lamba permissions {}'.format(response))
        return response

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceConflictException':
            logger.info('Unable to add Lambda permissions, resource conflict, likely already exists {}'.format(e))
            return e


def lambda_handler(event, context):
    if 'removeAllPermissions' in event and 'functionName' in event:
        if event['removeAllPermissions']:
            remove_all_lambda_permissions_response = remove_all_lambda_permissions(event['functionName'])
            return remove_all_lambda_permissions_response

    if 'getTargetsForRule' in event and 'rule' in event:
        if event['getTargetsForRule']:
            get_targets_for_rule_response = get_targets_for_rule(event['rule'])
            return get_targets_for_rule_response

    rule_arn = ''
    new_record = ''
    old_record = ''
    logger.info('event: {}'.format(event))
    for record in event['Records']:

        newimgexist =0
        if 'dynamodb' in record and 'NewImage' in record['dynamodb']:
            new_record = wfmutils.deseralize_dynamodb_item(record['dynamodb']['NewImage'])
            newimgexist =1

            if 'EventBusName' not in new_record:
                new_record['EventBusName'] = 'default'
            if 'State' not in new_record:
                new_record['State'] = "ENABLED"
            if 'runBy' not in new_record:
                new_record['runBy'] = ''
            if 'customerId' not in new_record["Input"]:
                new_record["Input"]['customerId'] = new_record['customerId']

        oldimgexist =0
        if 'dynamodb' in record and 'OldImage' in record['dynamodb']:
            old_record = wfmutils.deseralize_dynamodb_item(record['dynamodb']['OldImage'])
            oldimgexist = 1

            if 'customerId' not in old_record["Input"]:
                old_record["Input"]['customerId'] = old_record['customerId']
        
        if ( newimgexist == 1 and 'cron' in new_record["ScheduleExpression"] ):
            if record['eventName'] == 'INSERT':
                logger.info(new_record)
                rule_result = update_rule(new_record)
                rule_arn = rule_result['RuleArn']
    
            if record['eventName'] == 'MODIFY':
                logger.info(new_record)
                rule_result = update_rule(new_record)
                rule_arn = rule_result['RuleArn']
    
            if record['eventName'] == 'REMOVE':
                logger.info('REMOVING : {}'.format(old_record))
                function_name = os.environ['EXECUTION_QUEUE_PRODUCER_LAMBA_ARN'].split(':')[-1]
                if 'runBy' in old_record and old_record['runBy'].lower() == 'campaign':
                    function_name = os.environ['RUN_WORKFLOW_BY_CAMPAIGN_LAMBDA_ARN'].split(':')[-1]
    
                statement_id = '{}'.format(old_record['Name'])
                # remove_lambda_permissions_if_exist(function_name, statement_id)
                delete_rule_result = delete_rule(old_record)
    
            if (record['eventName'] == 'INSERT' or record['eventName'] == 'MODIFY'):
                if new_record['runBy'] == '':
                    logger.info('running individually not by campaign')
                    target = {
                        "Arn": os.environ['EXECUTION_QUEUE_PRODUCER_LAMBA_ARN'],
                        # "Id": new_record['Name'][:100],
                        "Id": "1",
                        "Input": json.dumps(new_record["Input"])
                    }
    
                    function_name = os.environ['EXECUTION_QUEUE_PRODUCER_LAMBA_ARN'].split(':')[-1]
    
                if new_record['runBy'].lower() == 'campaign':
                    logger.info('running by {}'.format(new_record['runBy']))
                    target = {
                        "Arn": os.environ['RUN_WORKFLOW_BY_CAMPAIGN_LAMBDA_ARN'],
                        # "Id": new_record['Name'][:100],
                        "Id": "1",
                        "Input": json.dumps(new_record["Input"])
                    }
                    function_name = os.environ['RUN_WORKFLOW_BY_CAMPAIGN_LAMBDA_ARN'].split(':')[-1]
    
                target_result = update_target(new_record, target)
                logger.info('target update response {}'.format(target_result))
    
                statement_id = '{}'.format(new_record['Name'])
    
                # remove_lambda_permissions_if_exist(function_name, statement_id):q
                # add_lambda_permission(rule_arn, function_name, statement_id)
        
        elif (newimgexist == 1 and 'custom' in new_record["ScheduleExpression"] and oldimgexist == 1 and 'cron' in old_record["ScheduleExpression"]) :
            logger.info('REMOVING : {}'.format(old_record))
            function_name = os.environ['EXECUTION_QUEUE_PRODUCER_LAMBA_ARN'].split(':')[-1]
            if 'runBy' in old_record and old_record['runBy'].lower() == 'campaign':
                function_name = os.environ['RUN_WORKFLOW_BY_CAMPAIGN_LAMBDA_ARN'].split(':')[-1]

            statement_id = '{}'.format(old_record['Name'])
            # remove_lambda_permissions_if_exist(function_name, statement_id)
            delete_rule_result = delete_rule(old_record)
        
        elif (record['eventName'] == 'REMOVE' and oldimgexist == 1 and 'cron' in old_record["ScheduleExpression"]) :
            logger.info('REMOVING : {}'.format(old_record))
            function_name = os.environ['EXECUTION_QUEUE_PRODUCER_LAMBA_ARN'].split(':')[-1]
            if 'runBy' in old_record and old_record['runBy'].lower() == 'campaign':
                function_name = os.environ['RUN_WORKFLOW_BY_CAMPAIGN_LAMBDA_ARN'].split(':')[-1]

            statement_id = '{}'.format(old_record['Name'])
            # remove_lambda_permissions_if_exist(function_name, statement_id)
            delete_rule_result = delete_rule(old_record)
            

    return {'statusCode': 200}
