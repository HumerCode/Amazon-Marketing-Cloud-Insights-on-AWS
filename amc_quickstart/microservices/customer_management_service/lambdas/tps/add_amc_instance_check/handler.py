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
import os
from aws_lambda_powertools import Logger


logger = Logger(service="AddAMCInstanceStatusCheck", level="INFO")
cfn = boto3.client('cloudformation')

in_progress_codes = ['CREATE_COMPLETE',
                    'CREATE_IN_PROGRESS',
                    'REVIEW_IN_PROGRESS',
                    'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS',
                    'UPDATE_COMPLETE',
                    'UPDATE_IN_PROGRESS']


def lambda_handler(event, context):
    if (event.get('body',{}).get("stackId", {}).get("StackId", None) != None):
        try:
            logger.info('event: {}'.format(json.dumps(event, indent=2)))
            stack_name = event['body']['stackId']['StackId']

            logger.info('Checking Stack {}'.format(stack_name))
            try:
                logger.info('Getting Stack Creation Status')
                stack_resp = cfn.describe_stacks(
                    StackName=stack_name
                )

                stack_status=stack_resp['Stacks'][0]['StackStatus']
                logger.info('Stack {} state is: {}'.format(stack_name, stack_status))

                if stack_status in in_progress_codes:
                    logger.info('In progress code: {}'.format(stack_status))
                    resp = stack_status
                else:
                    resp = 'FAILED'
            except:
                logger.info('DescribesStacks {} failed'.format(stack_name))
        except:
            logger.info('DescribesStacks {} failed'.format(stack_name))
            resp = 'FAILED'
        return resp
    else:
        logger.info('Nothing to check missing input parameters')
        resp = 'CREATE_COMPLETE'
        return resp