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
from botocore.exceptions import ClientError


logger = Logger(service="AddAMCInstance", level="INFO")

cfn = boto3.client('cloudformation')
s3 = boto3.client("s3")

template_url = os.environ['templateUrl']
prefix = os.environ['Prefix']

def parse_params(event):
    template_params = [
        {
            'ParameterKey': 'pBucketName',
            'ParameterValue': event['BucketName']
        },
        {
            'ParameterKey': 'pCrossAccountAccessAccountId',
            'ParameterValue': event['CrossAccountAccessAccountId']
        },
        {
            'ParameterKey': 'pTenantName',
            'ParameterValue': event['TenantName']
        },
        {
            'ParameterKey': 'pLambdaRoleArn',
            'ParameterValue': os.environ['lambdaRoleArn']
        },
        {
            'ParameterKey': 'pResourcePrefix',
            'ParameterValue': prefix
        }
    ]
    return template_params


def launch_stack(event):
    if (event.get('TenantName') != None and event.get('BucketName') and event.get('CrossAccountAccessAccountId')):
        stack_name = '{}-{}-instance-{}'.format(prefix, event["AmcDatasetName"], event['TenantName'])

        try:
            logger.info('Parsing Event Message for Parameters')
            template_params = parse_params(event)
            logger.info('Creating stack {} with pBucketName: {} and pCrossAccountAccessAccountId: {}'.format(stack_name,event['BucketName'],event['CrossAccountAccessAccountId']))
            stack_resp = None
            try:
                logger.info('Checking stack exists')
                stack_resp_desc = cfn.describe_stacks(StackName=stack_name)
                # print (stack_resp_desc)
                logger.info('Update stack')
                stack_resp = cfn.update_stack(
                                                StackName=stack_name,
                                                TemplateURL=template_url,
                                                Parameters=template_params
                                            )
                # print ("Resp update : " + str(stack_resp))
                # if 'No updates are to be performed' in stack_resp:
                #     stack_resp = stack_resp_desc
            except Exception as e:
                # print ("Error")
                # print (stack_resp)
                # print (str(e))
                if ('No updates are to be performed' in str(e)):
                    stack_resp = {"StackId" : stack_resp_desc["Stacks"][0]["StackId"] }
                    logger.info("Error : " + str(e))
                else:
                    logger.info('Stack does not exist. Creating stack')
                    stack_resp = cfn.create_stack(
                                                    StackName=stack_name,
                                                    TemplateURL=template_url,
                                                    Parameters=template_params
                                                )
                    # print (stack_resp)
        except Exception as e:
            error_msg = str(e)
            logger.error(error_msg)
            stack_resp = {"error": error_msg}
        return stack_resp
    else:
        logger.info('Nothing to create/update. Missing input parameters')
        return 'no activity'


def lambda_handler(event, context):
    logger.info('event: {}'.format(json.dumps(event, indent=2)))
    stack_resp = launch_stack(event)
    
    return stack_resp

