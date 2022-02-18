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