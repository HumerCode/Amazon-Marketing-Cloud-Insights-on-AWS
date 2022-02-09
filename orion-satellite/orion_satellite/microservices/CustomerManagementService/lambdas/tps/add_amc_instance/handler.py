import json
import boto3
import os
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError


logger = Logger(service="AddAMCInstance", level="INFO")

cfn = boto3.client('cloudformation')
s3 = boto3.client("s3")

template_url = os.environ['templateUrl']

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
        }
    ]
    return template_params


def launch_stack(event):
    if (event.get('TenantName') != None and event.get('BucketName') and event.get('CrossAccountAccessAccountId')):
        stack_name = 'orion-amcdataset-instance-{}'.format(event['TenantName'])

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

