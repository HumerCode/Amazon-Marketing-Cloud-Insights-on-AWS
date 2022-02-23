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
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

logger = Logger(service="AddAMCInstance", level="INFO")
logger.info('Get State Machine ARN env variable!')
STATE_MACHINE_ARN = os.environ['STATE_MACHINE_ARN']
def deserializeDyanmoDBItem(item):
    return {k: TypeDeserializer().deserialize(value=v) for k, v in item.items()}

# Serialize JSON object for Decimal
def default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError("Object of type '%s' is not JSON serializable" % type(obj).__name__)

def triggerStateMachine(sqsMessageId,payload):
    stepFunctionClient = boto3.client('stepfunctions')
    logger.info("Statemachine message id: {}".format(sqsMessageId))
    logger.info("State machine inputpayload: {}".format(payload))
    # Start state machine
    response = stepFunctionClient.start_execution(
        stateMachineArn = STATE_MACHINE_ARN,
        name = 'sqsMessage_{}'.format(sqsMessageId),
        input = json.dumps(payload, default=default),
    )
    return response

def lambda_handler(event, context):
    try:
        logger.info('event: {}'.format(json.dumps(event, indent=2)))
        output_payload={}
        sqsMessageId=''
        for sqsrecord in event['Records']:
            logger.info("Record: {} ".format(sqsrecord))
            if 'messageId' in sqsrecord:
                sqsMessageId = sqsrecord['messageId']
                logger.info("sqs messageId: {}".format(sqsMessageId))
            if 'body' in sqsrecord:
                body = sqsrecord['body']
                logger.info("sqs message body: {}".format(body))
                body_imp = json.loads(body)
                logger.info("Type of sqs message body: {}".format(type(body_imp)))
                if 'Message' in body_imp:
                    message = body_imp['Message']
                    logger.info("sqs message: {}".format(message))
                    logger.info("Type of sqs message: {}".format(type(message)))
                    if 'eventName' in json.loads(message):
                        eventName = json.loads(message)['eventName']
                        logger.info("DynamoDB eventName: {}".format(eventName))
                        dynamodb_payload = json.loads(message)['dynamodb']
                        logger.info("dynamodb_payload: {}".format(dynamodb_payload))
                        if 'NewImage' in dynamodb_payload:
                            dynamodb_record = dynamodb_payload['NewImage']
                            logger.info("dynamodb_record: {}".format(dynamodb_record))
                            new_payload = deserializeDyanmoDBItem(dynamodb_record)
                            logger.info("new_payload: {}".format(new_payload))
                            try:
                                output_payload['BucketName'] = new_payload.get("AMC",{}).get("amcS3BucketName",None)
                                output_payload['TenantName'] = new_payload['customerId']
                                output_payload['TenantPrefix'] = new_payload['customerPrefix']
                                output_payload['customerName'] = new_payload['customerName']
                                output_payload['customerType'] = new_payload['endemicType']
                                output_payload['CrossAccountAccessAccountId'] = new_payload.get("AMC",{}).get("amcOrangeAwsAccount",None)
                                output_payload['AmcDatasetName'] = new_payload.get("AMC",{}).get("amcDatasetName",None)
                                output_payload['AmcTeamName'] = new_payload.get("AMC",{}).get("amcTeamName",None)
                                output_payload['amcGreenAwsAccount'] = new_payload.get("AMC",{}).get("amcGreenAwsAccount",None)
                                output_payload['amcRegion'] = new_payload['AMC']['amcRegion']
                                output_payload['amcApiEndpoint'] =  new_payload.get("AMC",{}).get("amcApiEndpoint",None)
                                output_payload['SasDatasetName'] =new_payload.get("SAS",{}).get("sasDatasetName",None)
                                output_payload['SasTeamName'] =new_payload.get("AMC",{}).get("amcTeamName",None)
                                output_payload['SasProfiles'] =new_payload.get("SAS",{}).get("sasProfileDetails",None)
                                output_payload['SasBaseUrl'] =new_payload.get("SAS",{}).get("sasBaseApiUrl",None)
                                output_payload['SasCredArn'] =new_payload.get("SAS",{}).get("sasCredentails",None)
                                response = triggerStateMachine(sqsMessageId, output_payload)
                            except Exception as e1:
                                logger.info("Error in Customer config AMC/SAS details. Check input parameters")
                                response = 'DynamoDB operation is skipped due to faulty input parameters'
                        else:
                            logger.info("Not NewImage!")
                            response = 'DynamoDB operation is not INSERT'
    except ClientError as e:
        logger.info(e.response['Error']['Code'])
        logger.info(e.response['Error']['Message'])
        response = 'FAILED'
    return response