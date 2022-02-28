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
from boto3.dynamodb.conditions import Key, Attr

logger = Logger(service="AddAMCInstancePostDeployMetadata", level="INFO")

dynamodb = boto3.resource('dynamodb')
prefix = os.environ["Prefix"]
env = os.environ["ENV"] 
dataset = os.environ ["Dataset"] 

customer_table = dynamodb.Table('{}-ats-customer-config-{}'.format(prefix, env))
ssm=boto3.client('ssm')


def put_item(table, item, key):
    try:
        # response = table.put_item(
        #     Item=item,
        #     ConditionExpression=f"attribute_not_exists({key})",
        # )
        response = table.put_item(
            Item=item
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            logger.info(e.response['Error']['Message'])
        else:
            raise
    else:
        return response


def lambda_handler(event, context):
    response = None
    try:
        if event['body']['stackStatus'] in ['CREATE_COMPLETE','UPDATE_COMPLETE']:
            logger.info('Status is {}'.format(event['body']['stackStatus']))
            logger.info('Updating Metadata in DDB')

            logger.info('Initializing DynamoDB config and Interface')

            ## Updating SDLF customer config table for AMC datasets
            if (event.get('BucketName',None) != None and event.get('TenantName',None) != None
                    and event.get('AmcDatasetName',None) != None and event.get('TenantPrefix',None) != None
                    and event.get('AmcTeamName',None) != None ):
                logger.info("Updating SDLF customer config table for AMC datasets")
                item = {
                    "hash_key": event['BucketName'],
                    "customer_hash_key": event['TenantName'],
                    "dataset": event['AmcDatasetName'],
                    "prefix": event['TenantPrefix'],
                    "team": event['AmcTeamName']
                }

                response=put_item(customer_table, item, 'customer_hash_key')
            else:
                logger.info("Skipping update to SDLF customer config for AMC. Check input parameters")
                
            ## Updating SDLF customer config table for SAS datasets
            if (event.get('TenantName',None) != None
                    and event.get('SasDatasetName',None) != None and event.get('TenantPrefix',None) != None
                    and event.get('SasTeamName',None) != None ):
                logger.info("Updating SDLF customer config table for SAS datasets")
                
                try:
                    central_bucket = ssm.get_parameter(
                        Name='/SDLF/S3/CentralBucket',
                        WithDecryption=True
                    ).get('Parameter').get('Value')
                
                    item = {
                        "hash_key": (central_bucket + '/customer_id=' + event['TenantName']),
                        "customer_hash_key": event['TenantName'],
                        "dataset": event['SasDatasetName'],
                        "prefix": event['TenantPrefix'],
                        "team": event['SasTeamName']
                    }
                
                    response=put_item(customer_table, item, 'customer_hash_key')
                    
                except Exception as e2:
                    logger.info("Skipping update to SDLF customer config for SAS. Check input parameters")
                    logger.info(str(e2))
            else:
                logger.info("Skipping update to SDLF customer config for SAS. Check input parameters")
            

            ## Update WFM customer config
            if (event.get('amcApiEndpoint', None) != None and event.get('amcGreenAwsAccount', None) != None
                    and event.get('amcRegion', None) != None and event.get('TenantName', None) != None
                    and event.get('AmcTeamName', None) != None
                    and event.get('customerName', None) != None and event.get('TenantPrefix', None) != None
                    and event.get('customerType', None) != None ):
                logger.info ("Updating WFM customer config table")
                item = {
                  "AMC": {
                    "amcAccessCategory": "EXTERNAL",
                    "amcApiEndpoint": event['amcApiEndpoint'],
                    "amcGreenAwsAccount": event['amcGreenAwsAccount'],
                    "amcInstanceRegion": event['amcRegion'],
                    "amcWorkflowPackages": event["TenantName"],
                    "maximumConcurrentWorkflowExecutions": 10,
                    "WFM": {
                      "amcWorkflowExecutionDLQSQSQueueName": f'wfm-{event["AmcTeamName"]}-{env}-workflowExecution-{event["TenantName"]}-DLQ.fifo',
                      "amcWorkflowExecutionSQSQueueName": f'wfm-{event["AmcTeamName"]}-{env}-workflowExecution-{event["TenantName"]}.fifo',
                      "enableWorkflowLibraryNewContent": True,
                      "enableWorkflowLibraryRemoval": True,
                      "enableWorkflowLibraryScheduleCreation": True,
                      "enableWorkflowLibraryScheduleRemoval": True,
                      "enableWorkflowLibraryUpdates": True,
                      "runWorkflowByCampaign": {
                        "campaignAttributionLagDays": 14,
                        "campaignListDatabaseName": f'{event["AmcTeamName"]}_{dataset}_{env}_analytics',
                        "campaignListTableName": f'{event["TenantName"]}_active_campaigns_advertisers_v1_adhoc',
                        "defaultWorkflowExecutionTimeZone": "America/New_York",
                        "maximumCampaignAgeDays": 90,
                        "maximumCampaignEndAgeDays": 18,
                        "minimumCampaignAgeDays": 3
                      },
                      "snsTopicArn": f'arn:aws:sns:{os.environ["Region"]}:{os.environ["AccountId"]}:wfm-{event["AmcTeamName"]}-SNSTopic-{env}',
                      "syncWorkflowStatuses": {
                        "amcWorkflowExecutionTrackingDynamoDBTableName": f'wfm-{event["AmcTeamName"]}-AMCExecutionStatus',
                        "lastSyncedTime": "2021-06-02T15:58:21",
                        "latestLastUpdatedTime": "2021-06-02T15:41:17Z",
                        "workflowExeuctionStatusLookBackHours": 72,
                        "workflowStatusExpirationHours": 72,
                        "workflowStatusExpirationTimeZone": "America/New_York",
                        "WorkflowStatusRecordRetentionDays": 90
                      }
                    }
                  },
                  "customer_hash_key": event['TenantName'],
                  "customerId": event['TenantName'],
                  "customerName": event['customerName'],
                  "customerPrefix": event['TenantPrefix'],
                  "endemicType": event['customerType']
                }

                table_name = dynamodb.Table(f'wfm-{event["AmcTeamName"]}-CustomerConfig-{env}')
                try:
                    response = put_item(table_name, item, 'customer_hash_key')
                except Exception as e:
                    logger.info("Error updating WFM")
                    logger.info(str(e))
            else:
                logger.info ("Skipping update to WFM customer config. Check input parameters")
                
            ## Update SAS customer config
            if (event.get('TenantName', None) != None and event.get('SasCredArn', None) != None
                    and event.get('SasBaseUrl', None) != None and event.get('SasProfiles', None) != None 
                    and event.get('customerName', None) != None ):
                logger.info ("Updating SAS customer config table")
                item = {
                  "customerID": event['TenantName'],
                  "credential_arn": event['SasCredArn'],
                  "baseUrl": event['SasBaseUrl'],
                  "profile_id": event['SasProfiles'],
                  "customerName": event['customerName']
                }

                table_name = dynamodb.Table(f'sas-{event["AmcTeamName"]}-dlhs-CustomerConfig')
                try:
                    response = put_item(table_name, item, 'customer_hash_key')
                except Exception as e:
                    logger.info("Error updating SAS")
                    logger.info(str(e))
            else:
                logger.info ("Skipping update to SAS customer config. Check input parameters")

            return response


        else:
            logger.info('Status is {}'.format(event['body']['stackStatus']))
            logger.info('Skipping Metadata Update in DDB')
            response='Skipping Metadata Update in DDB'
            return response
    except:
        raise