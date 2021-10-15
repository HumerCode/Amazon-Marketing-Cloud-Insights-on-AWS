#Customer Config Queue Consumer
import boto3
import json
import os
from aws_lambda_powertools import Logger
from wfm import wfm_utils

logger = Logger(service="WorkFlowManagement", level="INFO")
wfmutils = wfm_utils.Utils(logger)

def parseDynamoDBEvent(dynamoDBEvent):
    DynamoDBRecords = {'eventName' : dynamoDBEvent['eventName'], 'tableName': dynamoDBEvent['eventSourceARN'].split('/')[1]}
    if 'dynamodb' in dynamoDBEvent and 'NewImage' in dynamoDBEvent['dynamodb'] :
        DynamoDBRecords['NewImage'] = wfmutils.deseralize_dynamodb_item(dynamoDBEvent['dynamodb']['NewImage'])
    if 'dynamodb' in dynamoDBEvent and 'OldImage' in dynamoDBEvent['dynamodb'] : 
        DynamoDBRecords['OldImage'] = wfmutils.deseralize_dynamodb_item(dynamoDBEvent['dynamodb']['OldImage'])
    return(DynamoDBRecords)
    
def updateDynamoDBTable(dynamoDBTableName, record):
    table = boto3.resource('dynamodb').Table(dynamoDBTableName)
    eventName = record['eventName']
    
    if eventName in ['INSERT', 'MODIFY'] and 'NewImage' in record :
        recordToUpdate = record['NewImage']
        logger.info('performing {} on table {} for record {}'.format(eventName,dynamoDBTableName, recordToUpdate ))
        response = table.put_item(Item=recordToUpdate)
        return response
        
    if eventName == 'REMOVE' and 'OldImage' in record :
        recordToUpdate = record['OldImage']
        response = table.delete_item(Key={"workflowId": recordToUpdate['workflowId'], "version" :recordToUpdate['version'] })
        
        return response

def lambda_handler(event, context):

    logger.info('raw event:{}'.format(event))
    for snsRecord in event['Records']:
        # Get the custom author message attribute if it was set
        snsRecordBody = json.loads(snsRecord['body'])
 
        sourceSystemEvent = json.loads(snsRecordBody['Message'])
        if sourceSystemEvent['eventSource'] == 'aws:dynamodb' : 
            DynamoDBRecords = parseDynamoDBEvent(sourceSystemEvent)
            logger.info('DynamoDBRecords: {}'.format(DynamoDBRecords))
            updateResponse = updateDynamoDBTable(os.environ['TARGET_DYNAMODB_TABLE'],DynamoDBRecords )
            logger.info('DynamoDBUpdateResponse: {}'.format(updateResponse))
            return(updateResponse)