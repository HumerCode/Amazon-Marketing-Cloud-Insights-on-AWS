import json
from dateutil.parser import parse
from datetime import date, timedelta, datetime, timezone
import boto3
import time
import os
from aws_lambda_powertools import Logger
from wfm import wfm_utils

logger = Logger(service="WorkFlowManagement", level="INFO")
wfmutils = wfm_utils.Utils(logger)

athenaclient = boto3.client('athena')


def get_execution_results(QueryExecutionId):
    return athenaclient.get_query_execution(QueryExecutionId=QueryExecutionId)


def lambda_handler(event, context):
    logger.info('event: {}'.format(event))

    if 'customerId' not in event:
        message = 'no customerId found in the request {}'.format(event)
        logger.error(message)
        return {"statusCode": 500, "message": message}

    configs = wfmutils.dynamodb_get_customer_config_records(os.environ['CUSTOMERS_DYNAMODB_TABLE'], event['customerId'])

    if event['customerId'] not in configs:
        message = 'customerId {} not found in customer config records'.format(event['customerId'])
        logger.error(message)
        return {"statusCode": 500, "message": message}
    # create a variable to hold our results
    result = ''

    config = configs[event['customerId']]

    campaigns = []
    messages = []

    maximumCampaignAgeDays = int(config['AMC']['WFM']['runWorkflowByCampaign']['maximumCampaignAgeDays'])
    maximumCampaignEndAgeDays = int(config['AMC']['WFM']['runWorkflowByCampaign']['maximumCampaignEndAgeDays'])
    defaultWorkflowExecutionTimeZone = config['AMC']['WFM']['runWorkflowByCampaign']['defaultWorkflowExecutionTimeZone']
    timeWindowTimeZone = defaultWorkflowExecutionTimeZone

    if 'timeWindowTimeZone' in event:
        logger.info(
            'timeWindowTimeZone from event overriding defaultWorkflowExecutionTimeZone calculated value of {} to {}'.format(
                defaultWorkflowExecutionTimeZone, event['timeWindowTimeZone']))
        timeWindowTimeZone = event['timeWindowTimeZone']

    if 'maximumCampaignAgeDays' in event:
        logger.info('maximumCampaignAgeDays from event overriding customer configuration value of {} to {}'.format(
            maximumCampaignAgeDays, event['maximumCampaignAgeDays']))
        maximumCampaignAgeDays = int(event['maximumCampaignAgeDays'])

    campaignStartDateGreaterThan = (datetime.utcnow() - timedelta(days=maximumCampaignAgeDays)).strftime('%Y-%m-%d')
    logger.info("calculated campaignStartDateGreaterThan date using customer config days {} days to be {}".format(
        maximumCampaignAgeDays, campaignStartDateGreaterThan))

    if 'campaignStartDateGreaterThan' in event:
        logger.info('campaignStartDateGreaterThan from event overriding calculated value of {} to {}'.format(
            campaignStartDateGreaterThan, event['campaignStartDateGreaterThan']))
        campaignStartDateGreaterThan = event['campaignStartDateGreaterThan']

    minimumCampaignAgeDays = int(config['AMC']['WFM']['runWorkflowByCampaign']['minimumCampaignAgeDays'])

    if 'maximumCampaignEndAgeDays' in event:
        logger.info('maximumCampaignEndAgeDays from event overriding customer configuration value of {} to {}'.format(
            maximumCampaignAgeDays, event['maximumCampaignEndAgeDays']))
        maximumCampaignEndAgeDays = int(event['maximumCampaignEndAgeDays'])

    campaignEndDateGreaterThan = (datetime.utcnow() - timedelta(days=maximumCampaignEndAgeDays)).strftime('%Y-%m-%d')

    if 'minimumCampaignAgeDays' in event:
        logger.info('minimumCampaignAgeDays from event overriding default of {} to {}'.format(minimumCampaignAgeDays,
                                                                                              event[
                                                                                                  'minimumCampaignAgeDays']))
        minimumCampaignAgeDays = int(event['minimumCampaignAgeDays'])

    campaignStartDateLessThan = (datetime.utcnow() - timedelta(days=minimumCampaignAgeDays)).strftime('%Y-%m-%d')
    logger.info(
        "campaignStartDateLessThan calculated the minimum date using customer config of {} days to be {}".format(
            minimumCampaignAgeDays, campaignStartDateLessThan))

    if 'campaignStartDateLessThan' in event:
        logger.info('campaignStartDateLessThan from event overriding calculated value of {} to {}'.format(
            campaignStartDateLessThan, event['campaignStartDateLessThan']))
        campaignStartDateLessThan = event['campaignStartDateLessThan']

    campaignListDatabaseName = config['AMC']['WFM']['runWorkflowByCampaign']['campaignListDatabaseName']
    if 'campaignListDatabaseName' in event:
        logger.info('found campaignListDatabaseName in event, overriding customer config of {} with {}'.format(
            campaignListDatabaseName, event['campaignListDatabaseName']))
        campaignListDatabaseName = event['campaignListDatabaseName']

    campaignListTableName = config['AMC']['WFM']['runWorkflowByCampaign']['campaignListTableName']
    if 'campaignListTableName' in event:
        logger.info('found campaignListTableName in event, overriding customer config of {} with {}'.format(
            campaignListTableName, event['campaignListTableName']))
        campaignListTableName = event['campaignListTableName']

    fields = ['campaign_id', 'campaign_start_date', 'campaign_end_date',
              'row_number() over (partition by campaign_id order by file_last_modified desc ) as rownum']

    campaignListQueryString = 'with campaigns as (SELECT {} FROM "{}"."{}" where customer_hash = \'{}\' AND CAST(CAST(campaign_start_date as date) as varchar) > \'{}\' and  CAST(CAST(campaign_start_date as date) as varchar) < \'{}\' )'.format(
        ','.join(fields), campaignListDatabaseName, campaignListTableName, config['customer_hash_key'], campaignStartDateGreaterThan,
        campaignStartDateLessThan)

    campaignQueryString = campaignListQueryString + ' SELECT {} FROM campaigns where rownum = 1'.format(
        ','.join(fields[:-1]))

    if 'additionalWhereClause' in event:
        campaignQueryString += ' AND {}'.format(event['additionalWhereClause'])

    logger.info('Query to get list of campaigns to run the report for is : {}'.format(campaignQueryString))

    query_response = athenaclient.start_query_execution(QueryString=campaignQueryString,
                                                        QueryExecutionContext={
                                                            'Database': campaignListDatabaseName,
                                                        },
                                                        ResultConfiguration={
                                                            'EncryptionConfiguration': {
                                                                'EncryptionOption': 'SSE_S3',
                                                            }
                                                        },
                                                        WorkGroup=os.environ['ATHENA_WORKGROUP']
                                                        )

    timeout = 120
    queryState = ''
    execution_results = None

    while queryState in ('', 'QUEUED', 'RUNNING') and timeout > 0:
        time.sleep(5)
        timeout -= 5
        execution_results = get_execution_results(query_response['QueryExecutionId'])
        queryState = execution_results['QueryExecution']['Status']['State']

    if queryState == 'SUCCEEDED':

        query_results_paginator = athenaclient.get_paginator('get_query_results')
        query_result_iterator = query_results_paginator.paginate(
            QueryExecutionId=query_response['QueryExecutionId'],
            PaginationConfig={

                'PageSize': 100
            }
        )

        totalCampaignRowsReceived = 0

        for page in query_result_iterator:

            rowsInPage = len(page['ResultSet']['Rows']) - 1
            totalCampaignRowsReceived += rowsInPage

            for row in page['ResultSet']['Rows']:
                campaign = {}
                for fieldcounter in range(0, len(row['Data'])):
                    if 'VarCharValue' in row['Data'][fieldcounter]:
                        # check to see if we received a column header, if so skip the row
                        if row['Data'][fieldcounter]['VarCharValue'] == fields[fieldcounter]:
                            continue
                        if fields[fieldcounter] in ('campaign_start_date', 'campaign_end_date'):
                            campaign[fields[fieldcounter]] = row['Data'][fieldcounter]['VarCharValue'].split('.')[0]
                        else:
                            campaign[fields[fieldcounter]] = row['Data'][fieldcounter]['VarCharValue']
                    fieldcounter += 1
                campaigns.append(campaign.copy())

        message = 'received {} rows from get campaign query {} '.format(totalCampaignRowsReceived, campaignQueryString)
        messages.append(message)
        logger.info(messages)

    else:
        message = 'query status {} \n {}'.format(queryState, execution_results)
        logger.info(message)
        messages.append(message)

    campaignAttributionLagDays = int(config['AMC']['WFM']['runWorkflowByCampaign']['campaignAttributionLagDays'])

    if 'campaignAttributionLagDays' in event:
        logger.info('overriding customer config campaignAttributionLagDays value of {} with value {} from event'.format(
            campaignAttributionLagDays, event['campaignAttributionLagDays']))
        campaignAttributionLagDays = int(event['campaignAttributionLagDays'])

    yesterday = datetime.utcnow() - timedelta(days=2)
    maxTimeWindowEnd = datetime(yesterday.year, yesterday.month, yesterday.day, yesterday.hour, 0, 0, 0)

    responses = []
    runWorkflowPayloads = []
    for campaign in campaigns:
        if 'campaign_start_date' in campaign and 'campaign_start_date' in campaign:

            campaignStartDate = parse(campaign['campaign_start_date'])
            # we must convert time windows to the nearest hour otherwise AMC will generate an error
            campaignStartDate = wfmutils.round_time_to_nearest_hour(campaignStartDate)

            timeWindowStart = campaignStartDate.strftime('%Y-%m-%dT%H:%M:%S')

            campaignEndDate = parse(campaign['campaign_end_date'])
            # we must convert time windows to the nearest hour otherwise AMC will generate an error
            campaignEndDate = wfmutils.round_time_to_nearest_hour(campaignEndDate)

            campaignEndWithAttributionLag = campaignEndDate + timedelta(days=campaignAttributionLagDays)

            if campaignEndWithAttributionLag > maxTimeWindowEnd:
                timeWindowEnd = maxTimeWindowEnd.strftime('%Y-%m-%dT%H:%M:%S')
            else:
                timeWindowEnd = campaignEndWithAttributionLag.strftime('%Y-%m-%dT%H:%M:%S')

            # initalize the customer parameters object
            custom_parameters = {}

            # if the payloadid is not at the root level of the event copy it from the payload item
            if 'workflowId' not in event and 'payload' in event and 'workflowId' in event['payload']:
                event['workflowId'] = event['payload']['workflowId']
            # load custom parameters from the event if they exit
            if 'custom_parameters' in event:
                custom_parameters = event['custom_parameters']
            # add the campaign_id as a custom parameter
            custom_parameters["campaign_id"] = campaign['campaign_id']
            logger.info(
                'Added Campaign ID {} to custom paramters {}'.format(campaign['campaign_id'], custom_parameters))
            logger.info('runWorkflowPayloads before add {}'.format(runWorkflowPayloads))
            runWorkflowPayloads.append({
                "workflowId": event['workflowId'],
                "timeWindowStart": timeWindowStart,
                "timeWindowEnd": timeWindowEnd,
                "timeWindowTimeZone": timeWindowTimeZone,
                "timeWindowType": "EXPLICIT",
                "parameterValues": custom_parameters.copy()
            })
            logger.info('runWorkflowPayloads AFTER add {}'.format(runWorkflowPayloads))
    if len(runWorkflowPayloads) > 0:
        runWorkflowEvent = {
            "customerId": config['customerId'],
            "payload": runWorkflowPayloads
        }

        response_payload = queue_workflow_execution(runWorkflowEvent)
        # payload is a streaming body object that should note be returned, if it exists then remove it.
        if 'Payload' in response_payload:
            del response_payload['Payload']
        return ({'runWorkflowEvent': runWorkflowEvent, 'messages': messages,
                 'queueWorkflowExecutionResponse': response_payload})

    return ({'messages': messages})


def queue_workflow_execution(event):
    logger.info(event)
    # create an event that will be sent to the email-s3-file lambda
    client = boto3.client('lambda')

    # invoke the email-s3-file lambda passing the event in the payload
    lambda_invoke_response = client.invoke(
        FunctionName=os.environ['QUEUE_WORKFLOW_EXECUTION_LAMBDA_FUNCTION_NAME'],
        InvocationType='Event',
        Payload=bytes(json.dumps(event), 'utf-8')
    )

    logger.info('response payload: {}'.format(lambda_invoke_response))
    return lambda_invoke_response
