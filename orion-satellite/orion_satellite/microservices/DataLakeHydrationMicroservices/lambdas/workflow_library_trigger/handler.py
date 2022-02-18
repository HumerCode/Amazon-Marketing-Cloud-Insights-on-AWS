import boto3
import json
import os
from aws_lambda_powertools import Logger
from wfm import wfm_utils

logger = Logger(service="WorkFlowManagement", level="INFO")
wfmutils = wfm_utils.Utils(logger)

sqs = boto3.client('sqs')
ssm = boto3.client('ssm')
ddb = boto3.client('dynamodb')

from wfm import wfm_utils

wfmutils = wfm_utils.Utils(logger)


def handle_updated_item(item, event_name, configs, workflows_table, workflow_schedule_table,
                        cloudwatch_rule_name_prefix):
    # set inital default values for update settings
    automatic_deploy_workflow = False

    # log the even type and the item received
    logger.info('event: {} item {} '.format(event_name, item))

    # check the automatic deploy setting of the workflow
    if 'workflowMetaData' in item and 'automaticDeployWorkflow' in item['workflowMetaData']:
        automatic_deploy_workflow = item['workflowMetaData']['automaticDeployWorkflow']

    # Log if automatic update is enabled for the workflow
    logger.info(
        'automatic_deploy_workflow is {} for the workflow id {} version {}'.format(automatic_deploy_workflow,
                                                                                   item['workflowId'],
                                                                                   item['version']))

    if not automatic_deploy_workflow:
        return

    # get the workflow endemic type from the workflow library record
    workflow_endemic_type = ''
    if 'workflowMetaData' in item and 'endemicType' in item['workflowMetaData']:
        workflow_endemic_type = item['workflowMetaData']['endemicType']

    # get the customer prefix from the workflow library record
    workflow_customer_prefix = ''
    if 'workflowMetaData' in item and 'customerPrefix' in item['workflowMetaData']:
        workflow_customer_prefix = item['workflowMetaData']['customerPrefix']

    for customerId in configs:
        # set the update settings to false by default
        enable_workflow_library_updates = False
        enable_workflow_library_new_content = False
        enable_workflow_library_schedule_creation = False

        # Load the update settings from the customer record if they exist:
        if "enableWorkflowLibraryNewContent" in configs[customerId]['AMC']['WFM']:
            enable_workflow_library_new_content = configs[customerId]['AMC']['WFM']["enableWorkflowLibraryNewContent"]

        if "enableWorkflowLibraryUpdates" in configs[customerId]['AMC']['WFM']:
            enable_workflow_library_updates = configs[customerId]['AMC']['WFM']["enableWorkflowLibraryUpdates"]

        if "enableWorkflowLibraryScheduleCreation" in configs[customerId]['AMC']['WFM']:
            enable_workflow_library_schedule_creation = configs[customerId]['AMC']['WFM'][
                "enableWorkflowLibraryScheduleCreation"]

        # Set the default value for the customer endemic type
        customer_endemic_type = ''

        if "endemicType" in configs[customerId]:
            customer_endemic_type = configs[customerId]["endemicType"]

        logger.info('workflow endemic type {} customer {} endemic type {}'.format(workflow_endemic_type, customerId,
                                                                                  customer_endemic_type))

        # if the endemic type is specified in the workflow and does not match the customer record then skip updating the record for the customer
        if workflow_endemic_type != '' and workflow_endemic_type != customer_endemic_type:
            continue

        # Set the default value for the customer prefix
        customer_customer_prefix = ''

        if "customerPrefix" in configs[customerId]:
            customer_customer_prefix = configs[customerId]["customerPrefix"]

        logger.info(
            'workflow customerPrefix  {} customer {} customerPrefix {}'.format(workflow_customer_prefix, customerId,
                                                                               customer_customer_prefix))

        # if the customer prefix is specified in the workflow and does not match the customer record then skip updating the record for the customer
        if workflow_customer_prefix != '' and workflow_customer_prefix != customer_customer_prefix:
            continue

        workflow_item_to_update = item.copy()
        workflow_item_to_update['customerId'] = customerId
        workflow_item_to_update['workflowId'] = '{}_v{}'.format(workflow_item_to_update['workflowId'],
                                                                workflow_item_to_update['version'])

        workflow_exists = wfmutils.dynamodb_check_if_item_exists(workflows_table, workflow_item_to_update)

        # if the customer config does not allow updates check to see if the workflow exists, if it does then skip
        if not enable_workflow_library_updates and workflow_exists:
            continue
        # if the customer config does not allow new items and the workflow does not already exist then skip
        if not enable_workflow_library_new_content and not workflow_exists:
            continue

        wfmutils.dynamodb_put_item(workflows_table, workflow_item_to_update)
        schedule_item_to_update = item['defaultSchedule'].copy()
        # copy the workflowid into the payload
        schedule_item_to_update['Input']['payload']['workflowId'] = workflow_item_to_update['workflowId']
        schedule_item_to_update['customerId'] = customerId
        schedule_item_to_update['Name'] = '{}-{}-{}'.format(cloudwatch_rule_name_prefix, customerId,
                                                            workflow_item_to_update['workflowId'])
        # only add the cloudformation rule to the list of items to be updated if it does not already exist
        if enable_workflow_library_schedule_creation and not wfmutils.dynamodb_check_if_item_exists(
                workflow_schedule_table, schedule_item_to_update):
            wfmutils.dynamodb_put_item(workflow_schedule_table, schedule_item_to_update)


def handle_deleted_item(item, event_name, configs, workflows_table, workflow_schedule_table,
                        cloudwatch_rule_name_prefix):
    # set inital default values for update settings
    automatic_deploy_workflow = False

    # log the even type and the item received
    logger.info('event: {} item {} '.format(event_name, item))

    # check the automatic deploy setting of the workflow
    if 'workflowMetaData' in item and 'automaticDeployWorkflow' in item['workflowMetaData']:
        automatic_deploy_workflow = item['workflowMetaData']['automaticDeployWorkflow']

    # Log if automatic update is enabled for the workflow
    logger.info(
        'automatic_deploy_workflow is {} for the workflow id {} version {}'.format(automatic_deploy_workflow,
                                                                                   item['workflowId'],
                                                                                   item['version']))

    if not automatic_deploy_workflow:
        return

    for customerId in configs:
        # set the update settings to false by default
        enable_workflow_library_removal = False
        enable_workflow_library_schedule_removal = False

        # Load the update settings from the customer record if they exist:
        if "enableWorkflowLibraryRemoval" in configs[customerId]['AMC']['WFM']:
            enable_workflow_library_removal = configs[customerId]['AMC']['WFM']["enableWorkflowLibraryRemoval"]

        if "enableWorkflowLibraryScheduleRemoval" in configs[customerId]['AMC']['WFM']:
            enable_workflow_library_schedule_removal = configs[customerId]['AMC']['WFM'][
                "enableWorkflowLibraryScheduleRemoval"]

        workflow_item_to_delete = item.copy()
        workflow_item_to_delete['customerId'] = customerId
        workflow_item_to_delete['workflowId'] = '{}_v{}'.format(workflow_item_to_delete['workflowId'],
                                                                workflow_item_to_delete['version'])

        workflow_exists = wfmutils.dynamodb_check_if_item_exists(workflows_table, workflow_item_to_delete)

        # only remove the worklow record if the customer config allows workflow removal and the record exists
        if enable_workflow_library_removal and workflow_exists:
            wfmutils.dynamodb_delete_item(workflows_table, workflow_item_to_delete, True)

        schedule_item_to_delete = item['defaultSchedule'].copy()
        schedule_item_to_delete['customerId'] = customerId
        schedule_item_to_delete['Name'] = '{}-{}-{}'.format(cloudwatch_rule_name_prefix, customerId,
                                                            workflow_item_to_delete['workflowId'])

        schedule_exists = wfmutils.dynamodb_check_if_item_exists(workflow_schedule_table, schedule_item_to_delete)
        # only add the cloudformation rule to the list of items to be updated if it does not already exist
        if enable_workflow_library_schedule_removal and schedule_exists:
            wfmutils.dynamodb_delete_item(workflow_schedule_table, schedule_item_to_delete, False)


def lambda_handler(event, context):
    logger.info('event: {}'.format(event))
    workflows_table = os.environ['WORKFLOWS_TABLE_NAME']
    workflow_schedule_table = os.environ['WORKFLOW_SCHEDULE_TABLE']
    cloudwatch_rule_name_prefix = os.environ['CLOUDWATCH_RULE_NAME_PREFIX']
    workflow_library_table = os.environ['WORKFLOW_LIBRARY_DYNAMODB_TABLE']

    # check to see if the trigger is being invoked for a new customer that needs to have the default workflows deployed
    if 'customerId' in event and 'deployForNewCustomer' in event and event['deployForNewCustomer']:
        # get the customer config record for the specific customer
        customer_config = wfmutils.dynamodb_get_customer_config_records(os.environ['CUSTOMERS_DYNAMODB_TABLE'],
                                                                        event['customerId'])
        # get the entire workflow library record set
        dynamodb_records = wfmutils.dynamodb_get_all_records(workflow_library_table)
        for workflow_library_record in dynamodb_records:
            # filter to only automatic deploy records
            if workflow_library_record['workflowMetaData']['automaticDeployWorkflow']:
                logger.info(workflow_library_record)
                # treat each workflow records with auto deploy as if it were a newly inserted record for this customerId
                handle_updated_item(workflow_library_record, 'INSERT', customer_config, workflows_table,
                                    workflow_schedule_table,
                                    cloudwatch_rule_name_prefix)
        return

    configs = wfmutils.dynamodb_get_customer_config_records(os.environ['CUSTOMERS_DYNAMODB_TABLE'])
    response = {}
    for record in event['Records']:
        # logger.info('dynamoDB Record: {}'.format(record))

        if 'dynamodb' in record and 'NewImage' in record['dynamodb']:
            new_record = wfmutils.deseralize_dynamodb_item(record['dynamodb']['NewImage'])

        if 'dynamodb' in record and 'OldImage' in record['dynamodb']:
            old_record = wfmutils.deseralize_dynamodb_item(record['dynamodb']['OldImage'])

        if record['eventName'] in ['INSERT', 'MODIFY']:
            handle_updated_item(new_record, record['eventName'], configs, workflows_table, workflow_schedule_table,
                                cloudwatch_rule_name_prefix)

        if record['eventName'] in ['REMOVE']:
            handle_deleted_item(old_record, record['eventName'], configs, workflows_table, workflow_schedule_table,
                                cloudwatch_rule_name_prefix)

    return response
