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

import boto3
import json
import os
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError
from decimal import Decimal
from aws_lambda_powertools import Logger
import time
from datetime import date, timedelta, datetime, timezone
import re
import calendar
from dateutil.relativedelta import relativedelta


class Utils:
    def __init__(self, logger):
        self.logger = logger

    def round_time_to_nearest_hour(self, time_to_round):
        self.logger.info('time before round {}'.format(time_to_round))
        rounded_time = time_to_round
        if time_to_round.microsecond != 0:
            self.logger.info('removing microseconds {}'.format(time_to_round.microsecond))
            rounded_time = rounded_time - timedelta(microseconds=rounded_time.microsecond)
            self.logger.info('rounded time after removing microseconds {}'.format(rounded_time))
        if time_to_round.second != 0:
            self.logger.info('removing seconds {}'.format(time_to_round.second))
            rounded_time = rounded_time - timedelta(seconds=rounded_time.second)
            self.logger.info('rounded time after removing seconds {}'.format(rounded_time))
        if time_to_round.minute < 30:
            self.logger.info('removing minutes {}'.format(time_to_round.minute))
            rounded_time = rounded_time - timedelta(minutes=rounded_time.minute)
        if time_to_round.minute >= 30:
            minute_offset = 60 - time_to_round.minute
            self.logger.info('adding minutes {}'.format(minute_offset))
            rounded_time = rounded_time + timedelta(minutes=minute_offset)
        self.logger.info('time after round {}'.format(rounded_time))
        return rounded_time


    def deseralize_dynamodb_item(self, item):
        return {k: TypeDeserializer().deserialize(value=v) for k, v in item.items()}


    def get_cloudformation_rule_names(self, cloudwatch_rule_name_prefix):
        events_client = boto3.client('events')
        list_rules_paginator = events_client.get_paginator('list_rules')
        rule_names = []

        listRulesResponseIterator = list_rules_paginator.paginate(
            NamePrefix=cloudwatch_rule_name_prefix,
            PaginationConfig={
                'PageSize': 100
            }
        )

        # Iterate over each page from the iterator
        for page in listRulesResponseIterator:
            if 'Rules' in page:
                for rule in page['Rules']:
                    rule_names.append(rule['Name'])
        self.logger.info(rule_names)
        return rule_names


    def dynamodb_get_all_records(self, dynamodb_table_name):
        dynamodb_records = []
        # Set up a DynamoDB Connection
        dynamodb = boto3.client('dynamodb')

        # We want to get the whole table for all configs so we pagniate in case there is large number of items
        paginator = dynamodb.get_paginator('scan')
        response_iterator = paginator.paginate(
            TableName=dynamodb_table_name, PaginationConfig={'PageSize': 100}
        )

        # Iterate over each page from the iterator
        for page in response_iterator:
            # deserialize each "item" (or record) into a client config dictionary
            if 'Items' in page:
                for item in page['Items']:
                    dynamodb_item = self.deseralize_dynamodb_item(item)
                    dynamodb_records.append(dynamodb_item.copy())
        return dynamodb_records

    def get_all_dynamodb_table_names(self):
        tables_list = []
        # Set up a DynamoDB Connection
        client = boto3.client('dynamodb')
        done = False
        last_evaluated_table_name = ''
        while not done:
            try:

                if last_evaluated_table_name != '':
                    self.logger.info(tables_list[-1])
                    response = client.list_tables(
                        ExclusiveStartTableName=last_evaluated_table_name,
                        Limit=40
                    )

                else:
                    self.logger.info('first run')
                    response = client.list_tables(
                        Limit=40
                    )

                if 'TableNames' in response and len(response['TableNames']) > 0:
                    tables_list.extend(response['TableNames'])
                else:
                    done = True

                if 'LastEvaluatedTableName' in response:
                    last_evaluated_table_name = response['LastEvaluatedTableName']
                else:
                    done = True

            except:
                done = True

        return tables_list


    def dynamodb_get_customer_config_records(self, dynamodb_table_name, customer_id=None):
        customer_configs = {}
        # Set up a DynamoDB Connection
        dynamodb = boto3.client('dynamodb')

        if customer_id is None:
            # We want to get the whole table for all configs so we pagniate in case there is large number of items
            paginator = dynamodb.get_paginator('scan')
            response_iterator = paginator.paginate(
                TableName=dynamodb_table_name, PaginationConfig={'PageSize': 100}
            )
        else:
            # paginate in case there is large number of items
            paginator = dynamodb.get_paginator('query')
            response_iterator = paginator.paginate(
                TableName=dynamodb_table_name,
                Select='ALL_ATTRIBUTES',
                Limit=1,
                ConsistentRead=False,
                ReturnConsumedCapacity='TOTAL',
                KeyConditions={
                    'customerId': {
                        'AttributeValueList': [
                            {
                                'S': customer_id
                            },
                        ],
                        'ComparisonOperator': 'EQ'
                    }
                }, PaginationConfig={'PageSize': 100})

        # Iterate over each page from the iterator
        for page in response_iterator:
            # deserialize each "item" (or record) into a client config dictionary
            if 'Items' in page:
                for item in page['Items']:
                    customer_config_item = self.deseralize_dynamodb_item(item)
                    # add the client config dictionary to our array
                    customer_configs[customer_config_item['customerId']] = customer_config_item.copy()
        return customer_configs


    def dynamodb_write_records(self, table, items):
        table = boto3.resource('dynamodb').Table(table)
        # self.logger.info('Items to write: {}'.format(items))
        for record in items:
            with table.batch_writer() as batch:
                # self.logger.info('record to update: {}'.format(record))
                batch.dynamodb_put_item(
                    Item=record
                )
        return


    def dynamodb_check_if_item_exists(self, table_name, item):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        key_schema = table.key_schema
        self.logger.info('key schema {}'.format(key_schema))
        key_conditions = {}
        for key in key_schema:
            key_conditions[key['AttributeName']] = {
                "AttributeValueList": [
                    {"S": item[key['AttributeName']]}
                ],
                'ComparisonOperator': 'EQ'
            }
        ddb = boto3.client('dynamodb')
        response = ddb.query(
            TableName=table_name,
            Select='COUNT',
            Limit=1,
            ConsistentRead=False,
            ReturnConsumedCapacity='NONE',
            KeyConditions=key_conditions
        )
        self.logger.info('check if exists response {}'.format(response))

        if response['Count'] > 0:
            return True
        else:
            return False

    def get_offset_value(self, offset_string):
        return int(offset_string.split('(')[1].split(')')[0])

    def get_current_date_with_offset(self, offset_in_days):
        return ((datetime.today() + timedelta(days=offset_in_days)).strftime('%Y-%m-%dT00:00:00'))

    def get_current_date_with_month_offset(self, offset_in_months):
        return (datetime.today() + relativedelta(months=offset_in_months))

    def get_last_day_of_month(self, date):
        return calendar.monthrange(date.year, date.month)[1]

    def process_parameter_functions(self, parameter):
        if parameter.upper() == 'NOW()':
            return datetime.today().strftime('%Y-%m-%dT%H:%M:%S')

        if "TODAY(" in parameter.upper():
            return self.get_current_date_with_offset(self.get_offset_value(parameter))

        if "LASTDAYOFOFFSETMONTH(" in parameter.upper():
            date_with_month_offset = self.get_current_date_with_month_offset(self.get_offset_value(parameter))
            last_day_of_previous_month = self.get_last_day_of_month(date_with_month_offset)
            return_value = datetime(date_with_month_offset.year, date_with_month_offset.month, last_day_of_previous_month,
                                   date_with_month_offset.hour, date_with_month_offset.minute).strftime('%Y-%m-%dT00:00:00')
            return return_value

        if "FIRSTDAYOFOFFSETMONTH(" in parameter.upper():
            date_with_month_offset = self.get_current_date_with_month_offset(self.get_offset_value(parameter))
            return_value = datetime(date_with_month_offset.year, date_with_month_offset.month, 1, date_with_month_offset.hour,
                                   date_with_month_offset.minute).strftime('%Y-%m-%dT00:00:00')
            return return_value

        if "FIFTEENTHDAYOFOFFSETMONTH(" in parameter.upper():
            date_with_month_offset = self.get_current_date_with_month_offset(self.get_offset_value(parameter))
            return_value = datetime(date_with_month_offset.year, date_with_month_offset.month, 15, date_with_month_offset.hour,
                                   date_with_month_offset.minute).strftime('%Y-%m-%dT00:00:00')
            return return_value
        # if no conditions are met, return the parameter unchanged
        return parameter


    def dynamodb_put_item(self, table_name, item):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        self.logger.info('table key schema: {}'.format(table.key_schema))

        response = ''
        try:
            response = table.put_item(Item=item)
            self.logger.info('put_item_response')
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                self.logger.info('successfully put record {}'.format(item))

        except ClientError as e:
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                self.logger.info(e.response['Error']['Message'])
            else:
                self.logger.info(e.response['Error']['Code'])
                self.logger.info(e.response['Error']['Message'])
                raise
        else:
            return response

    def dynamodb_enable_continuous_backups(self, table_name):
        client = boto3.client('dynamodb')
        response = client.update_continuous_backups(
            TableName=table_name,
            PointInTimeRecoverySpecification={
                'PointInTimeRecoveryEnabled': True
            })
        response_code = response['ResponseMetadata']['HTTPStatusCode']

        return {
            'TableName': table_name,
            'ResponseCode': response_code,
            'PointInTimeRecoveryStatus': response['ContinuousBackupsDescription']['PointInTimeRecoveryDescription'][
                'PointInTimeRecoveryStatus']
        }


    def dynamodb_delete_item(self, table_name, item, check_if_item_exists=False):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        key_schema = table.key_schema
        self.logger.info('key schema {}'.format(key_schema))
        item_key = {}
        condition_expressions = []

        for key in key_schema:
            item_key[key['AttributeName']] = item[key['AttributeName']]
            condition_expressions.append('attribute_exists({})'.format(key['AttributeName']))

        condition_expression = ' AND '.join(condition_expressions)

        if check_if_item_exists:
            self.logger.info(
                'attempting delete item from table {} with key{} and conditions {}'.format(table_name, item_key,
                                                                                           condition_expression))
            response = table.delete_item(Key=item_key, ConditionExpression=condition_expression)
        else:
            self.logger.info('attempting delete item from table  with key{}'.format(table_name, item_key))
            response = table.delete_item(Key=item_key)

        self.logger.info('delete item response {}'.format(response))

        return response

    def dynamodb_get_workflow_executions(self, config, *, execution_status=None, workflow_id=None, minimum_create_date_string=None,
                              max_items=None, workflow_id_to_exclude=None):

        # self.logger.info('execution_status:{} workflow_id:{} minimum_create_date_string:{} max_items:{}'.format(execution_status,workflow_id,minimum_create_date_string,max_items))
        # Set up a DynamoDB Connection
        dynamodb = boto3.client('dynamodb')

        # Get the name of the tracking table from an environment variable
        AMC_EXEUCTION_TRACKING_DYNAMODB_TABLE = config['AMC']['WFM']['syncWorkflowStatuses'][
            'amcWorkflowExecutionTrackingDynamoDBTableName']

        workflowExecutions = []

        # Set up a DynamoDB Connection
        dynamodb = boto3.client('dynamodb')

        # We want to get the whole table for all configs so we pagniate in case there is large number of items
        paginator = dynamodb.get_paginator('query')
        query_filter = {}
        key_conditions = {}

        key_conditions['customerId'] = {
            'AttributeValueList': [
                {
                    'S': config['customerId']
                },
            ],
            'ComparisonOperator': 'EQ'
        }

        if execution_status is not None:
            key_conditions['executionStatus'] = {
                'AttributeValueList': [
                    {
                        'S': execution_status
                    }
                ],
                'ComparisonOperator': 'EQ'
            }

        if minimum_create_date_string is not None:
            query_filter['createTime'] = {
                'AttributeValueList': [
                    {'S': minimum_create_date_string}

                ],
                'ComparisonOperator': 'GE'
            }

        if workflow_id is not None:
            if type(workflow_id) == str:
                workflow_id = [workflow_id]
            if type(workflow_id) == list:
                for workflow_id_to_include in workflow_id:
                    query_filter['workflowId'] = {
                        'AttributeValueList': [
                            {'S': workflow_id_to_include}

                        ],
                        'ComparisonOperator': 'EQ'
                    }

        if workflow_id_to_exclude is not None:
            if type(workflow_id_to_exclude) == str:
                workflow_id_to_exclude = [workflow_id_to_exclude]

            if type(workflow_id_to_exclude) == list :
                for workflow_id_item_to_exclude in workflow_id_to_exclude:
                    query_filter['workflowId'] = {
                        'AttributeValueList': [
                            {'S': workflow_id_item_to_exclude}

                        ],
                        'ComparisonOperator': 'NE'
                    }

        pagination_config = {'PageSize': 1000}

        if max_items is not None:
            pagination_config['max_items'] = max_items

        response_iterator = paginator.paginate(
            TableName=AMC_EXEUCTION_TRACKING_DYNAMODB_TABLE,
            IndexName='executionStatus-workflowId-index',
            Select='ALL_PROJECTED_ATTRIBUTES',
            ConsistentRead=False,
            ReturnConsumedCapacity='INDEXES',
            KeyConditions=key_conditions,
            QueryFilter=query_filter,
            ScanIndexForward=False,
            PaginationConfig=pagination_config
        )

        # Iterate over each page from the iterator
        for page in response_iterator:
            # deseralize each "item" (or record) into a client config dictionary
            if 'Items' in page:
                for item in page['Items']:
                    workflowExecution = self.deseralize_dynamodb_item(item)
                    # add the client config dictionary to our array
                    workflowExecutions.append(workflowExecution.copy())
        # self.logger.info('workflowExecutions count: {}'.format(len(workflowExecutions)))
        return (workflowExecutions)


    def json_encoder_default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        raise TypeError("Object of type '%s' is not JSON serializable" % type(obj).__name__)

    def sns_publish_message(self, sns_topic_arn, subject, message):
        client = boto3.client('sns')
        response = client.publish(
            TargetArn=sns_topic_arn,
            Message=json.dumps(message),
            Subject=subject[:100],
             MessageStructure='string'
        )
        return(response)
