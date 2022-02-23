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
# Author: Joshua Witt @jwittaws
# Description: Allows the AMC API to be invoked to create, update, delete and execute workflows
# Updated: 4-7-2021

import json
import calendar
from dateutil.relativedelta import relativedelta
import logging
from boto3 import Session
import urllib3
from urllib.parse import urlencode
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from datetime import datetime, timedelta

# This class will create HTTP Request for the AMC API Endpoint
class AMCAPIInterface:
    def __init__(self, logger, config):
        self.logger = logging
        self.config = config

    # gets the value between parenteses as an integer
    def get_offset_value(self, offset_string):
        return int(offset_string.split('(')[1].split(')')[0])

    # gets the full date based on an offset from the current date(positive or negative)
    def get_current_date_with_offset(self, offset_in_days):
        return ((datetime.today() + timedelta(days=offset_in_days)).strftime('%Y-%m-%dT00:00:00'))

    # gets the curent date based on an offset in number of months (positive or negative)
    def get_current_date_with_month_offset(self, offset_in_months):
        return (datetime.today() + relativedelta(months=offset_in_months))

    # gets the last day of a month
    def get_last_day_of_month(self, date):
        return calendar.monthrange(date.year, date.month)[1]

    # Detects if a function is being used in place of a staitc date for timewindowstart or timewindow end
    # and generates the date value as a string in a formation that AMC will accept
    def process_parameter_functions(self, parameter):
        if parameter.upper() == 'NOW()':
            return datetime.today().strftime('%Y-%m-%dT%H:%M:%S')

        if "TODAY(" in parameter.upper():
            return self.get_current_date_with_offset(self.get_offset_value(parameter))

        if "LASTDAYOFOFFSETMONTH(" in parameter.upper():
            date_with_month_offset = self.get_current_date_with_month_offset(self.get_offset_value(parameter))
            last_day_of_previous_month = self.get_last_day_of_month(date_with_month_offset)
            return_value = datetime(date_with_month_offset.year, date_with_month_offset.month,
                                    last_day_of_previous_month,
                                    date_with_month_offset.hour, date_with_month_offset.minute).strftime(
                '%Y-%m-%dT00:00:00')
            return return_value

        if "FIRSTDAYOFOFFSETMONTH(" in parameter.upper():
            date_with_month_offset = self.get_current_date_with_month_offset(self.get_offset_value(parameter))
            return_value = datetime(date_with_month_offset.year, date_with_month_offset.month, 1,
                                    date_with_month_offset.hour,
                                    date_with_month_offset.minute).strftime('%Y-%m-%dT00:00:00')
            return return_value

        if "FIFTEENTHDAYOFOFFSETMONTH(" in parameter.upper():
            date_with_month_offset = self.get_current_date_with_month_offset(self.get_offset_value(parameter))
            return_value = datetime(date_with_month_offset.year, date_with_month_offset.month, 15,
                                    date_with_month_offset.hour,
                                    date_with_month_offset.minute).strftime('%Y-%m-%dT00:00:00')
            return return_value
        # if no conditions are met, return the parameter unchanged
        return parameter

    # rounds a time to the nearest hour, this is used when running an execution based on a campaign's start or end
    # dates, if a campaing's start or end time was not ending with 00 minutes, the date must be rounded to the
    # nearest full hour to prevent errors when submitting timewindow value to AMC
    def round_time_to_nearest_hour(self, time_to_round):
        logging.info('time before round {}'.format(time_to_round))
        rounded_time = time_to_round
        if time_to_round.microsecond != 0:
            logging.info('removing microseconds {}'.format(time_to_round.microsecond))
            rounded_time = rounded_time - timedelta(microseconds=rounded_time.microsecond)
            logging.info('rounded time after removing microseconds {}'.format(rounded_time))
        if time_to_round.second != 0:
            logging.info('removing seconds {}'.format(time_to_round.second))
            rounded_time = rounded_time - timedelta(seconds=rounded_time.second)
            logging.info('rounded time after removing seconds {}'.format(rounded_time))
        if time_to_round.minute < 30:
            logging.info('removing minutes {}'.format(time_to_round.minute))
            rounded_time = rounded_time - timedelta(minutes=rounded_time.minute)
        if time_to_round.minute >= 30:
            minute_offset = 60 - time_to_round.minute
            logging.info('adding minutes {}'.format(minute_offset))
            rounded_time = rounded_time + timedelta(minutes=minute_offset)
        logging.info('time after round {}'.format(rounded_time))
        return rounded_time

    # Gets the sigV4 signed header value based on the customers endpointurl, request type, and body
    def get_signed_headers(self, request_method, request_endpoint_url, request_body):
        config = self.config
        # Generate signed http headers for Sigv4
        AWS_REGION = config['AMC']['amcInstanceRegion']
        request = AWSRequest(method=request_method.upper(), url=request_endpoint_url, data=request_body)
        SigV4Auth(Session().get_credentials(), "execute-api", AWS_REGION).add_auth(request)
        return dict(request.headers.items())

    # returns all workflows for the AMC endpoint
    def get_workflows(self):
        config = self.config
        logger = self.logger

        url = "{}/workflows".format(config['AMC']['amcApiEndpoint'])
        message = ''
        request_method = 'GET'
        request_body = ''
        receivedWorkFlows = False
        workflowIdList = []
        AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                         headers=self.get_signed_headers(request_method, url,
                                                                                         request_body),
                                                         body=request_body)

        AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))
        if AMC_API_RESPONSE.status == 200:

            for index in range(len(AMC_API_RESPONSE_DICTIONARY['workflows'])):
                workflowIdList.append(AMC_API_RESPONSE_DICTIONARY['workflows'][index]['workflowId'])
                receivedWorkFlows = True
            message = 'Successfully received list of workflows'
            logger.info(message)

        else:
            receivedWorkFlows = False
            message = 'Failed to receive list of workflows'
            logger.error(message)

        returnValue = {
            'statusCode': AMC_API_RESPONSE.status,
            'message': message,
            'endpointUrl': config['AMC']['amcApiEndpoint'],
            'workflowIdList': ','.join(workflowIdList),
            'body': AMC_API_RESPONSE_DICTIONARY
        }

        if not receivedWorkFlows:
            logger.error(message)
            logger.error(returnValue)
            return None

        return returnValue

    # returns all executions for a speicifc workflow based on workflow id
    def get_execution_status_by_workflowid(self, workflowId):
        config = self.config
        logger = self.logger
        message = []
        receivedWorkflowStatus = False
        request_method = 'GET'
        request_body = ''

        url = "{}/workflowExecutions/?workflowId={}".format(config['AMC']['amcApiEndpoint'], workflowId)

        AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                         headers=self.get_signed_headers(request_method, url,
                                                                                         request_body),
                                                         body=request_body)
        AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

        if (AMC_API_RESPONSE.status == 200):
            receivedWorkflowStatus = True
        else:
            receivedWorkflowStatus = False
            message = 'Failed to receive workflow status for workflow {}'.format(workflowId)
            logger.error(message)

        returnValue = {
            'statusCode': AMC_API_RESPONSE.status,
            'endpointUrl': url,
            'message': message,
            'body': AMC_API_RESPONSE_DICTIONARY
        }

        if not receivedWorkflowStatus:
            logger.error(message)
            logger.error(returnValue)
            return None

        return returnValue

    # returns the execution status for a execution based on execution id
    def get_execution_status_by_workflow_executionid(self, workflowExecutionId):
        config = self.config
        logger = self.logger
        receivedExecutionStatus = False
        message = []
        request_method = 'GET'
        request_body = ''
        url = "{}/workflowExecutions/{}".format(config['AMC']['amcApiEndpoint'], workflowExecutionId)
        AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                         headers=self.get_signed_headers(request_method, url,
                                                                                         request_body),
                                                         body=request_body)
        AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

        if (AMC_API_RESPONSE.status == 200):
            receivedExecutionStatus = True

        else:
            receivedExecutionStatus = False
            message.append(
                "Failed to Receive Execution Status for Workflow Execution ID {}".format(workflowExecutionId))
            logger.error(message)

        returnValue = {
            'statusCode': AMC_API_RESPONSE.status,
            'endpointUrl': url,
            'message': message,
            'body': AMC_API_RESPONSE_DICTIONARY
        }

        if not receivedExecutionStatus:
            logger.error(message)
            logger.error(returnValue)
            return None

    # Returns all executions for the Endpoint created after a specified creation time in %Y-%m-%dT00:00:00 format
    def get_execution_status_by_minimum_create_time(self, minCreationTime):
        config = self.config
        logger = self.logger
        receivedExecutionStatus = True
        message = []
        request_method = 'GET'
        request_body = ''

        executions = []
        statuses = {}
        AMC_API_RESPONSE_DICTIONARY = {}
        AMC_API_RESPONSE_DICTIONARY['nextToken'] = ''

        # Loop over responses if there is a nextToken in the response to get all executions
        while 'nextToken' in AMC_API_RESPONSE_DICTIONARY and receivedExecutionStatus:
            receivedExecutionStatus = False
            url = "{}/workflowExecutions/?{}".format(config['AMC']['amcApiEndpoint'], urlencode(
                {'minCreationTime': minCreationTime, "nextToken": AMC_API_RESPONSE_DICTIONARY['nextToken']}))
            AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                             headers=self.get_signed_headers(request_method, url,
                                                                                             request_body),
                                                             body=request_body)
            AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))
            statuses[url] = AMC_API_RESPONSE.status

            if (AMC_API_RESPONSE.status == 200 and 'executions' in AMC_API_RESPONSE_DICTIONARY):
                receivedExecutionStatus = True
                executions += AMC_API_RESPONSE_DICTIONARY['executions'].copy()
            else:
                receivedExecutionStatus = False
                message.append("Failed to Receive Execution Status: {} {}".format(AMC_API_RESPONSE.status,
                                                                                  AMC_API_RESPONSE_DICTIONARY))
                logger.error(message)

        returnValue = {
            'statusCode': max(statuses.values()),
            'statuses': statuses,
            'executions': executions
        }

        if not receivedExecutionStatus:
            wfmutils.sns_publish_message(config, ' '.join(message), returnValue)

        return returnValue

    # Creates a workflow, if the workflow already exists an error message will be displayed but an update
    # request will automatically be created by default
    def create_workflow(self, payload, update_if_already_exists=True):
        config = self.config
        logger = self.logger
        url = "{}/workflows".format(config['AMC']['amcApiEndpoint'])

        workflowCreated = False
        message = ''
        request_method = 'POST'
        request_body = json.dumps(payload)
        AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                         headers=self.get_signed_headers(request_method, url,
                                                                                         request_body),
                                                         body=request_body)
        AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

        if (AMC_API_RESPONSE.status == 200):
            workflowCreated = True
            message = 'Successfully created workflow {}'.format(payload['workflowId'])
            logger.info(message)
        else:
            workflowCreated = False
            message = 'Failed to create workflow {}'.format(payload['workflowId'])
            logger.error(message)
            if 'message' in AMC_API_RESPONSE_DICTIONARY and AMC_API_RESPONSE_DICTIONARY[
                "message"] == "Workflow with ID {} already exists.".format(payload['workflowId']):
                if update_if_already_exists:
                    # Insert failed due to workflow already exiting, running an update instead
                    logger.info(
                        'Insert failed bacause the workflowId {} already exists submitting an update request'.format(
                            payload['workflowId']))
                    updateResponse = self.update_workflow(payload)
                    return (updateResponse)

        returnValue = {
            'statusCode': AMC_API_RESPONSE.status,
            'message': message,
            'endpointUrl': url,
            'body': AMC_API_RESPONSE_DICTIONARY
        }

        if not workflowCreated:
            logger.error(returnValue)
            return None

        return returnValue

    # updates an existing workflow
    def update_workflow(self, payload):
        config = self.config
        logger = self.logger
        workflowUpdated = False
        message = ''

        url = "{}/workflows/{}".format(config['AMC']['amcApiEndpoint'], payload['workflowId'])
        request_method = 'PUT'
        request_body = json.dumps(payload)
        AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                         headers=self.get_signed_headers(request_method, url,
                                                                                         request_body),
                                                         body=request_body)
        AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

        if (AMC_API_RESPONSE.status == 200):
            workflowUpdated = True
            message = 'Successfully updated workflow {}'.format(payload['workflowId'])
            logger.info(message)
        else:
            workflowUpdated = False
            message = 'Failed to update workflow {}'.format(payload['workflowId'])
            logger.error(message)

        returnValue = {
            'statusCode': AMC_API_RESPONSE.status,
            'message': message,
            'endpointUrl': url,
            'body': AMC_API_RESPONSE_DICTIONARY
        }

        if not workflowUpdated:
            logger.error(message)
            logger.error(returnValue)
            return None

        return returnValue

    # deletes an existing workflow based on workflow id
    def delete_workflow(self, payload):
        config = self.config
        logger = self.logger
        workflowDeleted = False
        message = ''
        url = "{}/workflows/{}".format(config['AMC']['amcApiEndpoint'], payload['workflowId'])
        request_method = 'DELETE'
        request_body = json.dumps(payload)
        AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                         headers=self.get_signed_headers(request_method, url,
                                                                                         request_body),
                                                         body=request_body)
        AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

        logger.info('Workflow delete response {}'.format(AMC_API_RESPONSE))
        if (AMC_API_RESPONSE.status == 200):
            workflowDeleted = True
            message = 'Successfully deleted workflow {} for customerId {}'.format(payload['workflowId'],
                                                                                  config['customerId'])
            logger.info(message)
        else:
            workflowDeleted = False
            message = 'Failed to delete workflow {}'.format(payload['workflowId'])
            logger.error(message)

        returnValue = {
            'statusCode': AMC_API_RESPONSE.status,
            'message': message,
            'endpointUrl': url,
            'body': AMC_API_RESPONSE_DICTIONARY
        }

        if not workflowDeleted:
            logger.error(message)
            logger.error(returnValue)
            return None

        return returnValue

    # gets the the workflow definition of an existing workflow based on workflow id
    def get_workflow(self, payload):
        config = self.config
        logger = self.logger
        message = ''
        url = "{}/workflows/{}".format(config['AMC']['amcApiEndpoint'], payload['workflowId'])
        request_method = 'GET'
        request_body = ''
        logger.info('get workflow request URL: {}'.format(url))
        AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                         headers=self.get_signed_headers(request_method, url,
                                                                                         request_body),
                                                         body=request_body)
        logger.info('response data: {}'.format(AMC_API_RESPONSE.data))
        AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))
        logger.info('get workflow response {}'.format(AMC_API_RESPONSE))
        if (AMC_API_RESPONSE.status == 200):
            workflowreceived = True
            message = 'Successfully recevied workflow {}'.format(payload['workflowId'])
            logger.info(message)
        else:
            workflowreceived = False
            message = 'Failed to get workflow {}'.format(payload['workflowId'])
            logger.error(message)
            return None

        returnValue = {
            'statusCode': AMC_API_RESPONSE.status,
            'message': message,
            'endpointUrl': url,
            'body': AMC_API_RESPONSE_DICTIONARY
        }
        return (returnValue)

    # Creates a AMC workflow execution, allows for dynamic date offsets like TODAY(-1) etc.
    def create_workflow_execution(self, payload):
        config = self.config
        logger = self.logger
        executedWorkflow = False
        logger.info(payload)
        message = ''

        # Process the parameters to enable now() and today() functions
        if "parameterValues" in payload:
            for parameter in payload['parameterValues']:
                payload['parameterValues'][parameter] = self.process_parameter_functions(
                    payload['parameterValues'][parameter])
                logger.info("updated parameter {} to {}".format(parameter, payload['parameterValues'][parameter]))

        payload['timeWindowStart'] = self.process_parameter_functions(payload['timeWindowStart'])
        logger.info("updated parameter timeWindowStart to {}".format(payload['timeWindowStart']))
        payload['timeWindowEnd'] = self.process_parameter_functions(payload['timeWindowEnd'])
        logger.info("updated parameter timeWindowEnd to {}".format(payload['timeWindowEnd']))

        url = "{}/workflowExecutions".format(config['AMC']['amcApiEndpoint'])
        request_method = 'POST'
        request_body = json.dumps(payload)
        AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                         headers=self.get_signed_headers(request_method, url,
                                                                                         request_body),
                                                         body=request_body)
        AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

        if (AMC_API_RESPONSE.status == 200):
            executedWorkflow = True
            message = 'Successfully created an exeuction for workflow {}'.format(payload['workflowId'])
            logger.info(message)
        else:
            executedWorkflow = False
            message = 'Failed to create an exeuction for workflow {}'.format(payload['workflowId'])
            logger.error(message)
        returnValue = {
            'statusCode': AMC_API_RESPONSE.status,
            'message': message,
            'endpointUrl': url,
            'body': AMC_API_RESPONSE_DICTIONARY
        }
        if executedWorkflow == False:
            logger.error(returnValue)
            return None

        return returnValue

    # Deletes a workflow execution based on workflow id
    def delete_workflow_execution(self, payload):
        config = self.config
        logger = self.logger
        workflowExecutionDeleted = False
        message = ''
        url = "{}/workflowExecutions/{}".format(config['AMC']['amcApiEndpoint'], payload['workflowExecutionId'])

        request_method = 'DELETE'
        request_body = json.dumps(payload)
        AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                         headers=self.get_signed_headers(request_method, url,
                                                                                         request_body),
                                                         body=request_body)
        AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

        logger.info(
            'Workflow delete response code: {} response : {}'.format(AMC_API_RESPONSE.status,
                                                                     AMC_API_RESPONSE_DICTIONARY))
        if (AMC_API_RESPONSE.status == 200):
            workflowExecutionDeleted = True
            message = 'Successfully deleted workflow execution {} for customerId {}'.format(
                payload['workflowExecutionId'], config['customerId'])
            logger.info(message)
        else:
            workflowExecutionDeleted = False
            message = 'Failed to delete workflow {}'.format(payload['workflowExecutionId'])
            logger.error(message)

        returnValue = {
            'statusCode': AMC_API_RESPONSE.status,
            'customerId': config['customerId'],
            'message': message,
            'endpointUrl': url,
            'body': AMC_API_RESPONSE_DICTIONARY
        }

        if not workflowExecutionDeleted:
            logger.error(returnValue)
            return None

        return returnValue

    # returns all schedules for the AMC instance
    def get_schedules(self):
        config = self.config
        logger = self.logger
        message = ''
        url = "{}/schedules".format(config['AMC']['amcApiEndpoint'])
        request_method = 'GET'
        request_body = ''
        logger.info('get workflow request URL: {}'.format(url))
        AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                         headers=self.get_signed_headers(request_method, url,
                                                                                         request_body),
                                                         body=request_body)
        logger.info('response data: {}'.format(AMC_API_RESPONSE.data))
        AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

        logger.info('get schedules response {}'.format(AMC_API_RESPONSE))
        if (AMC_API_RESPONSE.status == 200):
            schedules_received = True
            message = 'Successfully received schedules for customerId {}'.format(config['customerId'])
            logger.info(message)
        else:
            schedules_received = False
            message = 'Failed to get schedules for customerId {}'.format(config['customerId'])
            logger.error(message)

        returnValue = {
            'customerId': config['customerId'],
            'statusCode': AMC_API_RESPONSE.status,
            'message': message,
            'endpointUrl': url,
            'body': AMC_API_RESPONSE_DICTIONARY
        }

        if not schedules_received:
            logger.error(returnValue)
            return None
        return returnValue

    # deletes one or more workflow schedules from the AMC instance
    def delete_schedule(self, payload):
        config = self.config
        logger = self.logger
        return_values = []
        if type(payload) != dict:
            payload = {"scheduleIds": [payload]}

        for schedule_id in payload['scheduleIds']:

            schedule_deleted = False
            message = ''
            url = "{}/schedules/{}".format(config['AMC']['amcApiEndpoint'], schedule_id)
            request_method = 'DELETE'
            request_body = json.dumps(payload)
            AMC_API_RESPONSE = urllib3.PoolManager().request(request_method, url,
                                                             headers=self.get_signed_headers(config, request_method,
                                                                                             url,
                                                                                             request_body),
                                                             body=request_body)
            AMC_API_RESPONSE_DICTIONARY = json.loads(AMC_API_RESPONSE.data.decode("utf-8"))

            logger.info('schedule delete response {}'.format(AMC_API_RESPONSE))
            if (AMC_API_RESPONSE.status == 200):
                schedule_deleted = True
                message = 'Successfully deleted schedule {} for customerId {}'.format(schedule_id, config['customerId'])
                logger.info(message)
            else:
                schedule_deleted = False
                message = 'Failed to delete schedule {} for customerId {}'.format(schedule_id, config['customerId'])
                logger.error(message)

            returnValue = {
                'customerId': config['customerId'],
                'statusCode': AMC_API_RESPONSE.status,
                'message': message,
                'endpointUrl': url,
                'body': AMC_API_RESPONSE_DICTIONARY
            }

            if not schedule_deleted:
                logger.error(returnValue)
            return_values.append(returnValue.copy())
        return return_values

