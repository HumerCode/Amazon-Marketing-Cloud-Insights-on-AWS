#######################################################
# Blueprint example of a custom transformation
# where a number of CSV files are dowloaded from
# Stage bucket and then submitted to a Glue Job
#######################################################
# License: Apache 2.0
#######################################################
# Author: jaidi
#######################################################

#######################################################
# Import section
# common-pipLibrary repository can be leveraged
# to add external libraries as a layer
#######################################################
import json
import datetime as dt

import boto3
import sys

import awswrangler as wr

from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import S3Configuration, KMSConfiguration
from datalake_library.interfaces.s3_interface import S3Interface


logger = init_logger(__name__)

# Create a client for the AWS Analytical service to use
client = boto3.client('glue')


def datetimeconverter(o):
    if isinstance(o, dt.datetime):
        return o.__str__()


class CustomTransform():
    def __init__(self):
        logger.info("Glue Job Blueprint Heavy Transform initiated")

    def transform_object(self, bucket, keys, team, dataset):
        
        ssm=boto3.client('ssm')

        silver_catalog = ssm.get_parameter(
            Name='/SDLF/Glue/{}/{}/StageDataCatalog'.format(team, dataset),
            WithDecryption=True
        ).get('Parameter').get('Value')

        gold_catalog = 'test'
        try :
            gold_catalog = ssm.get_parameter(
              Name='/SDLF/Glue/{}/{}/AnalyticsDataCatalog'.format(team, dataset),
              WithDecryption=True
             ).get('Parameter').get('Value')
        except: # catch *all* exceptions
            gold_catalog = 'test'
            logger.info('No analytic db')

        job_name = '{}-{}-{}-glue-job'.format('sdlf', team, dataset)  # Name of the Glue Job
        analytics_bucket = S3Configuration().analytics_bucket

        #######################################################
        # We assume a Glue Job has already been created based on
        # customer needs. This function makes an API call to start it
        #######################################################     
        tables = []

        s3LocationsToAdd = {}
         
        keycounter = 0
        logger.info('Processing Keys: {}\n-----------------'.format(keys))
        for key in keys:
            keycounter+=1 
            logger.info("key {}: {}".format(keycounter, key))
            tablePath = '/'.join(key.split('/')[:4])
            logger.info("tablePath:{}".format(tablePath))
            tableS3Location = 's3://{}/{}/{}'.format(bucket, 'post-stage', tablePath.split('/', 1)[1])
            logger.info('tableS3Location:{}'.format(tableS3Location))
            s3LocationsToAdd[tableS3Location]=True
            table_partitions = '/'.join(key.split('/')[4:-1])
            logger.info('table_partitions:{}'.format(table_partitions))

            # table=wr.catalog.sanitize_table_name(tablePath.rsplit('/')[-1])

            ### TO DO UPDATE TABLES APPEND ###
            sanitized_table_name = wr.catalog.sanitize_table_name(tablePath.rsplit('/')[-1])
            if sanitized_table_name not in tables:
                tables.append(
                    # key.split('/')[-1].split('_')[0]
                    # sanitized_table_name
                    "{}/{}".format(sanitized_table_name, table_partitions)
                )

        # commented as crawler not needed
        # crawlerName='sdlf-{}-{}-post-stage-crawler'.format(team,dataset)
        # getCrawlerResponse=client.get_crawler(Name=crawlerName)
        # s3Targets = getCrawlerResponse['Crawler']['Targets']['S3Targets']
        # for S3Target in s3Targets:
        #     s3LocationsToAdd[S3Target['Path']]= False
        
        # for (locationToAdd, add) in s3LocationsToAdd.items():
        #         if add:
        #             s3Targets.append({'Path': locationToAdd, 'Exclusions' : []})
        # crawlerUpdateRequest = getCrawlerResponse.copy()
        # crawlerUpdateRequest['Crawler']['Targets']['S3Targets'] = s3Targets
        # logger.info('updatedCrawlerRequest: {}'.format(crawlerUpdateRequest))

        ### TO DO ADD LOGIC TO BATCH KEY PROCESSING ###
        # uniqueKeys = list(dict.fromkeys([i.rsplit('/', 1)[0] for i in keys]))
        uniqueKeys=[]
        for i in keys:
            uniqueKeys.append('s3://{}/{}'.format(bucket, i))

        source_locations = ','.join(uniqueKeys)
        #### END UPDATE ####

        #Try to read the schema from the destination table (if it exists) and convert the CSV inferrred schema to match the table schema
        # try:
        #     glueClient = boto3.client('glue')
        #     tableSchema = {}
        #     getTableResult = glueClient.get_table(DatabaseName=silver_catalog,Name=targetTableName)
        #     logger.info('Get Table result:{}'.format(getTableResult))
        #     for tableColumn in getTableResult['Table']['StorageDescriptor']['Columns']:
        #         tableSchema[tableColumn['Name']] = tableColumn['Type']
        #         logger.info('{}'.format(tableschema))

        # except: # catch *all* exceptions
        #     logger.info('error getting table schema {}'.format(sys.exc_info()[0]))

        
        # S3 Path where Glue Job outputs processed keys
        # IMPORTANT: Build the output s3_path without the s3://stage-bucket/
        processed_keys_path = 'post-stage/{}/{}'.format(team, dataset)
        job_name = 'sdlf-{}-{}-glue-job'.format(team,dataset)  # Name of the Glue Job
        #source_location = 's3://{}/{}'.format(bucket, keys[0].rsplit('/', 1)[0])
        source_location = 's3://{}/{}'.format(bucket, keys[0])

        output_location = 's3://{}/{}'.format(bucket, processed_keys_path)
        logger.info('trying to call job: {} \nsource_location: {} \noutput_location: {} \ntables: {}'.format(job_name,source_location,output_location,tables))
        kms_key = KMSConfiguration(team).get_kms_arn
        # Submitting a new Glue Job
        job_response = client.start_job_run(
            JobName=job_name,
            Arguments={
                # Specify any arguments needed based on bucket and keys (e.g. input/output S3 locations)
                '--JOB_NAME': job_name,
                # '--additional-python-modules' : "pyarrow==2,awswrangler==2.4.0",
                '--job-bookmark-option': 'job-bookmark-disable',
                '--SOURCE_LOCATIONS': source_locations,
                '--SOURCE_LOCATION': source_location,
                '--OUTPUT_LOCATION': output_location,
                '--SILVER_CATALOG': silver_catalog,
                '--KMS_KEY' : kms_key,
                '--GOLD_CATALOG': gold_catalog,
            },
            MaxCapacity=1.0
        )
        # Collecting details about Glue Job after submission (e.g. jobRunId for Glue)
        json_data = json.loads(json.dumps(
            job_response, default=datetimeconverter))
        job_details = {
            "jobName": job_name,
            "jobRunId": json_data.get('JobRunId'),
            "jobStatus": 'STARTED',
            "tables": tables
        }

        #######################################################
        # IMPORTANT
        # This function must return a dictionary object with at least a reference to:
        # 1) processedKeysPath (i.e. S3 path where job outputs data without the s3://stage-bucket/ prefix)
        # 2) jobDetails (i.e. a Dictionary holding information about the job
        # e.g. jobName and jobId for Glue or clusterId and stepId for EMR
        # A jobStatus key MUST be present in jobDetails as it's used to determine the status of the job)
        # Example: {processedKeysPath' = 'post-stage/engineering/legislators',
        # 'jobDetails': {'jobName': 'legislators-glue-job', 'jobId': 'jr-2ds438nfinev34', 'jobStatus': 'STARTED'}}
        #######################################################
        response = {
            'processedKeysPath': processed_keys_path,
            'jobDetails': job_details
        }
        
        return response

    def check_job_status(self, bucket, keys, processed_keys_path, job_details):
        # This function checks the status of the currently running job
        job_response = client.get_job_run(
            JobName=job_details['jobName'], RunId=job_details['jobRunId'])
        json_data = json.loads(json.dumps(
            job_response, default=datetimeconverter))
        # IMPORTANT update the status of the job based on the job_response (e.g RUNNING, SUCCEEDED, FAILED)
        job_details['jobStatus'] = json_data.get('JobRun').get('JobRunState')

        #######################################################
        # IMPORTANT
        # This function must return a dictionary object with at least a reference to:
        # 1) processedKeysPath (i.e. S3 path where job outputs data without the s3://stage-bucket/ prefix)
        # 2) jobDetails (i.e. a Dictionary holding information about the job
        # e.g. jobName and jobId for Glue or clusterId and stepId for EMR
        # A jobStatus key MUST be present in jobDetails as it's used to determine the status of the job)
        # Example: {processedKeysPath' = 'post-stage/legislators',
        # 'jobDetails': {'jobName': 'legislators-glue-job', 'jobId': 'jr-2ds438nfinev34', 'jobStatus': 'RUNNING'}}
        #######################################################
        response = {
            'processedKeysPath': processed_keys_path,
            'jobDetails': job_details
        }

        return response
