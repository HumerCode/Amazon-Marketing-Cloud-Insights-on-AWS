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

#######################################################
# Blueprint example of a custom transformation
# where a JSON file is dowloaded from RAW to /tmp
# then parsed before being re-uploaded to STAGE
#######################################################
# License: Apache 2.0
#######################################################
# Author: jaidi
#######################################################



import json
import pandas as pd
import awswrangler as wr
import re
import pandas as pd
import numpy as np
import boto3
from boto3.dynamodb.conditions import Key, Attr
import io
import os

#######################################################
# Use S3 Interface to interact with S3 objects
# For example to download/upload them
#######################################################
from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import S3Configuration, KMSConfiguration
from datalake_library.interfaces.s3_interface import S3Interface

s3 = boto3.resource('s3')
dynamodb = boto3.resource("dynamodb")
ssm=boto3.client('ssm')

s3_interface = S3Interface()
# IMPORTANT: Stage bucket where transformed data must be uploaded
stage_bucket = S3Configuration().stage_bucket

logger = init_logger(__name__)


class CustomTransform():
    def __init__(self):
        logger.info("S3 Blueprint Light Transform initiated")

    def transform_object(self, bucket, key, team, dataset):
        
        #show the object key received
        logger.info('SOURCE OBJECT KEY: ' + key)

        #retreive the source file as an S3 object
        s3Object = s3.Object(bucket, key)

        #get the file size - originally we would send the file size to the email lambda to determine if it can be attached
        fileSize=s3Object.content_length

        #get the file last modified date as a formatted string        
        fileLastModified = s3_interface.get_last_modified(bucket, key)
        fileLastModified = fileLastModified.replace(' ', '-').replace(':', '-').split('+')[0]
        print('fileLastModified: {}'.format(fileLastModified))

        #initialize our variables extracted from the key:
        keyTeam = keyDataset = workflowName = scheduleFrequency = fileName = fileYear = fileMonth = fileDay = fileHour = fileMinute = fileSecond = fileMillisecond = fileBasename = fileExtension = fileVersion = ''

        processed_keys = []

        #break the file key into it's name compoments
        keyParts = re.match("workflow=([^/]*)/schedule=([^/]*)/(.*)", key) # USE WHEN INGESTION BUCKET OUTSIDE OF LAKE

        if keyParts is not None :
            # keyTeam,keyDataset,workflowName, scheduleFrequency, fileName = keyParts.groups() # use 1 line if ingesting from raw bucket
            workflowName, scheduleFrequency, fileName = keyParts.groups() # use below 3 lines if ingesting direct from amc bucket
            keyTeam = team
            keyDataset = dataset

            #see if the workflow name matches the naming scheme for a AMC UI result:
            if re.match("analytics-[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}", workflowName) is not None:
                logger.info("Workflow name {} appears to be a result from the AMC UI, skipping transformation, setting processed_keys to an empty array".format(workflowName))
                processed_keys = []
                return(processed_keys)

            #see if the key matches the file name patern with time e.g. workflow=standard_geo_date_summary_V3/schedule=adhoc/file_last_modified=2020-02-03-12-25-38/2020-02-03T14:01:47Z-standard_geo_date_summary.csv
        fileNameWithTimeSearchResults = re.match("([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})\.([0-9]{3})Z-([^\.]*)\.(.*)",fileName)

        if fileNameWithTimeSearchResults is not None:
            fileYear, fileMonth, fileDay, fileHour, fileMinute, fileSecond,fileMillisecond, fileBasename,fileExtension = fileNameWithTimeSearchResults.groups()
        else:
            #Check to see if the key matches the patern with date only e.g. workflow=standard_geo_date_summary_V3/schedule=weekly/2020-02-04-standard_geo_date_summary_V3-ver2.csv
            fileNameDateOnlySearchResults = re.match("([0-9]{4})-([0-9]{2})-([0-9]{2})-([^.]*)\.(.*)",fileName)
            if fileNameDateOnlySearchResults is not None:
                fileYear, fileMonth, fileDay, fileBasename ,fileExtension = fileNameDateOnlySearchResults.groups()
        
        #check to see if we have a version appended to the end fo the file basename
        versionResults = re.match(".*-(ver[0-9])",fileBasename)
        if versionResults is not None :
            fileVersion = versionResults.groups()[0]

        customer_config = ssm.get_parameter(
            Name='/AMC/DynamoDB/DataLake/CustomerConfig',
            WithDecryption=True
        ).get('Parameter').get('Value')
        config_table = dynamodb.Table(customer_config)
        response = config_table.query(
            IndexName='amc-index',
            Select='ALL_PROJECTED_ATTRIBUTES',
            KeyConditionExpression=Key('hash_key').eq(bucket)
        )
        prefix = response['Items'][0]['prefix'].lower()
        customer_hash_key = response['Items'][0]['customer_hash_key'].lower()
        print('prefix: {}'.format(prefix))
        ##################################

        #read the source file into a pandas sataframe, replaceing double quotes with single quotes before it is processed by pandas
        print ("fileExtension : " + fileExtension)
        print ("workflowName : " + workflowName)
        print ("scheduleFrequency : " + scheduleFrequency)
        
        if fileExtension.lower() == 'csv' and workflowName != '' and scheduleFrequency != '' :

            ### Validate small file ###
            if fileSize < 1000:
                print ("File Size small")
                line_count = s3Object.get()['Body'].read().decode('utf-8').count('\n')
                if line_count <= 1:
                    print ("Count small")
                    return processed_keys

            oob_reports = []

            if 'oob_reports' in response['Items'][0]:
                for wf in response['Items'][0]['oob_reports']:
                    oob_reports.append(wf)

            #Calculate the output path with paritioning based on the original file name
            output_path = ''

            # ### OUTPUT_PATH FOR OOB_REPORTS
            if workflowName not in oob_reports:
                table_prefix=prefix
            else:
                table_prefix = 'amc'

            if fileVersion != '':
                output_path = "{}_{}_{}_{}/customer_hash={}/export_year={}/export_month={}/file_last_modified={}/{}.{}".format(table_prefix,workflowName,scheduleFrequency,fileVersion,customer_hash_key,fileYear,fileMonth,fileLastModified,fileBasename,fileExtension)
            else:
                output_path = "{}_{}_{}/customer_hash={}/export_year={}/export_month={}/file_last_modified={}/{}.{}".format(table_prefix,workflowName,scheduleFrequency,customer_hash_key,fileYear,fileMonth,fileLastModified,fileBasename,fileExtension)


            print ("output Path : " + output_path)
            #Handle camelcase and make exception for jnj
            if table_prefix != 'jnj':
                output_path = os.path.splitext(output_path)[0].rsplit('/', 1)[0].split('/')
                output_path[0] = wr.catalog.sanitize_table_name(output_path[0])
                output_path = '/'.join(output_path)
                output_path = '{}/{}.{}'.format(output_path,fileBasename,fileExtension)

            # Uploading file to Stage bucket at appropriate path
            # IMPORTANT: Build the output s3_path without the s3://stage-bucket/
            s3_path = 'pre-stage/{}/{}/{}'.format(team,dataset, output_path)
            print('S3 Path: {}'.format(s3_path))

            s3OutputPath = 's3://{}/{}'.format(stage_bucket,s3_path)

            kms_key = KMSConfiguration("Stage").get_kms_arn
            
            content = s3Object.get()['Body'].read().decode("UTF8").replace('\\"',"'")

            fileMetaData = {
            'keyTeam' : keyTeam,
            'keyDataset' : keyDataset,
            'workflowName' : workflowName,
            'scheduleFrequency' : scheduleFrequency,
            'fileName' : fileName,
            'fileYear' : fileYear,
            'fileMonth' : fileMonth,
            'fileDay' : fileDay,
            'fileHour' : fileHour,
            'fileMinute' : fileMinute,
            'fileSecond' :  fileSecond,
            'fileMillisecond' : fileMillisecond,
            'fileBasename' : fileBasename,
            'fileExtension' : fileExtension,
            'fileVersion' : fileVersion,
            'partitionedPath': output_path.rsplit('/', 1)[0]
            }

            s3.Object(stage_bucket, s3_path).put(Body=content, ServerSideEncryption='aws:kms',SSEKMSKeyId=kms_key,
            Metadata=fileMetaData
            )

            # IMPORTANT S3 path(s) must be stored in a list
            processed_keys = [s3_path]

        #######################################################
        # IMPORTANT
        # This function must return a Python list
        # of transformed S3 paths. Example:
        # ['pre-stage/engineering/legislators/persons_parsed.json']
        #######################################################

        return processed_keys