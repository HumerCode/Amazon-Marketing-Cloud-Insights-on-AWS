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

import sys
sys.path.insert(0, '/glue/lib/installation')
keys = [k for k in sys.modules.keys() if 'boto' in k]
for k in keys:
    if 'boto' in k:
       del sys.modules[k]

import awswrangler as wr
import boto3
import pandas as pd
import numpy as np
import io
import re
import logging
from awsglue.utils import getResolvedOptions
import os

print('boto3 version')
print(boto3.__version__)

# create logger
logging.basicConfig()
logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)

print("region : " + os.environ['AWS_DEFAULT_REGION'])

glue_client = boto3.client('glue', region_name = str(os.environ['AWS_DEFAULT_REGION']) )
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
lf_client = boto3.client('lakeformation')

default_column_map={
    'advertiser_id': -1,
    'advertiser': 'FILTERED',
    'campaign_id': -1,
    'campaign':'FILTERED'}

default_column_type={
    'campaign_start_date': 'datetime64',
    'campaign_end_date': 'datetime64',
    'advertiser_id' : 'int64',
    'campaign_id' : 'int64'}

exclude_workflow = ['standard_impressions_by_browser_family', 'standard_impressions-by-browser-family']

float_data_types_kinds = ['i','u','f']
glue_data_type_excep = ['STRING', 'TIMESTAMP']
number_column_exclude = ['advertiser_id','campaign_id']
pandas_athena_datatypes = {
    "float64":"double",
    "datetime64[ns]":"timestamp",
    "int64":"bigint"
    }

athena_pandas_datatypes = {
    "TIMESTAMP":"datetime64"
    }

def add_partitions(outputfilebasepath, silverCatalog, list_partns, targetTableName):
    partn_values = []
    patn_path_value = outputfilebasepath
    for prtns in list_partns:
        patn_path_value = patn_path_value + prtns["orgcolnm"] +"=" +str(prtns["value"]) + "/"
        partn_values.append(str(prtns["value"]))
    print ("Partition S3 Path : " + patn_path_value)
    print (str(partn_values))
    try:
        print ("Update partitions")
        wr.catalog.add_parquet_partitions(
            database=silverCatalog,
            table=wr.catalog.sanitize_table_name(targetTableName),
            compression='snappy',
            partitions_values={
                patn_path_value: partn_values
            }
        )
    except Exception as e:
        print ("Partition exist No need to update")
        print (str(e))

def get_partition_values (sourceFilepartitionedPath):
    list_partns = []
    cust_hash = ''
    for prtns in sourceFilepartitionedPath.split('/'):
        if '=' in prtns:
            d = {
                "orgcolnm" : str(prtns.split("=")[0]),
                "santcolnm" : wr.catalog.sanitize_column_name(str(prtns.split("=")[0])),
                "value" : str(prtns.split("=")[1])
                }
            list_partns.append(d)
        if 'customer_hash' in prtns:
            cust_hash = str(prtns.split("=")[1])

    return list_partns, cust_hash

def get_data_types (col_name, panda_data_kind, glue_data_type):
    default_cast = 'string'
    if ( (panda_data_kind in float_data_types_kinds) and (glue_data_type == None or glue_data_type.upper() not in glue_data_type_excep) ):
        default_cast = 'Float64'
    else:
        if glue_data_type != None:
            default_cast = athena_pandas_datatypes.get(glue_data_type.upper(), 'string')
    return default_cast

def add_tags_lf(cust_hash_tag_dict, database_name, table_name):
    try:
        get_tag_dtl = lf_client.get_lf_tag(
            TagKey=list(cust_hash_tag_dict.keys())[0]
        )
        print("Existing Tag details :" + list(cust_hash_tag_dict.keys())[0] + " : " + str(get_tag_dtl))
        tag_values = set(get_tag_dtl['TagValues'])
        tag_values.add(list(cust_hash_tag_dict.values())[0])
        print("New Tag values")
        print(tag_values)

        try:
            upd_tag_dtl = lf_client.update_lf_tag(
                TagKey=list(cust_hash_tag_dict.keys())[0],
                TagValuesToAdd=list(tag_values)
            )
            print("Updating existing Tag details :" + list(cust_hash_tag_dict.keys())[0] + " : " + str(upd_tag_dtl))
        except Exception as e:
            print("Exception while updating existing tags : " + str(e))


    except Exception as e:
        print("Exception while retrieving tag details : " + str(e))
        creat_tag_dtl = lf_client.create_lf_tag(
            TagKey=list(cust_hash_tag_dict.keys())[0],
            TagValues=[
                list(cust_hash_tag_dict.values())[0],
            ]
        )
        print("Creating Tag details :" + list(cust_hash_tag_dict.keys())[0] + " : " + str(creat_tag_dtl))


    try:
        addTagTbl = lf_client.add_lf_tags_to_resource(
            Resource={
                'Table': {
                    'DatabaseName': database_name,
                    'Name': table_name

                }
            },
            LFTags=[
                {
                    'TagKey': list(cust_hash_tag_dict.keys())[0],
                    'TagValues': [
                        list(cust_hash_tag_dict.values())[0]
                    ]
                },
            ]
        )
        print("Adding tags to table :" + list(cust_hash_tag_dict.keys())[0] + " : " + str(addTagTbl))
    except Exception as e:
        print("Exception while adding tags to tables : " + str(e))

def create_update_tbl (csvdf, csv_schema, tbl_schema, silverCatalog, targetTableName, list_partns, outputfilebasepath, table_exist, cust_hash):
    if (table_exist == 1):
        extra_cols = list(set(csv_schema.keys()) - set(tbl_schema.keys()))
        print ("extra_cols : " + str(extra_cols))
        
        tbl = glue_client.get_table(
                                DatabaseName=silverCatalog,
                                Name=wr.catalog.sanitize_table_name(targetTableName)
                           )
        print ("Existing table")
        print (tbl)
        
        strg_descrptr = tbl["Table"]["StorageDescriptor"]
        new_cols = []
        if (len(extra_cols) > 0):
            print ("Adding new columns")
            for col in extra_cols:
                print ("New col name : " + col)
                print ("New col type : " + pandas_athena_datatypes.get(csv_schema[col].lower(), 'string') ) 
                col_dict = {}
                col_dict = {
                                'Name': col,
                                'Type': pandas_athena_datatypes.get(csv_schema[col].lower(), 'string')
                            }
                new_cols.append (col_dict)
            
            strg_descrptr["Columns"].extend(new_cols)
            
            newtbldetails = {
                        'Name': wr.catalog.sanitize_table_name(targetTableName),
                        'StorageDescriptor': strg_descrptr,
                        'PartitionKeys' : tbl["Table"]["PartitionKeys"],
                        'TableType' : tbl["Table"]["TableType"],
                        'Parameters' : tbl["Table"]["Parameters"]
                }
            
            print ("new table defn")
            print (newtbldetails)
            
            resp = glue_client.update_table(
                                            DatabaseName = silverCatalog,
                                            TableInput = newtbldetails
                                      )
            
            print ("new table")
            print (resp)
        else :
            print ("No change in table")
            
        cust_hash_tag_dict = {
            'customer_hash': cust_hash
        }
        add_tags_lf(cust_hash_tag_dict, silverCatalog, wr.catalog.sanitize_table_name(targetTableName))
    else:
        print ("Table does not exist. Creating new table")
        col_dict = {}
        cust_hash_tag_dict = {
            'customer_hash': cust_hash
        }

        for colm in csvdf.columns:
            print ("New col name : " + colm)
            print ("New col type : " + pandas_athena_datatypes.get(str(csvdf.dtypes[colm]).lower(), 'string') ) 
            col_dict[colm] = str(pandas_athena_datatypes.get(str(csvdf.dtypes[colm]).lower(), 'string') )
        part_dict = {}
        for prtns in list_partns:
            part_dict[prtns["santcolnm"]] = 'string'
        
        print ("Create Table")
        print ("Column Dictionary : " + str(col_dict) )
        print ("Partition Dictionary : " + str(part_dict) )
        wr.catalog.create_parquet_table(
            database=silverCatalog,
            table=wr.catalog.sanitize_table_name(targetTableName),
            path=outputfilebasepath,
            columns_types=col_dict,
            partitions_types=part_dict,
            compression='snappy',
            parameters=cust_hash_tag_dict
        )
        add_tags_lf(cust_hash_tag_dict, silverCatalog, wr.catalog.sanitize_table_name(targetTableName))


def process_files (sourceLocations, outputLocation, kms_key, silverCatalog):

    for key in sourceLocations: # added for batching
        print("Processing Key: {}".format(key)) # added for batching
        sourceLocation=key
    
        s3Pathmatches = re.match('s3://([^/]*)/(.*)',sourceLocation)
    
        sourceBucket = ''
        sourceKey = ''
    
        if s3Pathmatches is not None:
            sourceBucket = s3Pathmatches.groups()[0]
            sourceKey = s3Pathmatches.groups()[1]
            print ("sourcebucket : " +  sourceBucket)
            print ("sourceKey : " +  sourceKey)
        else :
            sys.exit(-1)
            
        exception=False
        try:
            sourceS3Object = s3_client.head_object(Bucket=sourceBucket, Key=sourceKey)
            print('metadata:{}'.format(sourceS3Object['Metadata']))
        except Exception as e:
            print ("Error reading s3 file metadata : " + str(e))
            exception=True
            pass
        if exception==True:
            continue
        
        ## Extracting file metadata
        sourceFilepartitionedPath= sourceS3Object['Metadata']['partitionedpath']
        sourceFileBaseName = sourceS3Object['Metadata']['filebasename']
        sourceFileVersion = sourceS3Object['Metadata']['fileversion']
        sourceFileDataSet = sourceS3Object['Metadata']['keydataset']
        sourceFileTeam = sourceS3Object['Metadata']['keyteam']
        sourceFileScheduleFrequency = sourceS3Object['Metadata']['schedulefrequency']
        sourceFileWorkflowName= sourceS3Object['Metadata']['workflowname']
    
        targetTableName = sourceFilepartitionedPath.split('/')[0]
        print ("Sanitized table name : " + targetTableName)
    
        if sourceFileWorkflowName in exclude_workflow:
            continue
    
        csvdf = wr.s3.read_csv(path=[sourceLocation], header=0, skip_blank_lines=True, escapechar='\\')
        
        s3OutputPath = '{}/{}/{}.{}'.format(outputLocation,sourceFilepartitionedPath,sourceFileBaseName,'parquet')
        print ("s3 outpath : " + s3OutputPath)
        
        outputfilebasepath = '{}/{}/'.format(outputLocation,targetTableName)

        ## getting Table details
        table_exist = 1
        tbl_schema = {}
        try:
            df_table = wr.catalog.table(database=silverCatalog, table=wr.catalog.sanitize_table_name(targetTableName))
            print ("Existing table defn")
            for i, row in df_table.iterrows():
                tbl_schema[row['Column Name']] = row['Type']
        except Exception as e:
            print ("Table does not exist : " + str(e))
            table_exist = 0
    
    
        ## Adding default values for columns if they exist in default_column_map
        for column_key in list(csvdf.columns):
            if column_key in default_column_map.keys():
                print('Filling NULLs for columns: {}'.format(column_key))
                csvdf[column_key].fillna(default_column_map[column_key], inplace=True)

        ## casting columns to appropriate data types
        for c in csvdf.columns:
            datatype_col = ''
            try:
                if c in default_column_type.keys():
                    print('Attempting to cast {} as {}'.format(c,default_column_type[c]))
                    csvdf[c] = csvdf[c].astype(default_column_type[c])
                    print('casted {} as {}'.format(c,default_column_type[c]))
                    datatype_col = default_column_type[c]
                else:
                    if (c not in number_column_exclude):
                        print('Attempting to cast {} as {}'.format(c,get_data_types (c, str(csvdf[c].dtype.kind), tbl_schema.get(wr.catalog.sanitize_column_name(c), None) ) ) )
                        csvdf[c] = csvdf[c].astype(get_data_types (c, str(csvdf[c].dtype.kind), tbl_schema.get(wr.catalog.sanitize_column_name(c), None)))
                        print('casted {} as {}'.format(c,get_data_types (c, str(csvdf[c].dtype.kind), tbl_schema.get(wr.catalog.sanitize_column_name(c), None))))
                        datatype_col = get_data_types (c, str(csvdf[c].dtype.kind), tbl_schema.get(wr.catalog.sanitize_column_name(c), None))
            except TypeError as e:
                print('could not cast {} to {}: {}'.format(c,datatype_col,str(e)))
        
        ## get partition values
        list_partns = []
        cust_hash = ''
        list_partns, cust_hash = get_partition_values (sourceFilepartitionedPath)
        print ("Partions values : " + str(list_partns))
    
        ## write the file
        #write the parquet file using the kms key
        # Note: if writing to parquet and not as a dataset must specify entire path name.
        csvdf = wr.catalog.sanitize_dataframe_columns_names(csvdf)
        wr.s3.to_parquet(df=csvdf, path=s3OutputPath, compression='snappy',
                            s3_additional_kwargs={
                            'ServerSideEncryption': 'aws:kms',
                            'SSEKMSKeyId': kms_key})
        csv_schema = {}
        for colm in csvdf.columns:
            csv_schema[colm] = str(csvdf.dtypes[colm])
        print ("Final CSV schema : " + str(csv_schema) )
        
        ## Create or update table
        create_update_tbl (csvdf, csv_schema, tbl_schema, silverCatalog, targetTableName, list_partns, outputfilebasepath, table_exist, cust_hash)

        ## add partitions
        add_partitions(outputfilebasepath, silverCatalog, list_partns, targetTableName)
    


if __name__ == '__main__':
    args = getResolvedOptions(
        sys.argv, ['JOB_NAME', 'SOURCE_LOCATION', 'SOURCE_LOCATIONS', 'OUTPUT_LOCATION','SILVER_CATALOG','GOLD_CATALOG','KMS_KEY'])
    
    jobName = args['JOB_NAME']
    # jobName = "sdlf-ats-amcremcrawlds-glue-job-test"
    sourceLocation = args['SOURCE_LOCATION']
    # sourceLocation = "s3://amazonadtech-datalake-dev-us-east-1-921232273325-stage/pre-stage/ats/amcdataset/jnj_AudienceAnalysisby-ASIN_adhoc/customer_hash=jnj/export_year=2021/export_month=04/file_last_modified=2021-04-27T16-37-32/AudienceAnalysisby-ASIN.csv"
    # sourceLocation = "s3://amazonadtech-datalake-dev-us-east-1-921232273325-stage/pre-stage/ats/amcremcrawlds/jnj_AudienceAnalysisby-ASIN_adhoc/customer_hash=jnj/export_year=2021/export_month=03/file_last_modified=2021-03-27T16-37-32/AudienceAnalysisby-ASIN.csv"
    # sourceLocation = "s3://amazonadtech-datalake-dev-us-east-1-921232273325-stage/pre-stage/ats/amcdataset/jnj_AudienceAnalysisby-ASIN_adhoc/customer_hash=jnj/export_year=2021/export_month=03/file_last_modified=2021-03-27T16-37-32/AudienceAnalysisby-ASIN.csv"
    print('sourceLocation:{}'.format(sourceLocation))
    sourceLocations = args['SOURCE_LOCATIONS']
    # sourceLocations ="s3://amazonadtech-datalake-dev-us-east-1-921232273325-stage/pre-stage/ats/amcdataset/jnj_AudienceAnalysisby-ASIN_adhoc/customer_hash=jnj/export_year=2021/export_month=04/file_last_modified=2021-04-27T16-37-32/AudienceAnalysisby-ASIN.csv"
    # sourceLocations ="s3://amazonadtech-datalake-dev-us-east-1-921232273325-stage/pre-stage/ats/amcremcrawlds/jnj_AudienceAnalysisby-ASIN_adhoc/customer_hash=jnj/export_year=2021/export_month=03/file_last_modified=2021-03-27T16-37-32/AudienceAnalysisby-ASIN.csv"
    # sourceLocations ="s3://amazonadtech-datalake-dev-us-east-1-921232273325-stage/pre-stage/ats/amcdataset/jnj_AudienceAnalysisby-ASIN_adhoc/customer_hash=jnj/export_year=2021/export_month=04/file_last_modified=2021-04-27T16-37-32/AudienceAnalysisby-ASIN.csv,s3://amazonadtech-datalake-dev-us-east-1-921232273325-stage/pre-stage/ats/amcremcrawlds/jnj_AudienceAnalysisby-ASIN_adhoc/customer_hash=jnj/export_year=2021/export_month=03/file_last_modified=2021-03-27T16-37-32/AudienceAnalysisby-ASIN.csv,s3://amazonadtech-datalake-dev-us-east-1-921232273325-stage/pre-stage/ats/amcdataset/samsung_spons_prod_imp_click_sales_v1_adhoc/customer_hash=samsungin/export_year=2021/export_month=04/file_last_modified=2021-04-29T15-38-22/spons-prod-imp-click-sales_v1.csv,s3://amazonadtech-datalake-dev-us-east-1-921232273325-stage/pre-stage/ats/amcdataset/samsung_spons_prod_imp_click_sales_v1_adhoc/customer_hash=samsungca/export_year=2021/export_month=04/file_last_modified=2021-04-28T15-27-34/spons-prod-imp-click-sales_v1.csv,s3://amazonadtech-datalake-dev-us-east-1-921232273325-stage/pre-stage/ats/amcdataset/png_frequency_roas_by_campaign_v1_adhoc/customer_hash=atspandgus/export_year=2021/export_month=04/file_last_modified=2021-04-03T18-46-58/frequency-roas-by-campaign_v1.csv,s3://amazonadtech-datalake-dev-us-east-1-921232273325-stage/pre-stage/ats/amcdataset/png_frequency_roas_by_campaign_v1_adhoc/customer_hash=atspandgus/export_year=2021/export_month=05/file_last_modified=2021-05-03T18-45-45/frequency-roas-by-campaign_v1.csv,s3://amazonadtech-datalake-dev-us-east-1-921232273325-stage/pre-stage/ats/amcdataset/geicous_standard_impressions_by_operating_system_daily_ver3/customer_hash=geicous/export_year=2021/export_month=04/file_last_modified=2021-05-02T08-51-45/standard_impressions-by-operating-system-ver3.csv,s3://amazonadtech-datalake-dev-us-east-1-921232273325-stage/pre-stage/ats/amcdataset/geicous_standard_reach_daily_ver3/customer_hash=geicous/export_year=2021/export_month=04/file_last_modified=2021-05-03T10-55-05/standard_reach-ver3.csv"
    print('sourceLocations:{}'.format(sourceLocations))
    sourceLocations=sourceLocations.split(',')
    outputLocation = args['OUTPUT_LOCATION']
    # outputLocation = "s3://amazonadtech-datalake-dev-us-east-1-921232273325-stage/post-stage/ats/amcremcrawlds"
    print('outputLocation:{}'.format(outputLocation))
    silverCatalog = args['SILVER_CATALOG']
    # silverCatalog = "ats_amcremcrawlds_dev_stage"
    print('silverCatalog:{}'.format(silverCatalog))
    goldCatalog = args['GOLD_CATALOG']
    # goldCatalog = "ats_amcremcrawlds_dev_analytics"
    print('goldCatalog:{}'.format(goldCatalog))
    kms_key = args['KMS_KEY']
    # kms_key = "arn:aws:kms:us-east-1:921232273325:key/3e58ba61-39c5-496f-9f09-b01c7336b54b"
    print('kms_key:{}'.format(kms_key))
    
    ## Processing the files
    process_files (sourceLocations, outputLocation, kms_key, silverCatalog)




