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
import awswrangler as wr
import boto3
import pandas as pd
import numpy as np
import logging
from awsglue.utils import getResolvedOptions
import io
import re
import unicodedata
from pandas.api.types import is_numeric_dtype, is_string_dtype

# create logger
logging.basicConfig()
logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)


def getBucketAndKeyFromS3Uri(s3Path: str) -> (str, str):
    outputBucket, outputKey = re.match('s3://([^/]*)/(.*)', s3Path).groups()
    return outputBucket, outputKey


def athena_sanitize_name(name: str) -> str:
    name = "".join(c for c in unicodedata.normalize("NFD", name) if unicodedata.category(c) != "Mn")  # strip accents
    return re.sub("[^A-Za-z0-9_]+", "_", name).lower()  # Replacing non alphanumeric characters by underscore


print('boto3 version')
print(boto3.__version__)

glue_client = boto3.client('glue')
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
lf_client = boto3.client('lakeformation')

# This map is used to convert Athena datatypes (in upppercase) to pandas Datatypes
DataTypeMap = {
    "ARRAY": object
    , "BIGINT": np.int64
    , "BINARY": object
    , "BOOLEAN": bool
    , "CHAR": str
    , "DATE": object
    , "DECIMAL": np.float64
    , "DOUBLE": np.float64
    , "FLOAT": np.float64
    , "INTEGER": np.int64
    , "INT": np.int64
    , "MAP": object
    , "SMALLINT": np.int64
    , "STRING": str
    , "STRUCT": object
    , "TIMESTAMP": np.datetime64
    , "TINYINT": np.int64
    , "VARCHAR": str}

pandas_athena_datatypes = {
    "float64": "double",
    "float32": "float",
    "datetime64[ns]": "timestamp",
    "int64": "bigint",
    "int8": "tinyint",
    "int16": "smallint",
    "int32": "int",
    "bool": "boolean"
}

BooleanValueMap = {"false": 0, "False": 0, "FALSE": 0,
                   "true": 1, "True": 1, "TRUE": 1, "-1": 0, -1: 0}

column_datatype_override = {
    ".*_fee[s]*($|_.*)": np.float64,
    "cost[s]*$|.*_cost[s]*($|_.*)|.*_cost[s]*_.*$": np.float64,
    ".*_rate[s]*($|_.*)": np.float64,
    ".*_score[s]*($|_.*)": np.float64,
    ".*_percent[s]*($|_.*)": np.float64,
    ".*_pct[s]*($|_.*)": np.float64,
    "avg($|_.*)|.*_avg[s]*($|_.*)": np.float64,
    "[e]*cpm$|.*_[e]*cpm$|.*_[e]*cpm_.*$": np.float64,
    ".*_id$": str
    # "[e]*cpa$|.*_[e]*cpa$|.*_[e]*cpa_.*$": np.float64,
}

exclude_workflow = ['standard_impressions_by_browser_family', 'standard_impressions-by-browser-family']


def add_partitions(outputfilebasepath, silverCatalog, list_partns, targetTableName):
    partn_values = []
    patn_path_value = outputfilebasepath
    for prtns in list_partns:
        patn_path_value = patn_path_value + prtns["orgcolnm"] + "=" + str(prtns["value"]) + "/"
        partn_values.append(str(prtns["value"]))
    print("Partition S3 Path : " + patn_path_value)
    print(str(partn_values))
    try:
        print("Update partitions")
        wr.catalog.add_parquet_partitions(
            database=silverCatalog,
            table=wr.catalog.sanitize_table_name(targetTableName),
            compression='snappy',
            partitions_values={
                patn_path_value: partn_values
            }
        )
    except Exception as e:
        print("Partition exist No need to update")
        print(str(e))


def get_partition_values(sourceFilepartitionedPath):
    list_partns = []
    cust_hash = ''
    for prtns in sourceFilepartitionedPath.split('/'):
        if '=' in prtns:
            d = {
                "orgcolnm": str(prtns.split("=")[0]),
                "santcolnm": wr.catalog.sanitize_column_name(str(prtns.split("=")[0])),
                "value": str(prtns.split("=")[1])
            }
            list_partns.append(d)
        if 'customer_hash' in prtns:
            cust_hash = str(prtns.split("=")[1])

    return list_partns, cust_hash


# def get_data_types(col_name, panda_data_kind, glue_data_type):
#    default_cast = 'string'
#    if ((panda_data_kind in float_data_types_kinds) and (
#            glue_data_type == None or glue_data_type.upper() not in glue_data_type_excep)):
#        default_cast = 'Float64'
#    else:
#        if glue_data_type != None:
#            default_cast = athena_pandas_datatypes.get(glue_data_type.upper(), 'string')
#    return default_cast

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


def create_update_tbl(csvdf, csv_schema, tbl_schema, silverCatalog, targetTableName, list_partns, outputfilebasepath,
                      table_exist, cust_hash, pandas_athena_datatypes):
    if table_exist == 1:
        extra_cols = list(set(csv_schema.keys()) - set(tbl_schema.keys()))
        print("extra_cols : " + str(extra_cols))

        tbl = glue_client.get_table(
            DatabaseName=silverCatalog,
            Name=wr.catalog.sanitize_table_name(targetTableName)
        )
        print("Existing table")
        print(tbl)

        strg_descrptr = tbl["Table"]["StorageDescriptor"]
        new_cols = []
        if len(extra_cols) > 0:
            print("Adding new columns")
            for col in extra_cols:
                print("New col name : " + col)
                print("New col type : " + pandas_athena_datatypes.get(csv_schema[col].lower(), 'string'))
                col_dict = {}
                col_dict = {
                    'Name': col,
                    'Type': pandas_athena_datatypes.get(csv_schema[col].lower(), 'string')
                }
                new_cols.append(col_dict)

            strg_descrptr["Columns"].extend(new_cols)

            newtbldetails = {
                'Name': wr.catalog.sanitize_table_name(targetTableName),
                'StorageDescriptor': strg_descrptr,
                'PartitionKeys': tbl["Table"]["PartitionKeys"],
                'TableType': tbl["Table"]["TableType"],
                'Parameters': tbl["Table"]["Parameters"]
            }

            print("new table defn")
            print(newtbldetails)

            resp = glue_client.update_table(
                DatabaseName=silverCatalog,
                TableInput=newtbldetails
            )

            print("new table")
            print(resp)
        else:
            print("No change in table")

        cust_hash_tag_dict = {
            'customer_hash': cust_hash
        }
    #        add_tags_lf(cust_hash_tag_dict, silverCatalog, wr.catalog.sanitize_table_name(targetTableName))
    else:
        print("Table does not exist. Creating new table")
        col_dict = {}
        cust_hash_tag_dict = {
            'customer_hash': cust_hash
        }

        for colm in csvdf.columns:
            col_dict[colm] = str(pandas_athena_datatypes.get(str(csvdf.dtypes[colm]).lower(), 'string'))
        part_dict = {}
        for prtns in list_partns:
            part_dict[prtns["santcolnm"]] = 'string'

        print("Create Table")
        print("Column Dictionary : " + str(col_dict))
        print("Partition Dictionary : " + str(part_dict))

        wr.catalog.create_parquet_table(
            database=silverCatalog,
            table=wr.catalog.sanitize_table_name(targetTableName),
            path=outputfilebasepath,
            columns_types=col_dict,
            partitions_types=part_dict,
            compression='snappy',
            parameters=cust_hash_tag_dict
        )


#        add_tags_lf(cust_hash_tag_dict, silverCatalog, wr.catalog.sanitize_table_name(targetTableName))


def process_files(sourceLocations, outputLocation, kms_key, silverCatalog):
    for key in sourceLocations:  # added for batching
        logger.info(f"Processing Key: {key}")  # added for batching
        sourceLocation = key

        sourceBucket, sourceKey = getBucketAndKeyFromS3Uri(sourceLocation)

        s3_resource = boto3.resource('s3')

        s3_client = boto3.client('s3')

        exception = False
        try:
            sourceS3Object = s3_resource.Object(sourceBucket, sourceKey).get()
            logger.info(f"metadata:{sourceS3Object['Metadata']}")
        except:
            exception = True
            pass
        if exception:
            continue
        sourceFilepartitionedPath = sourceS3Object['Metadata']['partitionedpath']
        sourceFileBaseName = sourceS3Object['Metadata']['filebasename']
        sourceFileVersion = sourceS3Object['Metadata']['fileversion']
        sourceFileDataSet = sourceS3Object['Metadata']['keydataset']
        sourceFileTeam = sourceS3Object['Metadata']['keyteam']
        sourceFileScheduleFrequency = sourceS3Object['Metadata']['schedulefrequency']
        sourceFileWorkflowName = sourceS3Object['Metadata']['workflowname']

        # targetTableName = '{}_{}_{}'.format(sourceFileWorkflowName,sourceFileScheduleFrequency,sourceFileVersion)
        targetTableName = sourceFilepartitionedPath.split('/')[0]

        if sourceFileWorkflowName in exclude_workflow:
            continue

        # Read the bytes of the csv file once so we can process it with pandas twice, only reading from S3 once.
        csv_file_data = io.StringIO(sourceS3Object['Body'].read().decode("UTF8").replace('\\"', "'"))

        # create filtered copy of the data that will be used to derive the schema in case there is no filter fields
        only_unfiltered_csv_file_data = csv_file_data

        # reload the csv data forcing string (object) datatypes
        csvdf = pd.read_csv(csv_file_data, header=0, skip_blank_lines=True, escapechar='\\', dtype=np.dtype('O'))

        # you must reset the buffer location to the beginning after using it in a previous read
        csv_file_data.seek(0)

        # Create a boolean flag to track if file has any unfiltered rows
        has_unfiltered_rows = True

        # If the dataset has a column named filtered check to see how many rows are filtered
        if 'filtered' in csvdf.columns:
            csvdf_filtered_rows = csvdf[csvdf.filtered.str.lower() == "true"]
            # Update the dataset to only include non filtered rows
            csvdf_only_unfiltered_rows = csvdf[csvdf.filtered.str.lower() != "true"]

            # create an out buffer to capture the CSV file output from the to_csv method
            csv_only_unfiltered_out_buffer = io.BytesIO()

            # write the filtered record set as a CSV into the buffer
            csvdf_only_unfiltered_rows.to_csv(csv_only_unfiltered_out_buffer)

            # Reset the buffer location after it is written to
            csv_only_unfiltered_out_buffer.seek(0)

            # create copy of the raw string data with only non filtered rows
            only_unfiltered_csv_file_data = io.StringIO(csv_only_unfiltered_out_buffer.read().decode("UTF8"))

            # reset the string buffer position after it has been written to
            only_unfiltered_csv_file_data.seek(0)

            # close the bytes buffer
            csv_only_unfiltered_out_buffer.close()

            # Log the number of filtered and unfiltered rows
            logger.info(
                f"input data had {csvdf_filtered_rows.shape[0]} filtered rows and {csvdf_only_unfiltered_rows.shape[0]} unfiltered rows")

            # has_unfiltered_rows will be true if there is at least 1 row left in the df after filtered rows are removed
            has_unfiltered_rows = csvdf_only_unfiltered_rows.shape[0] > 0

        if not has_unfiltered_rows:
            logger.info(f'There were no non-filtered rows in the data file, skipping file {key}')
            continue

        # Only use unfiltered text rows string buffer to derive the schema to try to get more accurate data types
        df_derived_schema = pd.read_csv(only_unfiltered_csv_file_data, header=0, skip_blank_lines=True, escapechar='\\')

        # close the buffers as we are now done with them
        only_unfiltered_csv_file_data.close()
        csv_file_data.close()

        # create a dictionary with the column name as the key and the datatype as the value
        derived_schema = dict(zip([*df_derived_schema.columns], [*df_derived_schema.dtypes]))

        # create a filtered copy of the dictionary only containing non string (object) dtypes columns that will need
        # to be cast
        only_nonstring_schema = dict(filter(lambda elem: elem[1] != np.dtype('O'), derived_schema.items()))

        logger.info(f"only_nonstring_schema : {only_nonstring_schema}")

        # create a filtered copy of the dictionary only containing non string (object) dtypes columns that will need
        # to be cast
        only_string_schema = dict(filter(lambda elem: is_string_dtype(elem[1]), derived_schema.items()))

        logger.info(f"only_string_schema : {only_string_schema}")

        # Iterate over the text only columns
        for column in csvdf.columns:

            # Check to see if the column matched an override suffix to force a datatype rather than deriving it
            override_matched = False
            for regex_expression_key in column_datatype_override:
                regex_match = re.match(regex_expression_key, column, re.IGNORECASE)
                if regex_match:
                    override_datatype = column_datatype_override[regex_expression_key]
                    if is_numeric_dtype(override_datatype):
                        csvdf[column].fillna(-1, inplace=True)
                    # if is_string_dtype(override_datatype):
                    # csvdf[column].fillna('', inplace=True)
                    csvdf[column] = csvdf[column].astype(override_datatype)
                    logger.info(
                        f"column {column} matched override regex expression {regex_expression_key} and was casted to {override_datatype}")
                    override_matched = True
            if override_matched:
                continue

            # If the derived types for the ext column is not a nonstring then fill na with blank string (rather than -1)
            if column in only_string_schema:
                # Fill NA values for string type columns with empty strings
                # csvdf[column].fillna('', inplace=True)
                # Explicitly cast string type columns as string
                csvdf[column] = csvdf[column].astype(str)
                continue
            # if the column is derived as a nonstring then we need to try to cast it to the appropriate type with rules
            if column in only_nonstring_schema:
                derived_datatype_name = only_nonstring_schema[column]
                # since we know the column is not a string, fill blanks with -1
                csvdf[column].fillna(-1, inplace=True)

                # if the nonstring column is boolean than map to 0 or 1 values before casting to boolean to ensure that
                # false, FALSE, and False end up as 0
                if only_nonstring_schema[column] == bool:
                    try:
                        logger.info(f"column {column} is derived as {derived_datatype_name}, "
                                    f"performing boolean mapping and casting")
                        csvdf[column] = csvdf[column].map(BooleanValueMap).astype('bool')
                        continue
                    except (TypeError, ValueError, KeyError) as e:
                        logger.info(f'could not cast {column} as {derived_datatype_name} : {e}')

                # Check to see if the derived datatype is numeric
                if is_numeric_dtype(only_nonstring_schema[column]):
                    # Convert any derived number columns to Int64 if possible
                    try:
                        csvdf[column] = csvdf[column].astype('int64')
                        logger.info(f'casted {column} derived as {derived_datatype_name} to int64')
                        # If we cast successfully then go to the next column
                        continue
                    except (TypeError, ValueError) as e:
                        # Log if we are unable to cast to an int64 then note it in the log
                        logger.info(
                            f'could not cast {column} derived as {derived_datatype_name} to int64: {str(e)}')

                # Attempt to cast the text to the derived datatype
                try:
                    csvdf[column] = csvdf[column].astype(only_nonstring_schema[column])
                    logger.info(f'casted derived {column} as {derived_datatype_name}')
                except (TypeError, ValueError) as e:
                    logger.info(f'could not cast {column} as {derived_datatype_name} : {e}')

        s3OutputPath = f'{outputLocation}/{sourceFilepartitionedPath}/{sourceFileBaseName}.parquet'

        # create a dictioary that contains the CSV file's casted schema
        csvSchema = dict(zip([*csvdf.columns], [*csvdf.dtypes]))

        # Try to read the schema from the destination table (if it exists) and convert the CSV inferrred schema to
        # match the table schema
        table_exist = 1
        try:
            glueClient = boto3.client('glue')
            tableSchema = {}
            # getTableResult = glueClient.get_table(DatabaseName=silverCatalog,Name=targetTableName)
            getTableResult = glueClient.get_table(DatabaseName=silverCatalog,
                                                  Name=athena_sanitize_name(targetTableName))
            logger.info(f"getting schema for table {silverCatalog}.{athena_sanitize_name(targetTableName)}")

            for tableColumn in getTableResult['Table']['StorageDescriptor']['Columns']:
                tableSchema[tableColumn['Name']] = tableColumn['Type']
                logger.info(f"table schema : {tableColumn['Name']} : {tableColumn['Type']}")

            for tableColumn in getTableResult['Table']['StorageDescriptor']['Columns']:
                tableSchema[tableColumn['Name']] = tableColumn['Type']

            # copy the old csv schema to start the new schema
            newSchema = csvSchema.copy()

            # if the csv schema column matches an existing table schema column, update the csv schema to match match
            # the table's schema
            for column in newSchema:
                if column in tableSchema:
                    # look up the datatype from the table in our DataTypeMap (convert the datatype name to uppercase
                    # first for the lookup matching)
                    newSchema[column] = DataTypeMap[tableSchema[column].upper()]

            logger.info(f'csvSchema:{csvSchema}')
            logger.info(f'newSchema:{newSchema}')

            # convert the CSV dataframe Schema to the new schema that was read from the glue table (if it exists)
            for c in csvdf.columns:
                if csvdf[c].dtype != newSchema[c]:
                    logger.info(
                        f'{c} datatype in file {csvdf[c].dtype} does not match datatype in table {newSchema[c]}')
                    try:
                        if newSchema[c] == np.int64:
                            csvdf[c].fillna(-1, inplace=True)
                            csvdf[c].replace('nan', -1, inplace=True)
                        csvdf[c] = csvdf[c].astype(newSchema[c])
                        logger.info(f'casted {c} as {newSchema[c]} to match table')
                        print(f"dtype:{csvdf[c].dtype}")
                        print(f"value is:{csvdf[c]}")
                    except (TypeError, ValueError) as e:
                        logger.info(f'could not cast {c} to {newSchema[c]} to match table: {str(e)}')

        # Catch the exception if the table does not exist and apply the generic logic to try to handle schema
        # conversions
        except glueClient.exceptions.EntityNotFoundException as e:
            table_exist = 0
            logger.info(
                f'Caught exception, destination table {silverCatalog}.{targetTableName} does not exist, attempting to '
                f'cast all numbers to Int64 if possible')

            # If there are blanks in the data integers will be cast to floats which causes inconsistent parquet schema
            # Convert any numbers to Int64 (as opposed to int64) since Int64 can handle nulls
            for c in csvdf.select_dtypes(np.number).columns:
                try:
                    csvdf[c] = csvdf[c].astype('Int64')
                    logger.info(f'casted {c} as Int64')
                except (TypeError, ValueError) as e:
                    logger.info(f'could not cast {c} to Int64: {str(e)}')

        logger.info(f'Converted Schema: {csvdf.dtypes}\n')
        logger.info(f'{len(csvdf)} records')

        # csvdf.fillna(csvdf.dtypes.replace({'float64': -1.0, 'object': 'FILTERED', 'Int64': -1, 'int64': -1}), inplace=True)

        # write the parquet file using the kms key
        # Note: if writing to parquet and not as a dataset must specify entire path name.
        out_buffer = io.BytesIO()
        # print(csvdf.head())
        # print(csvdf.info(verbose=True))
        csvdf.to_parquet(out_buffer, index=False, compression='snappy')

        # wr.s3.to_parquet(df=csvdf, path=s3OutputPath, compression='snappy',
        #                 s3_additional_kwargs={
        #                     'ServerSideEncryption': 'aws:kms',
        #                     'SSEKMSKeyId': kms_key})

        outputBucket, outputKey = getBucketAndKeyFromS3Uri(s3OutputPath)

        s3_client.put_object(Bucket=outputBucket, Key=outputKey, Body=out_buffer.getvalue(),
                             ServerSideEncryption='aws:kms', SSEKMSKeyId=kms_key)

        logger.info(f'Successfully wrote output file to {s3OutputPath}')

        csv_schema = {}
        for colm in csvdf.columns:
            csv_schema[colm] = str(csvdf.dtypes[colm])
        print("Final CSV schema : " + str(csv_schema))
        print("Table Schema: " + str(tableSchema))

        # get partition values
        list_partns = []
        cust_hash = ''
        list_partns, cust_hash = get_partition_values(sourceFilepartitionedPath)
        print("Partitions values : " + str(list_partns))

        outputfilebasepath = '{}/{}/'.format(outputLocation, targetTableName)

        # Create or update table
        create_update_tbl(csvdf, csv_schema, tableSchema, silverCatalog, targetTableName, list_partns,
                          outputfilebasepath, table_exist, cust_hash, pandas_athena_datatypes)

        # add partitions
        add_partitions(outputfilebasepath, silverCatalog, list_partns, targetTableName)


if __name__ == '__main__':
    args = getResolvedOptions(
        sys.argv,
        ['JOB_NAME', 'SOURCE_LOCATION', 'SOURCE_LOCATIONS', 'OUTPUT_LOCATION', 'SILVER_CATALOG', 'GOLD_CATALOG',
         'KMS_KEY'])

    jobName = args['JOB_NAME']
    sourceLocation = args['SOURCE_LOCATION']
    sourceLocations = args['SOURCE_LOCATIONS']
    sourceLocations = sourceLocations.split(',')
    outputLocation = args['OUTPUT_LOCATION']
    silverCatalog = args['SILVER_CATALOG']
    goldCatalog = args['GOLD_CATALOG']
    kms_key = args['KMS_KEY']

    ## Processing the files
    process_files(sourceLocations, outputLocation, kms_key, silverCatalog)
