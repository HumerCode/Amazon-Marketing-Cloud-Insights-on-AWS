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
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
import pandas as pd
# pd.set_option('max_columns', None)
# pd.set_option('max_colwidth', None)
# pd.set_option('max_rows', None)

## DynamoDB serialzation
def deserializeDyanmoDBItem(item):
    return {k: TypeDeserializer().deserialize(value=v) for k, v in item.items()}

## DynamoDB scan with pagination
def dump_table(table_name, dynamodb_client_rd):
    results = []
    last_evaluated_key = None
    while True:
        if last_evaluated_key:
            response = dynamodb_client_rd.scan(
                TableName=table_name,
                ExclusiveStartKey=last_evaluated_key
            )
        else: 
            response = dynamodb_client_rd.scan(TableName=table_name)
        last_evaluated_key = response.get('LastEvaluatedKey')
        
        results.extend(response['Items'])
        
        if not last_evaluated_key:
            break
    return results

## retirev custoemr config table details
def get_customers_config (TEAM_NAME, ENV):
    dynamodb_client_rd= boto3.client('dynamodb')
    dynamodb_resp_rd = dump_table(table_name=f'tps-{TEAM_NAME}-CustomerConfig-{ENV}', dynamodb_client_rd=dynamodb_client_rd)
    cust_dtls_list =[]
    for itm in dynamodb_resp_rd:
        itm_dict = deserializeDyanmoDBItem(itm)
    #     print (itm_dict)
        tbl_dict = {
            "customer_id":itm_dict.get("customerId",None),
            "customer_name":itm_dict.get("customerName",None),
            "customer_type": itm_dict.get("endemicType",None),
            "amc_region": itm_dict.get("AMC",{}).get("amcRegion",None),
            "amc_datset_name":itm_dict.get("AMC",{}).get("amcDatasetName",None),
            "amc_endpoint_url":itm_dict.get("AMC",{}).get("amcApiEndpoint",None),
            "amc_aws_orange_account_id":itm_dict.get("AMC",{}).get("amcOrangeAwsAccount",None),
            "amc_aws_green_account_id":itm_dict.get("AMC",{}).get("amcGreenAwsAccount",None),
            "amc_bucket_name":itm_dict.get("AMC",{}).get("amcS3BucketName",None),
            "amc_team_name":itm_dict.get("AMC",{}).get("amcTeamName",None)
        }
    #     print (tbl_dict)
        cust_dtls_list.append(tbl_dict)
    # print (cust_dtls_list)
    df = pd.DataFrame(cust_dtls_list)
    return df


def set_customers_config (customer_details, TEAM_NAME, ENV):
    dynamodb_client_wr= boto3.resource('dynamodb')

    cust_details = {
        "customerId":customer_details['customer_id'],
        "customerPrefix":customer_details['customer_id'],
        "customerName":customer_details['customer_name'],
        "endemicType":customer_details['customer_type'],
        "AMC":{
            "amcOrangeAwsAccount":customer_details.get("amc",{}).get("aws_orange_account_id",None),
            "amcS3BucketName":customer_details.get("amc",{}).get("bucket_name",None),
            "amcDatasetName":customer_details.get("amc",{}).get("amc_dataset_name",None),
            "amcApiEndpoint":customer_details.get("amc",{}).get("endpoint_url",None),
            "amcTeamName":TEAM_NAME,
            "amcRegion":customer_details['region']
        }
    }
    
    customer_table = dynamodb_client_wr.Table(f'tps-{TEAM_NAME}-CustomerConfig-{ENV}')
    dynamodb_resp_wr = customer_table.put_item(Item=cust_details)
    return dynamodb_resp_wr