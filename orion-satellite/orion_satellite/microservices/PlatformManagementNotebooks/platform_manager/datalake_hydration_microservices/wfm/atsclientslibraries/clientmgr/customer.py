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
def get_customers_config (ATS_TEAM_NAME):
    dynamodb_client_rd= boto3.client('dynamodb')
    dynamodb_resp_rd = dump_table(table_name=f'tps-{ATS_TEAM_NAME}-CustomerConfig-dev', dynamodb_client_rd=dynamodb_client_rd)
    cust_dtls_list =[]
    for itm in dynamodb_resp_rd:
        itm_dict = deserializeDyanmoDBItem(itm)
    #     print (itm_dict)
        tbl_dict = {
            "customer_id":itm_dict.get("customerId",None),
            "customer_name":itm_dict.get("customerName",None),
            "customer_type": itm_dict.get("endemicType",None),
            "amc_region": itm_dict.get("AMC",{}).get("amcRegion",None),
            "amc_sdlf_datset_name":itm_dict.get("AMC",{}).get("amcDatasetName",None),
            "amc_endpoint_url":itm_dict.get("AMC",{}).get("amcApiEndpoint",None),
            "amc_aws_orange_account_id":itm_dict.get("AMC",{}).get("amcOrangeAwsAccount",None),
            "amc_aws_green_account_id":itm_dict.get("AMC",{}).get("amcGreenAwsAccount",None),
            "amc_bucket_name":itm_dict.get("AMC",{}).get("amcS3BucketName",None),
            "amc_advertiser_ids":itm_dict.get("AMC",{}).get("amcAdvertiserIds",None),
            "amc_entity_ids":itm_dict.get("AMC",{}).get("amcEntityIds",None),
            "amc_team_name":itm_dict.get("AMC",{}).get("amcTeamName",None),
            "sas_credentials_arn":itm_dict.get("SAS",{}).get("sasCredentails",None),
            "sas_sdlf_datset_name":itm_dict.get("SAS",{}).get("sasDatasetName",None),
            "sas_team_name":itm_dict.get("SAS",{}).get("sasTeamName",None),
            "sas_profile_details":itm_dict.get("SAS",{}).get("sasProfileDetails",None),
            "sas_api_base_url":itm_dict.get("SAS",{}).get("sasBaseApiUrl",None)
        }
    #     print (tbl_dict)
        cust_dtls_list.append(tbl_dict)
    # print (cust_dtls_list)
    df = pd.DataFrame(cust_dtls_list)
    return df


def set_customers_config (customer_details, ATS_TEAM_NAME):
    dynamodb_client_wr= boto3.resource('dynamodb')

    cust_details = {
        "customerId":customer_details['customer_id'],
        "customerPrefix":customer_details['customer_id'],
        "customerName":customer_details['customer_name'],
        "endemicType":customer_details['customer_type'],
        "AMC":{
            "amcOrangeAwsAccount":customer_details.get("amc",{}).get("aws_orange_account_id",None),
            "amcGreenAwsAccount":customer_details.get("amc",{}).get("aws_green_account_id",None),
            "amcS3BucketName":customer_details.get("amc",{}).get("bucket_name",None),
            "amcDatasetName":customer_details.get("amc",{}).get("sdlf_datset_name",None),
            "amcApiEndpoint":customer_details.get("amc",{}).get("endpoint_url",None),
            "amcAdvertiserIds":customer_details.get("amc",{}).get("advertiser_ids",None),
            "amcEntityIds":customer_details.get("amc",{}).get("entity_ids",None),
            "amcTeamName":ATS_TEAM_NAME,
            "amcRegion":customer_details['region']
        },
        "SAS":{
            "sasCredentails":customer_details.get("sas",{}).get("credential_arn",None),
            "sasDatasetName":customer_details.get("sas",{}).get("sdlf_datset_name",None),
            "sasProfileDetails":customer_details.get("sas",{}).get("profile_details",None),
            "sasTeamName":ATS_TEAM_NAME,
            "sasBaseApiUrl":customer_details.get("sas",{}).get("api_base_url",None)
        }
    }
    
    customer_table = dynamodb_client_wr.Table(f'tps-{ATS_TEAM_NAME}-CustomerConfig-dev')
    dynamodb_resp_wr = customer_table.put_item(Item=cust_details)
    return dynamodb_resp_wr