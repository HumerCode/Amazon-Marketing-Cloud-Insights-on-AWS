import json
import boto3
import sys

from boto3 import Session

region = str(sys.argv[2])
profile_name = str(sys.argv[3])

if profile_name != "default":
    session = boto3.session.Session(profile_name=profile_name)
    dynamodb = session.client('dynamodb', region_name=region)
else:
    dynamodb = boto3.client('dynamodb', region_name=region)

def table_is_empty(table_name):
   
    empty_check = False
    response = dynamodb.scan(TableName=table_name, Limit=1)
    if len(response['Items'])>0:
        empty_check = False
    else:
        empty_check = True
    
    return empty_check

            
if __name__ == "__main__":
    try: 
        table_name = str(sys.argv[1])
        if table_is_empty(table_name):
            # Read the JSON file
            with open('wfm-demoteam-dlhs-AmcWorkflowLibrary.json') as json_data:
                items = json.load(json_data)
                # Loop through the JSON objects
                for item in items:
                    dynamodb.put_item(TableName=table_name, Item=dict(items.get(item)))
    except IndexError:
        print('DynamoDb Table name not defined')
        