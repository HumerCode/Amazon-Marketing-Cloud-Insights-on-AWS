import json
import boto3
import sys

dynamodb = boto3.client('dynamodb')

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
        table_name = sys.argv[1]
        if table_is_empty(table_name):
            # Read the JSON file
            with open('wfm-ats-dlhs-AmcWorkflowLibrary.json') as json_data:
                items = json.load(json_data)
                # Loop through the JSON objects
                for item in items:
                    dynamodb.put_item(TableName=table_name, Item=dict(items.get(item)))
    except IndexError:
        print('DynamoDb Table name not defined')
        