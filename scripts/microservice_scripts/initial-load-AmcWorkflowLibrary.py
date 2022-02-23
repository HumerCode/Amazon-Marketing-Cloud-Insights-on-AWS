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


import json
import boto3
import sys

from boto3 import Session
import os

region = str(sys.argv[1])
profile_name = str(sys.argv[2])
env = str(sys.argv[3])

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
        # Get Name of Table from DDK.json
        config_path = os.path.abspath(__file__).split("/")[:-3]
        config_path = "/".join(config_path) + "/ddk.json"

        with open(config_path) as config_data:
            config = json.loads(config_data.read())
            team = config["environments"][env]["wfm_parameters"]["team"]
            pipeline =  config["environments"][env]["wfm_parameters"]["pipeline"]
            table_name = "-".join(["wfm", team, pipeline, "AMCWorkflowLibrary"])

        if table_is_empty(table_name):
            # Read the JSON file
            with open('wfm-AmcWorkflowLibrary.json') as json_data:
                items = json.load(json_data)
                # Loop through the JSON objects
                for item in items:
                    dynamodb.put_item(TableName=table_name, Item=dict(items.get(item)))
    except IndexError:
        print('DynamoDb Table name not defined')
        