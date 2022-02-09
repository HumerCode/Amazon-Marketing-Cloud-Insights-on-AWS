import boto3
import json

s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')
kms_client = boto3.client('kms')
sqs_client = boto3.client("sqs")
lambda_client = boto3.client("lambda")
events_client = boto3.client('events')
cfn_client = boto3.client('cloudformation')

def empty_bucket(bucket_name):
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in response:
        for item in response['Contents']:
            print('deleting file', item['Key'])
            s3_client.delete_object(Bucket=bucket_name, Key=item['Key'])
            while response['KeyCount'] == 1000:
                response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                StartAfter=response['Contents'][0]['Key'],
                )
                for item in response['Contents']:
                    print('deleting file', item['Key'])
                    s3_client.delete_object(Bucket=bucket_name, Key=item['Key'])
    
    return
    
def delete_bucket(bucket_name):
    response = s3_client.delete_bucket(Bucket=bucket_name)
    return

def delete_table(table_name):
    response = dynamodb_client.delete_table(
        TableName=table_name
    )
    return

def schedule_key_deletion(key_id):
    response = kms_client.describe_key(
        KeyId=key_id
    )
    if response["KeyMetadata"]["KeyState"] not in ["Disabled","PendingDeletion", "Unavailable"]:
        response = kms_client.schedule_key_deletion(
            KeyId=key_id,
            PendingWindowInDays=7
        )
        print(f"Key Deletion Scheduled: {key_id}")  
    else:
        state = response["KeyMetadata"]["KeyState"]
        print(f"Skipping because Key is in state: {state}")
    return

def delete_queue(queue_url):
    sqs_client.delete_queue(
        QueueUrl=queue_url
    )
    return

def delete_lambda_layer(layer_name):
    lambda_client.delete_layer_version(
        LayerName=layer_name,
        VersionNumber=1
    )
    return

def delete_rule(rule_name):
    targets = []
    response = events_client.list_targets_by_rule(
        Rule=rule_name
    )
    for target in response["Targets"]:
        targets.append(target["Id"])

    events_client.remove_targets(
        Rule=rule_name,
        Ids=targets
    )

    events_client.delete_rule(
        Name=rule_name
    )
    return

def delete_cfn_stack(stack_name):
    cfn_client.delete_stack(
        StackName=stack_name
    )
    return

if __name__ == "__main__":
    try: 
        with open('delete_file.json') as json_data:
            items = json.load(json_data)

            if len(items["s3"]) > 0:
                for s3_bucket in items["s3"]:
                    print(f"Emptying Content From: {s3_bucket}")
                    empty_bucket(s3_bucket)
                    print(f"Bucket: {s3_bucket} is Empty")
            
                for s3_bucket in items["s3"]:
                    print(f"Deleting Bucket: {s3_bucket}")
                    delete_bucket(s3_bucket)
                    print(f"Bucket: {s3_bucket} Deleted")

            if len(items["ddb"]) > 0:
                for ddb_table in items["ddb"]:
                    print(f"Deleting Table: {ddb_table}")
                    delete_table(ddb_table)
                    print(f"Table Name: {ddb_table} Deleted")

            if len(items["sqs"]) > 0:
                for queue_url in items["sqs"]:
                    print(f"Deleting SQS Queue: {queue_url}")
                    delete_queue(queue_url)
                    print(f"Queue Deleted: {queue_url}")
            
            if len(items["lambdaLayer"]) > 0:
                for layer in items["lambdaLayer"]:
                    print(f"Deleting Lambda Layer: {layer}")
                    delete_lambda_layer(layer)
                    print(f"Lambda Layer Deleted: {layer}")

            if len(items["eventbridge"]) > 0:
                for rule in items["eventbridge"]:
                    print(f"Deleting Eventbridge Rule: {rule}")
                    delete_rule(rule)
                    print(f"Eventbridge Rule Deleted: {rule}")
            
            if len(items["cloudformation"]) > 0:
                for stack in items["cloudformation"]:
                    print(f"Deleting Cloudformation Stack: {stack}")
                    delete_cfn_stack(stack)
                    print(f"Cloudformation Stack Deleted: {stack}")

            if len(items["kms"]) > 0:
                for key_id in items["kms"]:
                    print(f"Scheduling KMS Key Delete: {key_id}")
                    schedule_key_deletion(key_id)
            
            json_data.close()

    except Exception as e:
        print(f"Error: {e}")


