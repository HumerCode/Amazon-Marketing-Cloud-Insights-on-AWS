import boto3
import json


def invoke_workflow(workflow, ATS_TEAM_NAME):
    client = boto3.client('lambda')
    item = workflow

    payload = {
        'customerId': item['customerId'],
        'payload': item['Input']['payload']
    }

    response = client.invoke(
        FunctionName=f'wfm-{ATS_TEAM_NAME}-dlhs-ExecutionQueueProducer',
        InvocationType='Event',
        Payload=json.dumps(payload)
    )
    return response
