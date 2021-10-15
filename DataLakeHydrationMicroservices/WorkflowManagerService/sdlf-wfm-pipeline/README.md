# Workflow Management Service

The Workflow Management Service is an Amazon Ad Tech solution that adds functionality for managing Amazon Marketing Cloud workflows.

**Purpose**
* The Purpose of the Workflow Management Service is to allow AMC users to automate the creation, scheduling, and execution of AMC workflows without having to develop a custom solution. Below is a list of the features of the service.

**Integrates with the Tenant Provisioning Service**
* Adding or updating a customer record to the Tenant Provisioning Service will trigger automated deployment of the SQS queues, IAM policies, workflows and workflow schedules in WFM for the customer's AMC instance. 

**Integrates with the Workflow Library Service**
* The Workflow Management Service can synchronize workflows and workflow schedules in the Workflow Library service with multiple AMC Instances.
    
**Creates a buffer between API Execution Requests and the AMC API**
* Within WFM, Execution requests are sent to an SQS queue rather than directly to the AMC endpoint. It prevents timeout failures when there are large number of requests executed in a short period of time.
    
**Enables dynamic time windows for executions**
* AMC's scheduling feature only allows predefined scheduled reporting such as Daily or Weekly. WFM Allows workflows to be scheduled with dynamic relative time windows.
    
**Tracks AMC Workflow executions**
* WFM tracks the status of all workflow executions for customer AMC instances whether they are submitted through WFM or other means (postman, etc.). Having the status synced to DynamoDB allows events to be triggered or notifications to be sent when executions change state. This table can also be used to track historical executions for troubleshooting or performance monitoring.
    
**Contents:**

* [Reference Architecture](#reference-architecture)
* [Prerequisites](#prerequisites)
* [AWS Service Requirements](#aws-service-requirements)
* [Resources Deployed](#resources-deployed)
* [Deploying the CICD pipelines](#deploying-the-cicd-pipelines)
* [Parameters](#parameters)

## Reference Architecture

![Alt](wfm_pipeline.png)

## Prerequisites
1. The below resources are deployed as part of the SDLF (Serverless Datalake Package) package but it can deployed manually to only use the TPS MicroService. Refer the SDLF (Serverless Datalake Package) package deployment steps.
      1. Parameter Store /SDLF/KMS/KeyArn
      2. Parameter Store /SDLF/Lambda/${pTeamName}/PowerTools
      3. Parameter Store /SDLF/S3/CFNBucket
      4. Parameter Store /SDLF/Misc/pEnv
      5. Parameter Store /SDLF/S3/AnalyticsBucket
      6. Parameter Store /SDLF/S3/AthenaBucket
      7. Parameter Store /SDLF/S3/StageBucket
      8. Parameter Store /SDLF/Glue/${pTeamName}/${pDatasetName}/StageDataCatalog}
      9. Parameter Store /SDLF/CodeBuild/${TEAM}/BuildDeployDatalakeLibraryLayer 
      10. Parameter Store /SDLF/IAM/${TEAM}/CloudWatchRepositoryTriggerRoleArn
      11. Parameter Store /SDLF/IAM/${TEAM}/CodePipelineRoleArn
2. AWS Account
3. Deployed the CICD pipelines.
4. IDE for  e.g. [Pycham](https://www.jetbrains.com/pycharm/) or [AWS Cloud9](https://aws.amazon.com/cloud9/)
5. [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)

## AWS Service Requirements

The following AWS services are required for this utility:

1. [AWS Lambda](https://aws.amazon.com/lambda/)
2. [S3](https://aws.amazon.com/s3/)
3. [Amazon Cloudwatch](https://aws.amazon.com/cloudwatch/)
4. [Amazon DynamoDB](https://aws.amazon.com/dynamodb/)
5. [AWS Identity and Access Management (IAM)](https://aws.amazon.com/iam/)
6. [Amazon Simple Queue Service](https://aws.amazon.com/sqs/)
7. [Amazon Simple Notification Service](https://aws.amazon.com/sns/)
8. [AWS Key Management Service (KMS)](https://aws.amazon.com/kms/)

## Resources Deployed

This template deploys the following resources: 
1. Deploys the CloudFormation stack for WFM, as defined in the template.yaml of the repository.

## Deploying the CICD pipelines

This can be done while deploying the WFM initialization package. It executes the ./deploy-init.sh and deploys the below resources. 
1. Codepipeline - Deploys the CloudFormation stack for WFM, as defined in the template.yaml of the repository. 
2. CloudWatch Rules — Triggers the pipeline upon changes to the CodeCommit repositories containing the code.

### Parameters

1. `pMicroserviceName` [***REQUIRED***] — Name of the microservice.
2. `pPipelineName` [***REQUIRED***] — Name to give the pipeline being deployed.
3. `pTeamName` [***REQUIRED***] — The name of the team which owns the pipeline.
4. `pEnvironment` [***REQUIRED***] — The name of the branch which is configured while deploying the CICD pipeline

The required parameters MUST be defined in a parameters-$ENV.json file at the same level as deploy.sh. The file should look like the following:

      {
            "ParameterKey": "pMicroserviceName",
            "ParameterValue": "wfm"
      },
      {
            "ParameterKey": "pPipelineName",
            "ParameterValue": "dlhs"
      },
      {
            "ParameterKey": "pTeamName",
            "ParameterValue": "<teamname>"
      },
      {
            "ParameterKey": "pEnvironment",
            "ParameterValue": "dev"
      }

A tags.json file in the same directory can also be amended to tag all resources deployed by CloudFormation.

### Deployment

Run this command from the root directory:

      ./deploy.sh

***Important**: Note that the arguments to the script can be left out and defaults will be used. The script works for both creating, as well as updating, CloudFormation stacks. Please ensure that the parameters defined in parameters-$ENV.json are correct.

      The arguments are as follows: 
      1. s — Name of the S3 bucket used for uploading any CloudFormation artifacts to. 
      2. p — AWS profile to use, as listed in ~/.aws/credentials. Defaults to default. 
      3. h — Displays a help message.

Wait for the CloudFormation stack to complete the deployment of all the infrastructure before proceeding further.