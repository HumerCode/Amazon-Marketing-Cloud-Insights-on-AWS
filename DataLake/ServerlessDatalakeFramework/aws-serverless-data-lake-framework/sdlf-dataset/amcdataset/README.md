# README 
This repository contains the code that enables the deployment of a new dataset.
 
## Prerequisites
Before attempting to deploy a dataset, ensure that both the **CORE** and **PIPELINE** infrastructure has completed its provisioning, and has been successfully deployed. They contain infrastructure resources that the **DATASET**  stack depends on, and without them in place, the deployment of this stack will fail. These resources include:
1. IAM roles
2. The Glue data catalog
3. CodeBuild jobs
4. S3 buckets

## Deploying a dataset

### Parameters
Before deploying a new dataset, ensure that the `parameters-$ENV.json` file contains the following parameters that **DATASET** requires:

1. `pTeamName` [***REQUIRED***] — Name of the team as defined in `common-team` that will be owning this dataset.
2. `pPipelineName` [***REQUIRED***] — Name of the pipeline as defined in `common-pipeline` that will be processing this dataset.
3. `pDatasetName` [***REQUIRED***] — Name to give the dataset being deployed. Pick a name that refers to a group of data with a common schema or datasource (e.g. telemetry, orders...). This name will be used as an S3 prefix.

The required parameters MUST be defined in a `parameters-$ENV.json` file at the same level as `deploy.sh`. The file should look like the following:

    [
        {
            "ParameterKey": "pTeamName",
            "ParameterValue": "<teamName>"
        },
        {
            "ParameterKey": "pPipelineName",
            "ParameterValue": "<pipelineName>"
        },
        {
            "ParameterKey": "pDatasetName",
            "ParameterValue": "<datasetName>"
        }
    ]

A tags.json file in the same directory can also be amended to tag all resources deployed by CloudFormation.

### Deployment
Run this command from the root directory:

    ./deploy.sh -n <DATASET_NAME> -s <S3_BUCKET_NAME> -p <AWS_CREDENTIAL_PROFILE>

For example, the command in the next line creates a CloudFormation stack named `<pTeamName>-legislators` using the credentials defined in `~/.aws/credentials` with the profile name `my-aws-profile`. Any CloudFormation artifacts are uploaded to the `my-dl-bucket`:

    ./deploy.sh -n legislators -s my-dl-bucket -p my-aws-profile

***Important**: Note that the arguments to the script can be left out and defaults will be used. The script works for both creating, as well as updating, CloudFormation stacks. Please ensure that the parameters defined in `parameters-$ENV.json` are correct.

The arguments are as follows:
1. `n` — Name of the dataset which is used in combination with the team name as the CloudFormation stack name.
2. `s` — Name of the S3 bucket used for uploading any CloudFormation artifacts to.
3. `p` — AWS profile to use, as listed in `~/.aws/credentials`. Defaults to `default`. 
4. `h` — Displays a help message.
   
Wait for the CloudFormation stack to complete the deployment of all the infrastructure before proceeding further.

## Architecture
This template creates the following resources:
1. SQS Queues - Hold dataset specific messages awaiting to be processed in the next stage
   1. `QueuePostStepRouting`
   2. `DeadLetterQueuePostStepRouting`
2. CloudWatch Rules - Triggers the the next stage pipeline every X minutes to process the dataset messages from the queue
3. Glue crawler

## Troubleshooting  
#### 1. Validation error detected: Value '`<name>`' at 'targets.1.member.id' failed to satisfy constraint. 
Caused by the limits placed on the length of names. To fix, ensure that the parameters given, `pOrganizationName`, `pApplicationName`, `pTeamName` and `pDatasetName`, follow the `[a-z0-9]` regex pattern, and are less than 10-12 charaters long each.