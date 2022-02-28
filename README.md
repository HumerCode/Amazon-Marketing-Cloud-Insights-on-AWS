# Deployment Steps for AMC QuickStart

#

## Prerequisites for the Deployment

To complete this deployment, you'll need the following in your local environment

Programmatic access to an AWS Account
Python (version 3.7 or above) and its package manager, pip (version 9.0.3 or above), are required

```
$ python --version
$ pip --version
```

The AWS CLI installed and configured

```
$ aws --version
```

The AWS CDK CLI (version 2.10 and above) installed

```
$ cdk --version
```

The Git CLI installed and configured

```
$ git --version
```

If this is your first time using Git, set your git username and email by running:

```
$ git config --global user.name "YOUR NAME"
$ git config --global user.email "YOU@EMAIL.COM"
```

You can verify your git configuration with

```
$ git config --list
```

The jq Linux command line utility to extract data from JSON documents.

```
$ jq --version
```

If not installed already, you can install jq by running in your command line:

```
$ sudo yum install jq
```

### [OPTIONAL] Using AWS Cloud9 for Deployment:

If you would like to deploy this quickstart using an AWS Cloud9 Environment rather than on your local environment, follow these steps to set up AWS Cloud9:

1. Log in to the AWS account console using the Admin role and select an AWS region. We recommend choosing a mature region where most services are available (e.g. eu-west-1, us-east-1…)
2. Navigate to `Cloud9` in the AWS console. Set up a [Cloud9 Environment](https://docs.aws.amazon.com/cloud9/latest/user-guide/create-environment-main.html) in the same AWS region (t3.small or higher, Amazon Linux 2) and open the IDE
3. Download the package, upload it to your Cloud9 instance, and unzip it
4. Install jq by running in your command line:

```
$ sudo yum install jq
```

Python, Pip, AWS CLI, AWS CDK CLI, and Git CLI packages should all be installed and configured for you by defualt in your Cloud9 environment. Ensure that these pacakges are installed with the correct version with the following commands:

```
$ python --version
$ pip --version
$ aws --version
$ cdk --version
$ git --version
```

The version requirements for the packages installed are:
Python (version 3.7 or above)
pip (version 9.0.3 or above)
AWS CDK CLI (version 2.10 and above)

Continue to the [Initial setup with the DDK CLI](#Initial-setup-with-the-DDK-CLI) Section in order to deploy this QuickStart.

#

## Initial setup with the DDK CLI

Clone the repository for AMC QuickStart

```
$ git clone GITHUB-PATH
$ cd amc_quickstart
```

Install AWS DDK CLI, a command line interface to manage your DDK apps

```
$ pip install aws-ddk
```

To verify the installation, run:

```
$ ddk --help
```

Create and actitavte a virtualenv

```
$ python -m venv .venv && source .venv/bin/activate
```

Install the dependencies from requirements.txt
This is when the AWS DDK Core library is installed

```
$ pip install -r requirements.txt --no-cache-dir
```

If your AWS account hasn't been used to deploy DDK apps before, then you need to bootstrap your environment:

```
$ ddk bootstrap --help
$ ddk bootstrap --profile [AWS_PROFILE] --trusted-accounts [AWS_ACCOUNT_ID]
```

You might recognize a number of files typically found in a CDK Python application (e.g. app.py, cdk.json...). In addition, a file named ddk.json holding configuration about DDK specific constructs is present. Edit the DDK file with right account id and other data lake parameters (i.e. app, org, team, dataset, and pipeline names).

```
$ Edit ddk.json
```

Initialise git for the repository

```
$ git init --initial-branch main
```

Execute the create repository command to create a new codecommit repository

```
$ ddk create-repository AMC_QUICKSTART_REPO_NAME --profile [AWS_PROFILE] --region [AWS_REGION]
```

Add and push the initial commit to the repository

```
$ git config --global credential.helper "!aws codecommit --profile <my-profile> credential-helper $@"
$ git config --global credential.UseHttpPath true
$ git add .
$ git commit -m "Configure AMC QUICKSTART"
$ git push --set-upstream origin main
```

#

## Deploying the Quickstart

Once the above steps are performed, run the deploy command to deploy the quickstart

```
$ ddk deploy --profile [AWS_PROFILE]
```

The deploy all step deploys an AWS CodePipeline along with its respective AWS CloudFormation Stacks. The last stage of each pipeline delivers the AMC Quickstart infrastructure respectively in the child (default dev) environment through CloudFormation.

![Alt](/docs/images/AMC-Quickstart-Deploy.png)

_Foundations:_ This application creates the foundational resources for the quickstart. These resources include Lambda Layers, Glue Jobs, S3 Buckets, routing SQS Queues, and Amazon DynamoDB Tables for data and metadata storage.

_Data Lake:_ This application creates the resources for the data lake. All the resources needed for orchestration between services and data processing code are provisioned here.

_Microservices:_ This application creates the resources for the supporting Microservices. All the resources needed for orchestration between the microservices, data processing code, and data and metadata storage for the microservices are provisioned here.

For a walkthrough of the steps the AWS CodePipeline goes through to deploy these resources please refer to [here](#amc-quickstart-codepipeline-steps).

_NOTE:_ If deploying in a new AWS Account, the Assets stage of the CodePipeline may fail due to limitations for the number of concurrent file assets to publish. This is a current limitation of AWS CodeBuild. To fix, click the `Retry` button in CodePipeline for the Assets Stage. This will manually continue the Assets Stage to continue building file assets from its most current progress.

#

## Hydrating the Data Lake

To hydrate the data lake first edit the `tps.json` file located under the `scripts/microservice_scripts` folder. Configure the tps.json with the correct parameters for your AMC Instance, including S3 Bucket Name, AMC API Endpoint, Orange/Green Account IDs, etc.

Then run the below shell scripts:

```
$ make insert_tps_records
$ make create_workflows
```

_NOTE:_ Make sure the `ENV` variable in the `Makefile` is set to the correct environment name before proceeding to hydrate the data lake (default: `dev`)

For a more detailed description of the aforementioned commands, refer to below:

_Insert TPS Records:_ To initialize the process of onboarding your AMC instance on the Amazon AD Tech platform, this script adds client configurations to a TPS Customer Configuration table in Amazon DynamoDB. The configuration includes your AMC Endpoint URL, AMC Bucket Name and other related information on your AMC Instance. The Tenant Provisioning Service (TPS) will then automatically:

- Onboard clients using configuration which is persisted in a DynamoDB Table. It helps to reduce time to onboard new customers
- Provide functionality to automatically enable different modules (AMC/Sponsored ADs/DSP) during the onboarding process for each client
- Provide a centralized location to manage various clients and modules and supports multi-tenancy

_Create Workflows:_ To initialize the creation, scheduling and execution of AMC workflows, this script adds 4 default workflows to an AMC Workflows Library table in Amazon DynamoDB. From there Workflow Manager Service (WFM) will automatically create workflow schedules and add them to your AMC instance. WFM also allows you to:

- Automatically trigger the deployment of the SQS queues, IAM policies, workflows and workflow schedules in WFM for the customer's AMC instance upon adding or updating a customer record to the Tenant Provisioning Service (TPS)
- Synchronize workflows and workflow schedules in the Workflow Library service with multiple AMC Instances
- Send execution requests to an SQS queue rather than directly to the AMC endpoint to prevents timeout failures when there are large number of requests in a short period of time
- Scheduled with dynamic relative time windows rather than using AMC's scheduling feature which only allows predefined scheduled reporting such as Daily or Weekly
- Track the status of all workflow executions for customer AMC instances whether they are submitted through WFM or other means (postman, etc.). Having the status synced to DynamoDB allows events to be triggered or notifications to be sent when executions change state. This table can also be used to track historical executions for troubleshooting or performance monitoring.

#

## AMC Quickstart CodePipeline Steps

![Alt](/docs/images/AMC-Quickstart-CodePipeline-Steps.png)

The Code Pipeline Steps (as shown to the right) are:

- Source → Pull code from the source CodeCommit Repository
- Build → Runs `cdk synth` to translate CDK defintions into CloudFormation Template Definitions
- UpdatePipeline → Automatically update if new CDK applications or stages are added in the source code
- Assets → Publish CDK Assets
- AMCQuickstart → Prepares and Deploys all of the Resources in CloudFormation Stacks, including:
  - Foundational Resources
  - Data Lake Resources
  - Microservice Resources

#

## Cleaning Up the Quickstart

Once the solution has been deployed and tested, use the following command to clean up the resources

```
$ make delete_all
```

_NOTE:_ Before running this command, look into the `Makefile` and ensure that:

1.  The `delete_repositories` function is passing the correct `-d AMC_QUICKSTART_REPO_NAME` (default: `ddk-amc-quickstart`)

2.  The `delete_bootstrap` function is passing the correct `--stack-name BOOTSTRAP_STACK_NAME` (default: `DdkDevBootstrap`)

This command will go through the following sequence of steps in order to clean up your AWS account environment:

![Alt](/docs/images/AMC-Quickstart-Delete.png)
