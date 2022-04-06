# Tenant Provisioning Service (Client Manager MicroService)

The Tenant Provisioning Service is part of the Client Manager MicroServices package, it is an Amazon Ad Tech solution that adds functionality for managing multiple Amazon Marketing Cloud clients.

**Purpose**

- The Purpose of the Tenant Provisioning Service is to allow AMC/Sponsored Ads/DSP users to automate the onboarding of multiple clients on the Amazon Ad Tech platform without having to develop custom solutions. Below is a list of the features of the service.

**Ability to add new customers using configuration**

- TPS provides an ability to onboard clients using configuration which is persisted in a DynamoDB Table. It helps to reduce time to onboard new customers.

**Enable different modules for each clients**

- TPS provides functionality to automatically enable different modules (AMC/Sponsored ADs/DSP) during the onboarding process for each client.

**Multi-tenant Support**

- TPS provides a centralized location to manage various clients and modules and supports multi-tenancy.

**Contents:**

- [Reference Architecture](#reference-architecture)
- [Prerequisites](#prerequisites)
- [AWS Service Requirements](#aws-service-requirements)
- [Resources Deployed](#resources-deployed)
- [Parameters](#parameters)
- [Deployment](#deployment)

## Reference Architecture

![Alt](../../../docs/images/AMC-QuickStart-TPS-Architecture.png)

## Prerequisites

1. The TPS Stack resources are deployed as part of the AMC Quickstart package. Refer to the AMC QuickStart Deployment Steps in order to deploy the TPS Stack.
2. AWS Account
3. IDE for e.g. [Pycharm](https://www.jetbrains.com/pycharm/) or [AWS Cloud9](https://aws.amazon.com/cloud9/)
4. [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)

## AWS Service Requirements

The following AWS services are required for this utility:

1.  [AWS Lambda](https://aws.amazon.com/lambda/)
2.  [Amazon S3](https://aws.amazon.com/s3/)
3.  [Amazon DynamoDB](https://aws.amazon.com/dynamodb/)
4.  [AWS Identity and Access Management (IAM)](https://aws.amazon.com/iam/)
5.  [AWS Step Functions](https://aws.amazon.com/step-functions/)
6.  [Amazon Simple Queue Service](https://aws.amazon.com/sqs/)
7.  [Amazon Simple Notification Service](https://aws.amazon.com/sns/)
8.  [AWS Key Management Service (KMS)](https://aws.amazon.com/kms/)

## Resources Deployed

Following the AMC QuickStart Deployment Steps, this CDK Application deploys the following resources:

1. Deploys the cdk stack `tenant_provisioning_service.py` present in the repository. This stack will be deployed in the microservices pipeline stage of the CICD Pipeline defined in the `app.py`.

### Parameters

Before deploying the pipeline, ensure that the `tps_parameters` dictionary in the `ddk.json` file contains the correct following parameters for the **TPS** stack:

1. `team` — The name of the team which owns the pipeline.
2. `pipeline` — Name to give the pipeline being deployed.

The `ddk.json` is the same level as `app.py` within `amc-quickstart`. If the parameters are not filled, default values for team and pipeline will be used (i.e. demoteam and cmpl).

### Deployment

Refer to the AMC QuickStart Deployment Steps in order to deploy this stack.
