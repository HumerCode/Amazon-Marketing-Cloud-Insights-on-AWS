# Orion AMC QuickStart Cleanup Guide

## Cleanup Steps

1. From within the **quickstart-amazon-marketing-cloud** repository run:

```
make delete_all
```

That is it! All of your Quickstart resources are in the process of getting deleted from your AWS Account. The clean up takes approximately 30 minutes to complete as it goes through a series of steps to delete all of the Quickstart resources. To get more information on the steps taken for cleanup read the document [below](#make-delete_all-command-steps).

NOTE: The delete command assumes that you have only deployed this Quickstart in your AWS Account and thus will empty and delete ALL of the resources in the account, including all S3 Buckets, all DynamoDB Tables, all KMS customer-managed keys, and all AWS Lambda Layers. If you have other resources you wish to retain separate the Quickstart in your AWS Account, please review the **list_items_to_delete.py** python script in the **scripts/** repository.

#

## Make Delete_All Command Steps

The Makefile _delete_all_ command goes through a series of 6 steps to delete the Quickstart resources in your AWS account: [Empty S3 Buckets](#empty-s3-buckets), [Delete Orion Satellite](#delete-orion-satellite), [Delete Orion Artifacts](#delete-orion-artifacts), [Delete Bootstrap](#delete-bootstrap-stacks), [Delete Repositories](#delete-repositories), and [Delete Remaining Items](#delete-remaining-items).

![Alt](./docs/static/images/Orion-AMC-Quickstart-Delete-All.png)

### Empty S3 Buckets

Run 2 python scripts. The **_list_items_to_delete.py_** script lists all the S3 Buckets in the AWS Account and writes them to a JSON file, **_delete_file.json._ **Next, the **_empty_bucket.py_ **uses the **_delete_file.json_** JSON file to empty all of the contents from the S3 Buckets so the CloudFormation Stacks are able to delete them.

### Delete Orion Satellite

Destroys all of the AWS CloudFormation stacks for orion-satellite. These stacks include the following:

- Data Lake Specific CloudFormation Stacks
  - orion-dev-satellite-orion-foundations
  - orion-dev-satellite-orion-sdlf-pipelines
  - orion-dev-satellite-orion-sdlf-datasets
- Microservice Specific CloudFormation Stacks
  - orion-dev-microservices-orion-wfm (for Workflow Management Service - WFM)
  - orion-dev-microservices-orion-tps (for Tenant Provisioning Service - TPS)
  - orion-dev-microservices-orion-platform-manager (for Platform Manager Notebooks - PMN)
- Event Rule CloudFormation Stack (Listening for Orion Artifacts CodePipeline Deploy)
  - orion-cicd-satellite/orion-cicd-event-rule
- Orion Satellite CICD CodePipeline Infrastructure CloudFormation Stack
  - orion-satellite-pipeline

### Delete Orion Artifacts

Destroys all of the AWS CloudFormation stacks for orion-artifacts. These stacks include the following:

- Artifacts Resource Specific CloudFormation Stacks
  - orion-dev-artifacts-glue (Glue Jobs)
  - orion-dev-artifacts-layers (Lambda Layers)
  - orion-dev-artifacts-base (Base Resources - i.e. Artifacts S3 Bucket)
- Orion Artifacts CICD CodePipeline Infrastructure CloudFormation Stack
  - orion-cicd-artifacts-pipeline

### Delete Bootstrap Stacks

Deletes the 2 AWS CloudFormation stacks responsible with bootstrapping the AWS Account with infrastructure required by the CDK and Orion to manage Infrastructure as Code (IaC). These resources include:

- AWS CodePipeline for orion-commons
- AWS CodeArtifacts Domain
- AWS IAM Policies
- AWS SSM Parameters
- AWS EventBridge Triggers

### Delete Repositories

Deletes the 3 AWS CodeCommit repositories and removes git version control for:

- orion-commons
- orion-artifacts
- orion-satellite

### Delete Remaining Items

Run 2 python scripts. The **_list_items_to_delete.py_** script lists all the remaining resources in the AWS Account and writes them to a JSON file, **_delete_file.json_**, including:

- S3 Buckets (if not deleted)
- DDB Tables (if not deleted)
- KMS Customer Managed Keys
- Lambda Layers (if not deleted)
- SQS Queues (that are created separate from the Orion CloudFormation Stacks)
- Eventbridge Rules (that are created separate from the Orion CloudFormation Stacks)
- CloudFormation Stacks (that are created when onboarding a new AMC Instance by Tenant Provisioning Service)

The **_delete_script.py_** then uses the **_delete_file.json_** JSON file to delete (or schedule for deletion for KMS Keys) all the remaining listed resources in the AWS Account.
