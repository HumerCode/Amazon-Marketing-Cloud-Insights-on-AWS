# Platform Notebook Manager
The package contains sample jupyter notebooks which will help to interact and get started with using the Tenant Provisioning Service and Workflow Manager Service. 
The package deploys a Jupyter Notebook instance with pre-loaded Getting started notebooks for Tenant Provisioning Service and Workflow Manager Service.


**Contents:**

* [Prerequisites](#prerequisites)
* [AWS Service Requirements](#aws-service-requirements)
* [Resources Deployed](#resources-deployed)

## Prerequisites
1. The below resources are deployed as part of the SDLF (Serverless Datalake Package) package but it can deployed manually to only use the TPS MicroService. Refer the SDLF (Serverless Datalake Package) package deployment steps.
    1. Parameter Store /SDLF/S3/CFNBucket
2. AWS Account

## AWS Service Requirements

The following AWS services are required for this utility:

 1. [Amazon Sagemaker Notebook instance](https://docs.aws.amazon.com/sagemaker/latest/dg/nbi.html)
 2. [AWS Key Management Service (KMS)](https://aws.amazon.com/kms/)
 
## Resources Deployed
This template deploys the following resources:
1. Deploys the CloudFormation `platform_manager_sagemaker.yaml` present in the repository.
2. The Amazon Sagemaker Notebook instance `saw-platform-manager`. It will contain the below sample Notebooks.
    1. ### [To use the Tenant Provisioning Service](../CustomerManagementService/TenantProvisioningService/sdlf-tps-pipeline/README.md)
        1. **platform_manager/client_manager_microservices/tps/tps_GettingStarted** - Provides a guide about configuring/using the different features of the Tenant Provisioning Service with FAQs.
        2. **platform_manager/client_manager_microservices/tps/client_manager_adminstrator_sample** - Provides a sample notebook for a demo client and how a new AMC instance or a tenant can be onboarded to the platform. It also provides a functionality for scheduling default AMC workflows during the onboarding process.
    2. ### [To use the Workflow Manager Service Service](../DataLakeHydrationMicroservices/WorkflowManagerService/sdlf-wfm-pipeline/README.md)
        1. **platform_manager/datalake_hydration_microservices/wfm/wfm_GettingStarted** - Provides a Guide about configuring/using the different features of the Workflow Management Service with FAQs.
        2. **platform_manager/datalake_hydration_microservices/wfm/customerConfig_wfm_sample** - Provides a sample notebook to add/update/delete AMC instance related details for an existing onboarded AMC instance or tenant.
        3. **platform_manager/datalake_hydration_microservices/wfm/workflowLibrary_wfm_sample** - Provides a sample notebook to add/update/delete pre-loaded AMC workflows which will be applicable to all new AMC instance or teant which are onboarded to the platform.
        4. **platform_manager/datalake_hydration_microservices/wfm/workflowSchedules_wfm_sample** - Provides a sample notebook to add/update/delete AMC workflow schedules for existing AMC instance or tenant which are already onboarded to the platform.
        5. **platform_manager/datalake_hydration_microservices/wfm/workflows_wfm_sample** - Provides a sample notebook to add/update/delete AMC workflow details for existing AMC instance or tenant which are already onboarded to the platform.
        6. **platform_manager/datalake_hydration_microservices/wfm/workflow_invoke_wfm_sample** - Provides a sample notebook to adhoc (out of schedule) invocation of AMC workflows fro a particular AMC instance or tenant.
