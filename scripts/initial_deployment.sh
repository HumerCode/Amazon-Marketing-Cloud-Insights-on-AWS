#!/bin/bash
sflag=false
rflag=false
fflag=false
eflag=false

startTime=$(date +%s)
DIRNAME=$(pwd)

usage () { echo "
    -h -- Opens up this help message
    -s -- Name of the AWS profile to use
    -r -- AWS Region to deploy to (e.g. eu-west-1)
    -f -- Deploys overall infrastructure
    -rp -- Pass the name of input REPO where the code base is
    -e -- Environment to deploy to (dev, test or prod)
"; }
options=':s:r:e:f:h:rp'
while getopts $options option
do
    case "$option" in
        s  ) sflag=true; DEVOPS_PROFILE=${OPTARG};;
        r  ) rflag=true; REGION=${OPTARG};;
        e  ) eflag=true; ENV=${OPTARG};;
        f  ) fflag=true;;
        h  ) usage; exit;;
        rp ) REPO=${OPTARG};;
        \? ) echo "Unknown option: -$OPTARG" >&2; exit 1;;
        :  ) echo "Missing option argument for -$OPTARG" >&2; exit 1;;
        *  ) echo "Unimplemented option: -$OPTARG" >&2; exit 1;;
    esac
done


if ! $eflag
then
    echo "-e not specified, using dev environment..." >&2
    ENV=dev
fi
if ! $sflag
then
    echo "-s not specified, using default..." >&2
    DEVOPS_PROFILE="default"
fi
if ! $rflag
then
    echo "-r not specified, using default region..." >&2
    REGION=$(aws configure get region --profile ${DEVOPS_PROFILE})
fi


DEVOPS_ACCOUNT=$(aws sts get-caller-identity --query 'Account' --output text --profile ${DEVOPS_PROFILE})
echo $REPO
TEAM=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq '.[] | select(.ParameterKey=="pTeamName") | .ParameterValue' \
      "$DIRNAME"/CustomerManagementService/TenantProvisioningService/sdlf-tps-pipeline/parameters-$ENV.json)")
echo "$TEAM"

function deploy_sdlf_pipeline()
{
    aws cloudformation create-stack \
        --stack-name sdlf-lw-ats-foundation \
        --template-body file://${DIRNAME}/DataLake/template_foundation_sdlf.yaml \
        --capabilities "CAPABILITY_NAMED_IAM" "CAPABILITY_AUTO_EXPAND" \
        --parameters \
            ParameterKey=pRepoS3Path,ParameterValue=${REPO}DataLake/ServerlessDatalakeFramework/aws-serverless-data-lake-framework \
            ParameterKey=pConfigFile,ParameterValue=${REPO}DataLake/ServerlessDatalakeFramework/ats-customer-config/demoCustomer \
        --region ${REGION} \
        --profile ${DEVOPS_PROFILE} \
        --tags file://$DIRNAME/DataLake/ServerlessDatalakeFramework/aws-serverless-data-lake-framework/tags.json
    echo "Waiting for SDLF stack to be created ..."
    aws cloudformation wait stack-create-complete --profile ${DEVOPS_PROFILE} --region ${REGION} --stack-name sdlf-lw-ats-foundation

}

function deploy_tps_pipeline()
{
    aws cloudformation create-stack \
        --stack-name tps-ats-foundation \
        --template-body file://${DIRNAME}/CustomerManagementService/template_tps_microservice_initialization.yaml \
        --capabilities "CAPABILITY_NAMED_IAM" "CAPABILITY_AUTO_EXPAND" \
        --parameters \
            ParameterKey=pRepoS3Path,ParameterValue=${REPO}CustomerManagementService/TenantProvisioningService/sdlf-tps-pipeline \
            ParameterKey=pConfigFile,ParameterValue=${REPO}CustomerManagementService/TenantProvisioningService/ats-customer-config/demoCustomer/tps-config/parameters-dev.json \
        --region ${REGION} \
        --profile ${DEVOPS_PROFILE} \
        --tags file://$DIRNAME/CustomerManagementService/TenantProvisioningService/sdlf-tps-pipeline/tags.json
    echo "Waiting for TPS stack to be created ..."
    aws cloudformation wait stack-create-complete --profile ${DEVOPS_PROFILE} --region ${REGION} --stack-name tps-ats-foundation

}

function deploy_wfm_pipeline()
{
    aws cloudformation create-stack \
        --stack-name wfm-ats-foundation \
        --template-body file://${DIRNAME}/DataLakeHydrationMicroservices/template_wfm_microservice_initialization.yml \
        --capabilities "CAPABILITY_NAMED_IAM" "CAPABILITY_AUTO_EXPAND" \
        --parameters \
            ParameterKey=pRepoS3Path,ParameterValue=${REPO}DataLakeHydrationMicroservices/WorkflowManagerService/sdlf-wfm-pipeline \
            ParameterKey=pConfigFile,ParameterValue=${REPO}DataLakeHydrationMicroservices/WorkflowManagerService/ats-wfm-customer-config/demoCustomer/wfm-config/parameters-dev.json \
        --region ${REGION} \
        --profile ${DEVOPS_PROFILE} \
        --tags file://$DIRNAME/DataLakeHydrationMicroservices/WorkflowManagerService/sdlf-wfm-pipeline/tags.json
    echo "Waiting for WFM stack to be created ..."
    aws cloudformation wait stack-create-complete --profile ${DEVOPS_PROFILE} --region ${REGION} --stack-name wfm-ats-foundation

}

function deploy_platform_notebooks()
{
    aws cloudformation create-stack \
        --stack-name notebook-ats-foundation \
        --template-body file://${DIRNAME}/PlatformManagementNotebooks/platform_manager_sagemaker_initialization.yaml \
        --capabilities "CAPABILITY_NAMED_IAM" "CAPABILITY_AUTO_EXPAND" \
        --parameters \
            ParameterKey=pRepoS3Path,ParameterValue=${REPO}PlatformManagementNotebooks \
        --region ${REGION} \
        --profile ${DEVOPS_PROFILE} \
        --tags file://$DIRNAME/PlatformManagementNotebooks/tags.json
    echo "Waiting for Platform notebooks stack to be created ..."
    aws cloudformation wait stack-create-complete --profile ${DEVOPS_PROFILE} --region ${REGION} --stack-name notebook-ats-foundation

}

function get_stack_resource()
{
  PHYSICALRID=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq -r '.StackResourceDetail.PhysicalResourceId' \
  <<<"$(aws cloudformation describe-stack-resource --stack-name "$1" --logical-resource-id "$2" \
        --profile ${DEVOPS_PROFILE} --region ${REGION})")")
  echo "$PHYSICALRID"
}


function renew_cred ()
{
  echo "Refreshing token"
  echo AWS_CONTAINER_CREDENTIALS_RELATIVE_URI $AWS_CONTAINER_CREDENTIALS_RELATIVE_URI
  curl -qL -o aws_credentials.json http://169.254.170.2/$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI > aws_credentials.json
  aws configure set aws_access_key_id `jq -r '.AccessKeyId' aws_credentials.json`
  aws configure set aws_secret_access_key `jq -r '.SecretAccessKey' aws_credentials.json`
  aws configure set aws_session_token `jq -r '.Token' aws_credentials.json`
  aws configure set default.region $AWS_REGION
  echo "Token Refreshed"
}


function check_code_build_status()
{
  set -e
  echo $1
  ID=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq -r '.ids[0]' <<<"$(aws codebuild list-builds-for-project \
                                          --profile ${DEVOPS_PROFILE} --region ${REGION} --project-name "$1")")")
  COUNT=0
  while [ "$COUNT" -lt 90 ] && [ "$(jq -r '.builds[0].buildStatus' \
  <<<"$(aws codebuild batch-get-builds --ids "${ID}" --profile ${DEVOPS_PROFILE} --region ${REGION})")" = IN_PROGRESS ]
  do
  sleep 60
  COUNT=$((COUNT+1))
  echo "${COUNT} Checking Code Build status..."
  # Added to renew cred
  endTime=$(date +%s)
  totalTime=$(($endTime-$startTime))
  if [[ $totalTime > 1800 ]]
  then
    startTime=$(date +%s)
    renew_cred
    echo "$totalTime and cred renewed"
  fi
  done
}

function insert_dynamo_record() {
  TABLE_NAME=tps-${TEAM}-CustomerConfig-dev
  NEW_UUID=$(cat /dev/urandom | tr -dc 'a-z0-9' | fold -w 12 | head -n 1)
  BUCKET_NAME=amc-testdemocustomer-${NEW_UUID}
  JSON_STRING=$( jq -n \
                    --arg bn "$BUCKET_NAME" \
                    --arg on "$TEAM" \
                    --arg re "$REGION" \
                    --arg ac "$DEVOPS_ACCOUNT" \
                    '{"customerId":{"S": "testdemocustomer"},
                      "customerName":{"S": "Test Demo Customer"},
                      "endemicType":{"S": "ENDEMIC"},
                      "customerPrefix":{"S": "testdemocustomer"},
                      "AMC":{"M": {"amcTeamName": {"S": $on},
                                   "amcOrangeAwsAccount": {"S": $ac},
                                   "amcS3BucketName": {"S": $bn},
                                   "amcDatasetName": {"S": "amcdataset"},
                                   "amcRegion": {"S": $re}
                                  }
                              }
                      }')
  aws dynamodb put-item \
      --table-name "${TABLE_NAME}" \
      --item "${JSON_STRING}" \
      --profile ${DEVOPS_PROFILE} \
      --region ${REGION}
  sleep 180
  echo "$BUCKET_NAME"
}

if $fflag
then
    echo "Deploying SDLF stack..." >&2
    deploy_sdlf_pipeline
#   Wait for all SDLF stacks to complete
    PR=$(get_stack_resource "sdlf-lw-ats-foundation" "rCodeBuildProject")
    check_code_build_status "$PR"
    echo "SDLF Code Build Complete"
#   Deploy TPS, WFM and Notebooks parallely
    echo "Deploying TPS foundational stack..." >&2
    deploy_tps_pipeline
    echo "Deploying WFM foundational stack..." >&2
    deploy_wfm_pipeline
    echo "Deploying Platform notebooks foundational stack..." >&2
    deploy_platform_notebooks
#   Wait for TPS, WFM and notebooks code build to finish
    echo "Waiting on all TPS, WFM and Notebook Stacks to deploy..." >&2
    PR=$(get_stack_resource "tps-ats-foundation" "rCodeBuildProject")
    check_code_build_status "$PR"
    echo "TPS Code Build Complete"
    PR=$(get_stack_resource "wfm-ats-foundation" "rCodeBuildProject")
    check_code_build_status "$PR"
    echo "WFM Code Build Complete"
    PR=$(get_stack_resource "notebook-ats-foundation" "rCodeBuildProject")
    check_code_build_status "$PR"
    echo "Notebook Code Build Complete"

   # Renew token
    renew_cred
#  After the infrastructure is deployed, insert Demo customer record to Dynamo
    BUCKET_NAME=$(insert_dynamo_record)
    echo "Waiting for AMC S3 bucket to be created ..."
#    aws s3api wait bucket-exists --bucket $BUCKET_NAME --profile ${DEVOPS_PROFILE} --region ${REGION}
    aws cloudformation wait stack-create-complete --profile ${DEVOPS_PROFILE} --region ${REGION} --stack-name sdlf-amcdataset-instance-testdemocustomer
    sleep 10
#   Once the AMC bucket is created, upload some sample data in it
    DATE=$(date +"%Y-%m-%d")
    aws s3 cp "${DIRNAME}"/sample_amc_files/DeviceExposure.csv \
      s3://${BUCKET_NAME}/workflow=device_exposure/schedule=adhoc/"${DATE}"T01:01:01.212Z-device_exposure.csv \
      --profile ${DEVOPS_PROFILE} --region ${REGION}
    aws s3 cp "${DIRNAME}"/sample_amc_files/FrequencyDistribution.csv \
      s3://${BUCKET_NAME}/workflow=frequency_distribution/schedule=adhoc/"${DATE}"T01:01:01.212Z-frequency_distribution.csv \
      --profile ${DEVOPS_PROFILE} --region ${REGION}
    aws s3 cp "${DIRNAME}"/sample_amc_files/GeoAnalysis.csv \
      s3://${BUCKET_NAME}/workflow=geo_analysis/schedule=adhoc/"${DATE}"T01:01:01.212Z-geo_analysis.csv \
      --profile ${DEVOPS_PROFILE} --region ${REGION}
    aws s3 cp "${DIRNAME}"/sample_amc_files/ProductMix.csv \
      s3://${BUCKET_NAME}/workflow=product_mix/schedule=adhoc/"${DATE}"T01:01:01.212Z-product_mix.csv \
      --profile ${DEVOPS_PROFILE} --region ${REGION}
    aws s3 cp "${DIRNAME}"/sample_amc_files/AudienceAnalysis.csv \
      s3://${BUCKET_NAME}/workflow=audience_analysis/schedule=adhoc/"${DATE}"T01:01:01.212Z-audience_analysis.csv \
      --profile ${DEVOPS_PROFILE} --region ${REGION}

    sleep 10m
fi