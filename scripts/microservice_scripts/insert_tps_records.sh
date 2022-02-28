#!/usr/bin/env bash
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

set -x 

DEFAULT_PROFILE="default"
DEFAULT_REGION=$(aws configure get region --profile ${DEFAULT_PROFILE})
BASENAME=$(basename -a $0)
DIRNAME=$(pwd)
DEFAULT_ENV="dev"

      

usage() {
    echo "Usage Options for ${BASENAME}
    -s : Name of the AWS Profile for the CICD Account
    -t : Name of the Child CICD account
    -r : Region for the Deployment
    -e ; Environment for the Deployment
    -h : Displays this help message
    "
}

while getopts "s:t:r:e:h" option; do
    case ${option} in
    s) CICD_PROFILE=${OPTARG:-$DEFAULT_PROFILE} ;;
    t) CHILD_PROFILE=${OPTARG:-$DEFAULT_PROFILE} ;;
    r) REGION=${OPTARG:-$DEFAULT_REGION} ;;
    e) ENV=${OPTARG:-$DEFAULT_ENV} ;;
    h)
        usage
        exit
        ;;
    \?)
        echo "Unknown Option ${OPTARG} at ${OPTIND} "
        exit 10
        ;;
    :)
        echo "Missing Argument for ${OPTARG} at ${OPTIND} "
        exit 20
        ;;
    *)
        echo "Option Not Implemented"
        exit 30
        ;;
    esac
done
OPTIND=1

CONFIG_PATH=".environments.${ENV}.data_pipeline_parameters.team"
AMC_TEAM_NAME=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq "$CONFIG_PATH" \
      "$DIRNAME"/ddk.json)")

CONFIG_PATH=".environments.${ENV}.data_pipeline_parameters.dataset"
AMC_DATASET_NAME=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq "$CONFIG_PATH" \
      "$DIRNAME"/ddk.json)")

CUSTOMER_ID=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq ".customerId" \
      "$DIRNAME"/scripts/microservice_scripts/tps.json)")

CUSTOMER_NAME=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq ".customerName" \
      "$DIRNAME"/scripts/microservice_scripts/tps.json)")

ENDEMIC_TYPE=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq ".endemicType" \
      "$DIRNAME"/scripts/microservice_scripts/tps.json)")

CUSTOMER_PREFIX=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq ".customerPrefix" \
      "$DIRNAME"/scripts/microservice_scripts/tps.json)")

AMC_ORANGE_ACCOUNT=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq ".amcOrangeAwsAccount" \
      "$DIRNAME"/scripts/microservice_scripts/tps.json)")

AMC_GREEN_ACCOUNT=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq ".amcGreenAwsAccount" \
      "$DIRNAME"/scripts/microservice_scripts/tps.json)")

AMC_API_ENDPOINT=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq ".amcApiEndpoint" \
      "$DIRNAME"/scripts/microservice_scripts/tps.json)")

AMC_S3_BUCKET=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq ".amcS3BucketName" \
      "$DIRNAME"/scripts/microservice_scripts/tps.json)")
   
# echo "$CICD_PROFILE"
# echo "$CHILD_PROFILE"
# echo "$REGION"
# echo "$AMC_TEAM_NAME"
# echo "$ENV"
# echo "$AMC_DATASET_NAME"
# echo "$CUSTOMER_ID"
# echo "$CUSTOMER_NAME"
# echo "$ENDEMIC_TYPE"
# echo "$CUSTOMER_PREFIX"
# echo "$AMC_ORANGE_ACCOUNT"
# echo "$AMC_GREEN_ACCOUNT"
# echo "$AMC_API_ENDPOINT"
# echo "$AMC_S3_BUCKET"

# DEVOPS_ACCOUNT=$(aws sts get-caller-identity --query 'Account' --output text --profile ${CHILD_PROFILE})

insert_dynamo_record() {
	TABLE_NAME=tps-${AMC_TEAM_NAME}-CustomerConfig-${ENV}
	JSON_STRING=$( jq -n \
						--arg ci "$CUSTOMER_ID" \
                        --arg cn "$CUSTOMER_NAME" \
                        --arg et "$ENDEMIC_TYPE" \
                        --arg cp "$CUSTOMER_PREFIX" \
                        --arg tn "$AMC_TEAM_NAME" \
						--arg oa "$AMC_ORANGE_ACCOUNT" \
                        --arg ga "$AMC_GREEN_ACCOUNT" \
                        --arg ae "$AMC_API_ENDPOINT" \
                        --arg sb "$AMC_S3_BUCKET" \
                        --arg dn "$AMC_DATASET_NAME" \
						--arg re "$REGION" \
						'{"customerId":{"S": $ci},
						"customerName":{"S": $cn},
						"endemicType":{"S": $et},
						"customerPrefix":{"S": $cp},							
						"AMC":{"M": {"amcTeamName": {"S": $tn},
									"amcOrangeAwsAccount": {"S": $oa},
									"amcGreenAwsAccount": {"S": $ga},
									"amcApiEndpoint": {"S": $ae},
									"amcS3BucketName": {"S": $sb},
									"amcDatasetName": {"S": $dn},
									"amcRegion": {"S": $re}
									}
								}
						}')
	aws dynamodb put-item \
		--table-name "${TABLE_NAME}" \
		--item "${JSON_STRING}" \
		--profile "${CHILD_PROFILE}" \
		--region "${REGION}"
	sleep 120
    echo "$JSON_STRING"
	echo "$BUCKET_NAME"
}



BUCKET_NAME=$(insert_dynamo_record)
echo "Waiting for AMC S3 bucket to be created ..."
aws cloudformation wait stack-create-complete --profile ${DEFAULT_PROFILE} --region ${DEFAULT_REGION} --stack-name amc-amcdataset-instance-testdemocustomer
sleep 10