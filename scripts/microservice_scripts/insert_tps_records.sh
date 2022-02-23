#!/usr/bin/env bash
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT


DEFAULT_PROFILE="default"
DEFAULT_REGION=$(aws configure get region --profile ${DEFAULT_PROFILE})
BASENAME=$(basename -a $0)
DIRNAME=$(pwd)
DEFAULT_ENV="d"

      

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

CONFIG_PATH=".environments.${ENV}.tps_parameters.team"

TEAM=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq "$CONFIG_PATH" \
      "$DIRNAME"/ddk.json)")
      
echo "$CICD_PROFILE"
echo "$CHILD_PROFILE"
echo "$REGION"
echo "$TEAM"
echo "$ENV"


DEVOPS_ACCOUNT=$(aws sts get-caller-identity --query 'Account' --output text --profile ${CHILD_PROFILE})

insert_dynamo_record() {
	TABLE_NAME=tps-${TEAM}-CustomerConfig-${ENV}
	NEW_UUID=$(dd bs=18 count=1 if=/dev/urandom | base64 | tr -dc 'a-z0-9'| fold -w 12 | head -n 1)
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
									"amcGreenAwsAccount": {"S": $ac},
									"amcApiEndpoint": {"S": "https://PLACEHOLDER.com"},
									"amcS3BucketName": {"S": $bn},
									"amcDatasetName": {"S": "amcdataset"},
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
	echo "$BUCKET_NAME"
}



BUCKET_NAME=$(insert_dynamo_record)
echo "Waiting for AMC S3 bucket to be created ..."
aws cloudformation wait stack-create-complete --profile ${DEFAULT_PROFILE} --region ${DEFAULT_REGION} --stack-name amc-amcdataset-instance-testdemocustomer
sleep 10