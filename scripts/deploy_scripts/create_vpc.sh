#!/usr/bin/env bash
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

set -x

DEFAULT_PROFILE="default"
DEFAULT_REGION=$(aws configure get region --profile ${DEFAULT_PROFILE})
DEFAULT_ENV="dev"
DEFAULT_NUM_AZS=2
DIRNAME=$(pwd)

usage() { echo "
    -h -- Opens up this help message
    -p -- Name of the AWS profile to use
    -r -- AWS Region to deploy to (e.g. eu-west-1)
    -e -- Environment to deploy to
    -a -- Number of availability zones to utilize [1-3]
"; }

while getopts ":p:r:e:h:a" option; do
    case $option in
    p) AWS_PROFILE=${OPTARG:-$DEFAULT_PROFILE} ;;
    r) REGION=${OPTARG:-$DEFAULT_REGION} ;;
    e) ENV=${OPTARG:-$DEFAULT_ENV} ;;
    a) NUM_AZS=${OPTARG:-$DEFAULT_NUM_AZS} ;;
    h)
        usage
        exit
        ;;
    \?)
        echo "Unknown option: -$OPTARG" >&2
        exit 1
        ;;
    :)
        echo "Missing option argument for -$OPTARG" >&2
        exit 1
        ;;
    *)
        echo "Option Not Implemented"
        exit 30
        ;;
    esac
done

AWS_ACCOUNT=$(aws sts get-caller-identity --query "Account" --output text --profile ${AWS_PROFILE})

echo "Deploying VPC..." >&2
STACK_NAME=orion-${ENV}-vpc
aws cloudformation deploy \
    --stack-name ${STACK_NAME} \
    --template-file ${DIRNAME}/bootstrap/template-vpc.yaml \
    --parameter-overrides \
    Qualifier="orion-${ENV}" \
    NumberOfAZs=${NUM_AZS} \
    --tags Framework=orion \
    --capabilities "CAPABILITY_NAMED_IAM" "CAPABILITY_AUTO_EXPAND" \
    --region ${REGION} \
    --profile ${AWS_PROFILE}
