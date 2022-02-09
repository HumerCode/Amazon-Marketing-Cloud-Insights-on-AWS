#!/usr/bin/env bash
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

set -x

DEFAULT_PROFILE="default"
DEFAULT_REGION=$(aws configure get region --profile ${DEFAULT_PROFILE})
DEFAULT_ENV="dev"
DEFAULT_VPC_ID=""
BASENAME=$(basename -a $0)
DIRNAME=$(pwd)
fflag=false
cflag=false

usage() { echo "
    -h -- Opens up this help message
    -s -- Name of the AWS profile to use for the Shared CICD Account
    -t -- Name of the AWS profile to use for the Child Account
    -r -- AWS Region to deploy to (e.g. eu-west-1)
    -e -- Environment to deploy to (dev, test or prod)
    -f -- Bootstrap Shared CICD Account
    -c -- Bootstrap Child Account
    -v -- Identifier of the VPC to attach compute resources to
"; }

while getopts ":s:t:r:e:v:fch" option; do
    case $option in
    s) CICD_PROFILE=${OPTARG:-$DEFAULT_PROFILE} ;;
    t) CHILD_PROFILE=${OPTARG:-$DEFAULT_PROFILE} ;;
    r) REGION=${OPTARG:-$DEFAULT_REGION} ;;
    e) ENV=${OPTARG:-$DEFAULT_ENV} ;;
    v) VPC_ID=${OPTARG:-$DEFAULT_VPC_ID} ;;
    f) fflag=true ;;
    c) cflag=true ;;
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

CICD_ACCOUNT=$(aws sts get-caller-identity --query "Account" --output text --profile ${CICD_PROFILE})
CHILD_ACCOUNT=$(aws sts get-caller-identity --query "Account" --output text --profile ${CHILD_PROFILE})

if $fflag; then
    echo "Bootstrapping CICD account..." >&2
    STACK_NAME=orion-cicd-bootstrap
    aws cloudformation deploy \
        --stack-name ${STACK_NAME} \
        --template-file ${DIRNAME}/bootstrap/template-cicd.yaml \
        --parameter-overrides \
        AttachToVpcId="${DEFAULT_VPC_ID}" \
        --tags Framework=orion \
        --capabilities "CAPABILITY_NAMED_IAM" "CAPABILITY_AUTO_EXPAND" \
        --region ${REGION} \
        --profile ${CICD_PROFILE}
fi

if $cflag; then
    echo "Bootstrapping Child Account..." >&2
    STACK_NAME=orion-${ENV}-bootstrap
    aws cloudformation deploy \
        --stack-name ${STACK_NAME} \
        --template-file ${DIRNAME}/bootstrap/template-child.yaml \
        --parameter-overrides \
        Environment="${ENV}" \
        TrustedAccount="${CICD_ACCOUNT}" \
        --tags Framework=orion \
        --capabilities "CAPABILITY_NAMED_IAM" "CAPABILITY_AUTO_EXPAND" \
        --region ${REGION} \
        --profile ${CHILD_PROFILE}
fi
