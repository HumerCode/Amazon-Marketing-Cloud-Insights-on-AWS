#!/usr/bin/env bash
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

set -x

DEFAULT_PROFILE="orion-adtech"
DEFAULT_REGION=$(aws configure get region --profile ${DEFAULT_PROFILE})
BASENAME=$(basename -a $0)
DIRNAME=$(pwd)

usage() {
    echo "Usage Options for ${BASENAME}
    -s : Name of the AWS Profile for the CICD Account
    -r : Region for the Deployment
    -h : Displays this help message
    "
}

while getopts "s:r:h" option; do
    case ${option} in
    s) CICD_PROFILE=${OPTARG:-$DEFAULT_PROFILE} ;;
    r) REGION=${OPTARG:-$DEFAULT_REGION} ;;
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

CICD_ACCOUNT=$(aws sts get-caller-identity --query "Account" --output text --profile ${CICD_PROFILE})

aws codeartifact login --tool pip --repository orion-commons --domain orion --domain-owner ${CICD_ACCOUNT} --profile ${CICD_PROFILE} --region ${REGION}
