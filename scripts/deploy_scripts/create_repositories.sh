#!/usr/bin/env bash
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

set -x

typeset -i RETSTAT=0
DEFAULT_PROFILE="default"
DEFAULT_REGION=$(aws configure get region --profile ${DEFAULT_PROFILE})
BASENAME=$(basename -a $0)
DIRNAME=$(pwd)

usage() {
    echo "Usage Options for ${BASENAME}
    -s : Name of the AWS Profile for the CICD Account
    -t : Name of the Child CICD account
    -r : Region for the Deployment
    -h : Displays this help message
    "
}

check_repositories() {

    for REPOSITORY in ${REPOSITORIES[@]}; do
        REPOSITORY_DETAILS=$(aws codecommit get-repository --repository-name ${REPOSITORY} --region ${REGION} --profile ${CICD_PROFILE} &>/dev/null)
        RETSTAT=$?
        if [ ${RETSTAT} -eq 0 ]; then
            echo "Repository ${REPOSITORY} already exists, aborting..."
            exit 40
        else
            echo "Repository ${REPOSITORY} does not exists in ${REGION}."
        fi
    done

}

create_repositories() {
    git config --global credential.helper "!aws --profile ${CICD_PROFILE} codecommit credential-helper $@"
    git config --global credential.UseHttpPath true
    for REPOSITORY in ${REPOSITORIES[@]}; do
        aws codecommit create-repository --profile ${CICD_PROFILE} --region ${REGION} --repository-name ${REPOSITORY} --tags Framework=orion
        pushd ${REPOSITORY}
        # Updating cdk.json file with user AWS account ID and Region
        sed -i"" -e "s/111111111111/${CICD_ACCOUNT}/g" cdk.json
        sed -i"" -e "s/222222222222/${CHILD_ACCOUNT}/g" cdk.json
        sed -i"" -e "s/us-east-1/${REGION}/g" cdk.json
        git init
        git checkout -b main
        RETSTAT=$?
        if [ ${RETSTAT} -ne 0 ]; then
            echo "Branch main already exists for repository ${REPOSITORY}. Exiting..."
            exit 50
        fi
        git add .
        git remote add origin https://git-codecommit.${REGION}.amazonaws.com/v1/repos/${REPOSITORY}
        yes | git defender --setup
        git commit -m "Initial Commit" --no-verify
        git push --set-upstream origin main
        RETSTAT=$?
        if [ ${RETSTAT} -ne 0 ]; then
            echo "There was an issue when pushing changes to the remote branch."
            exit 60
        fi
        popd
    done
}

while getopts "s:t:r:h" option; do
    case ${option} in
    s) CICD_PROFILE=${OPTARG:-$DEFAULT_PROFILE} ;;
    t) CHILD_PROFILE=${OPTARG:-$DEFAULT_PROFILE} ;;
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

declare -a REPOSITORIES=$(find . -maxdepth 1 -name "orion*" -type d | sed "s/.\///")

CICD_ACCOUNT=$(aws sts get-caller-identity --query "Account" --output text --profile ${CICD_PROFILE})
CHILD_ACCOUNT=$(aws sts get-caller-identity --query "Account" --output text --profile ${CHILD_PROFILE})

check_repositories
create_repositories
