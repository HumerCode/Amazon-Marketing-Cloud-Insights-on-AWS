#!/bin/bash
sflag=false
rflag=false

DIRNAME=$(pwd)

usage () { echo "
    -h -- Opens up this help message
    -s -- Name of the AWS profile to use
    -r -- AWS Region to deploy to (e.g. eu-west-1)
"; }
options=':s:r:h'
while getopts $options option
do
    case "$option" in
        s  ) sflag=true; DEVOPS_PROFILE=${OPTARG};;
        r  ) rflag=true; REGION=${OPTARG};;
        h  ) usage; exit;;
        \? ) echo "Unknown option: -$OPTARG" >&2; exit 1;;
        :  ) echo "Missing option argument for -$OPTARG" >&2; exit 1;;
        *  ) echo "Unimplemented option: -$OPTARG" >&2; exit 1;;
    esac
done



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

# TODO Still in progress, it is a working version
aws cloudformation create-stack \
        --stack-name amc-delivery-repo-bkt \
        --template-body file://"${DIRNAME}"/repo-bucket-creation.yaml \
        --capabilities "CAPABILITY_NAMED_IAM" "CAPABILITY_AUTO_EXPAND" \
        --region ${REGION} \
        --profile ${DEVOPS_PROFILE} \
        --tags file://"$DIRNAME"/tags.json

echo "Waiting for Repo Bucket Creation ..."
aws cloudformation wait stack-create-complete --profile ${DEVOPS_PROFILE} --region ${REGION} --stack-name amc-delivery-repo-bkt

BUCKET_NAME_REPO=$(aws cloudformation describe-stacks --profile ${DEVOPS_PROFILE} --region ${REGION} --stack-name amc-delivery-repo-bkt --query "Stacks[0].Outputs[?OutputKey=='oBucketName'].OutputValue" --output text)
echo "Removing hidden Git folders ..."
rm -rf ./.gitignore
rm -rf ./.git
echo "Copying Repo to S3 bucket"
aws s3 sync ./ s3://"${BUCKET_NAME_REPO}" --region ${REGION} --profile ${DEVOPS_PROFILE} --quiet --only-show-error
echo "Launching Init All Stack"
aws cloudformation create-stack \
        --stack-name amc-delivery-kit-init-all \
        --template-body file://"${DIRNAME}"/template_delivery_kit_all_initialization.yaml \
        --capabilities "CAPABILITY_NAMED_IAM" "CAPABILITY_AUTO_EXPAND" \
        --parameters \
            ParameterKey=pRepoS3Path,ParameterValue=s3://"${BUCKET_NAME_REPO}" \
        --region ${REGION} \
        --profile ${DEVOPS_PROFILE} \
        --tags file://"$DIRNAME"/tags.json
