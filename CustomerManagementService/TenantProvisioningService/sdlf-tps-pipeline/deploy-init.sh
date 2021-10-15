#!/bin/bash
sflag=false
rflag=false
eflag=false
fflag=false


DIRNAME=$(pwd)

usage () { echo "
    -h -- Opens up this help message
    -s -- Name of the AWS profile to use
    -r -- AWS Region to deploy to (e.g. eu-west-1)
    -f -- Deploys TPS Foundations

"; }
options=':s:r:e:fh'
while getopts $options option
do
    case "$option" in
        s  ) sflag=true; DEVOPS_PROFILE=${OPTARG};;
        r  ) rflag=true; REGION=${OPTARG};;
        f  ) fflag=true;;
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


DEVOPS_ACCOUNT=$(aws sts get-caller-identity --query 'Account' --output text --profile ${DEVOPS_PROFILE})

ARTIFACT_STORE_BUCKET=$(sed -e 's/^"//' -e 's/"$//' <<<"$(aws ssm get-parameter --name /SDLF/S3/CFNBucket --profile $DEVOPS_PROFILE --query "Parameter.Value")")
echo $ARTIFACT_STORE_BUCKET
ENV=$(sed -e 's/^"//' -e 's/"$//' <<<"$(aws ssm get-parameter --name /SDLF/Misc/pEnv --profile $DEVOPS_PROFILE --query "Parameter.Value")")
echo $ENV
TEAM=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq '.[] | select(.ParameterKey=="pTeamName") | .ParameterValue' $DIRNAME/parameters-$ENV.json)")
echo $TEAM
PIPELINE_NAME=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq '.[] | select(.ParameterKey=="pPipelineName") | .ParameterValue' $DIRNAME/parameters-$ENV.json)")
echo $PIPELINE_NAME

function bootstrap_repository()
{
    REPOSITORY=${1}
    echo "Creating and Loading ${REPOSITORY} Repository"
    aws codecommit create-repository --region ${REGION} --profile ${DEVOPS_PROFILE} --repository-name ${REPOSITORY}
    git init
    git add .
    git commit -m "Initial Commit"
    git remote add origin https://git-codecommit.${REGION}.amazonaws.com/v1/repos/${REPOSITORY}
    git push --set-upstream origin master
    git checkout -b test
    git push --set-upstream origin test
    git checkout -b dev
    git push --set-upstream origin dev
    aws codecommit update-default-branch --repository-name ${REPOSITORY} --default-branch-name dev --profile ${DEVOPS_PROFILE}
}

function deploy_tps_foundations()
{
    git config --global credential.helper '!aws --profile '${DEVOPS_PROFILE}' codecommit credential-helper $@'
    git config --global credential.UseHttpPath true
    for REPOSITORY in "${REPOSITORIES[@]}"
    do
        bootstrap_repository ${REPOSITORY}
    done
    cd ${DIRNAME}
}



if $fflag
then
    echo "Deploying TPS foundational repositories..." >&2
    declare -a REPOSITORIES=("sdlf-tps-pipeline")
    deploy_tps_foundations
    STACK_NAME=sdlf-${TEAM}-cicd-tps-repos
    aws cloudformation create-stack \
        --stack-name ${STACK_NAME} \
        --template-body file://${DIRNAME}/nested-stacks/template-cicd-tps.yaml \
        --parameters \
            ParameterKey=pArtifactoryStore,ParameterValue=${ARTIFACT_STORE_BUCKET} \
            ParameterKey=pBranchName,ParameterValue=${ENV} \
            ParameterKey=pBuildDatalakeLibrary,ParameterValue=/SDLF/CodeBuild/${TEAM}/BuildDeployDatalakeLibraryLayer \
            ParameterKey=pCloudWatchRepositoryTriggerRoleArn,ParameterValue=/SDLF/IAM/${TEAM}/CloudWatchRepositoryTriggerRoleArn \
            ParameterKey=pCodePipelineRoleArn,ParameterValue=/SDLF/IAM/${TEAM}/CodePipelineRoleArn \
            ParameterKey=pEnv,ParameterValue=${ENV} \
            ParameterKey=pKMSInfraKeyId,ParameterValue=/SDLF/KMS/KeyArn \
            ParameterKey=pPipeline,ParameterValue=${PIPELINE_NAME} \
            ParameterKey=pRepositoryName,ParameterValue=sdlf-tps-pipeline \
            ParameterKey=pSNSTopic,ParameterValue=/SDLF/SNS/${TEAM}/Notifications \
            ParameterKey=pTeamName,ParameterValue=${TEAM} \
        --tags file://${DIRNAME}/tags.json \
        --capabilities "CAPABILITY_NAMED_IAM" "CAPABILITY_AUTO_EXPAND" \
        --region ${REGION} \
        --profile ${DEVOPS_PROFILE}
    echo "Waiting for stack to be created ..."
    aws cloudformation wait stack-create-complete --profile ${DEVOPS_PROFILE} --region ${REGION} --stack-name ${STACK_NAME}
fi
