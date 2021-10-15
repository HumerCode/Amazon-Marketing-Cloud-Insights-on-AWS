#!/bin/bash
fflag=true
eflag=false
pflag=false

DIRNAME=$(pwd)
echo "$DIRNAME"

usage () { echo "
    -h -- Opens up this help message
    -p -- Name of the AWS profile to use
    -e -- Environment to deploy to (dev, test or prod)
    -f -- Get Code Pipeline status
"; }
options=':p:e:f:h'
while getopts $options option
do
    case "$option" in
        p  ) pflag=true; PROFILE=$OPTARG;;
        e  ) eflag=true; ENV=${OPTARG};;
        f  ) fflag=true;;
        h  ) usage; exit;;
        \? ) echo "Unknown option: -$OPTARG" >&2; exit 1;;
        :  ) echo "Missing option argument for -$OPTARG" >&2; exit 1;;
        *  ) echo "Unimplemented option: -$OPTARG" >&2; exit 1;;
    esac
done


if ! $pflag
then
    echo "-p not specified, using default..." >&2
    PROFILE="default"
fi

if ! $eflag
then
    echo "-e not specified, using dev environment..." >&2
    ENV=dev
fi

REGION=$(aws configure get region --profile ${PROFILE})

#ENV=$(sed -e 's/^"//' -e 's/"$//' <<<"$(aws ssm get-parameter --name /SDLF/Misc/pEnv --profile $PROFILE --query "Parameter.Value")")
#TEAM_NAME=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq '.[] | select(.ParameterKey=="pTeamName") | .ParameterValue' $DIRNAME/parameters-$ENV.json)")
#PIPELINE_NAME=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq '.[] | select(.ParameterKey=="pPipelineName") | .ParameterValue' $DIRNAME/parameters-$ENV.json)")
#STAGES=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq '.[] | select(.ParameterKey | endswith("StateMachineRepository")) | .ParameterKey | .[6:7]' $DIRNAME/parameters-$ENV.json)")

#STAGEA_SM_STACK_NAME=$(cut -d'/' -f2 <<<"$(aws cloudformation describe-stack-resource --stack-name sdlf-$TEAM_NAME-$PIPELINE_NAME --logical-resource-id stageAStateMachinePipeline --query 'StackResourceDetail.PhysicalResourceId')")
#STAGEB_SM_STACK_NAME=$(cut -d'/' -f2 <<<"$(aws cloudformation describe-stack-resource --stack-name sdlf-$TEAM_NAME-$PIPELINE_NAME --logical-resource-id stageBStateMachinePipeline --query 'StackResourceDetail.PhysicalResourceId')")
#STAGEA_SM_PIPELINE_NAME=$(aws cloudformation describe-stack-resource --stack-name $STAGEA_SM_STACK_NAME --logical-resource-id rStateMachinePipeline --query 'StackResourceDetail.PhysicalResourceId' --output text)
#STAGEB_SM_PIPELINE_NAME=$(aws cloudformation describe-stack-resource --stack-name $STAGEB_SM_STACK_NAME --logical-resource-id rStateMachinePipeline --query 'StackResourceDetail.PhysicalResourceId' --output text)

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


function deploy_sdlf_components()
{
    REPOSITORY=${1}
    git checkout dev
    git add .
    git commit -m "Deploy ${REPOSITORY}"
    git pull origin dev
    git push --set-upstream origin dev
}

function check-pipeline-status()
{
  PIPELINE=${1}
  while true
  do
    echo "Checking if pipeline exists ..."
    if ! aws codepipeline get-pipeline --name ${PIPELINE};
    then
        echo "Pipeline is being created ..."
    else
        break
    fi
  done

  i=30
  while [ $i -gt 0 ]
  do
      if [ "$(aws codepipeline get-pipeline-state --name ${PIPELINE} --query 'length(stageStates[*])')" == "$(aws codepipeline get-pipeline-state --name ${PIPELINE} --query 'length(stageStates[?latestExecution.status==`Succeeded`])')" ];
      then
          echo "All pipeline stages succeeded."
          break
      else
          for stage in $(aws codepipeline get-pipeline-state --name ${PIPELINE} --query 'length(stageStates[?latestExecution.status==`In Progress`])');
              do echo "Pipeline stage $stage is in progress ...";
          done
          sleep 1m
      fi
      ((i--))
  done
}

if $fflag
then
    echo "Get Step 1 SDLF Initialization sdlf-cicd-team pipeline status" >&2
    check-pipeline-status "sdlf-cicd-team"
    echo "Get Step 1 SDLF Initialization sdlf-cicd-foundations pipeline status" >&2
    check-pipeline-status "sdlf-cicd-foundations"
    sleep 10
    # Renew token
    renew_cred

    echo "Deploy Step 2 SDLF Foundations" >&2
    cd ${DIRNAME}/sdlf-foundations/
    cp ${DIRNAME}/../ats-customer-config/sdlf-foundations/parameters-${ENV}.json ${DIRNAME}/sdlf-foundations/parameters-${ENV}.json
    deploy_sdlf_components "sdlf-foundations"
    sleep 5m
    echo "Get Step 2 SDLF Foundations sdlf-cicd-foundations pipeline status" >&2
    check-pipeline-status "sdlf-cicd-foundations"
    sleep 10
    # Renew token
    renew_cred

    echo "Deploy Step 3 SDLF Team" >&2
    cd ${DIRNAME}/sdlf-team/
    cp ${DIRNAME}/../ats-customer-config/sdlf-team/parameters-${ENV}.json ${DIRNAME}/sdlf-team/parameters-${ENV}.json
    deploy_sdlf_components "sdlf-team"
    sleep 5m
    TEAM_NAME=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq '.[] | select(.ParameterKey=="pTeamName") | .ParameterValue' ${DIRNAME}/sdlf-team/parameters-${ENV}.json)")
    echo "Get Step 3 SDLF Team sdlf-cicd-team pipeline status" >&2
    check-pipeline-status "sdlf-cicd-team"
    echo "Get Step 3 SDLF Team sdlf-$TEAM_NAME-cicd-dataset pipeline status" >&2
    check-pipeline-status "sdlf-$TEAM_NAME-cicd-dataset"
    echo "Get Step 3 SDLF Team sdlf-$TEAM_NAME-cicd-pipeline pipeline status" >&2
    check-pipeline-status "sdlf-$TEAM_NAME-cicd-pipeline"
    echo "Get Step 3 SDLF Team sdlf-$TEAM_NAME-default-pip-libraries pipeline status" >&2
    check-pipeline-status "sdlf-$TEAM_NAME-default-pip-libraries"
    echo "Get Step 3 SDLF Team sdlf-$TEAM_NAME-datalake-lib-layer pipeline status" >&2
    check-pipeline-status "sdlf-$TEAM_NAME-datalake-lib-layer"
    sleep 10
    # Renew token
    renew_cred

    echo "Deploy Step 4 SDLF Pipeline" >&2
    cd $DIRNAME
    git clone https://git-codecommit.$REGION.amazonaws.com/v1/repos/sdlf-$TEAM_NAME-pipeline
    cd sdlf-$TEAM_NAME-pipeline
    cp ${DIRNAME}/../ats-customer-config/sdlf-ats-pipeline/parameters-${ENV}.json ${DIRNAME}/sdlf-$TEAM_NAME-pipeline/parameters-${ENV}.json
    deploy_sdlf_components "sdlf-$TEAM_NAME-pipeline"
    sleep 5m
    PIPELINE_NAME=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq '.[] | select(.ParameterKey=="pPipelineName") | .ParameterValue' $DIRNAME/sdlf-$TEAM_NAME-pipeline/parameters-${ENV}.json)")
    STAGEA_SM_STACK_NAME=$(cut -d'/' -f2 <<<"$(aws cloudformation describe-stack-resource --stack-name sdlf-$TEAM_NAME-$PIPELINE_NAME --logical-resource-id stageAStateMachinePipeline --query 'StackResourceDetail.PhysicalResourceId')")
    STAGEB_SM_STACK_NAME=$(cut -d'/' -f2 <<<"$(aws cloudformation describe-stack-resource --stack-name sdlf-$TEAM_NAME-$PIPELINE_NAME --logical-resource-id stageBStateMachinePipeline --query 'StackResourceDetail.PhysicalResourceId')")
    STAGEA_SM_PIPELINE_NAME=$(aws cloudformation describe-stack-resource --stack-name $STAGEA_SM_STACK_NAME --logical-resource-id rStateMachinePipeline --query 'StackResourceDetail.PhysicalResourceId' --output text)
    STAGEB_SM_PIPELINE_NAME=$(aws cloudformation describe-stack-resource --stack-name $STAGEB_SM_STACK_NAME --logical-resource-id rStateMachinePipeline --query 'StackResourceDetail.PhysicalResourceId' --output text)
    echo "Get Step 4 SDLF Pipeline sdlf-$TEAM_NAME-cicd-pipeline pipeline status" >&2
    check-pipeline-status "sdlf-$TEAM_NAME-cicd-pipeline"
    echo "Get Step 4 SDLF Pipeline $STAGEA_SM_PIPELINE_NAME pipeline status" >&2
    check-pipeline-status $STAGEA_SM_PIPELINE_NAME
    echo "Get Step 4 SDLF Pipeline $STAGEB_SM_PIPELINE_NAME pipeline status" >&2
    check-pipeline-status $STAGEB_SM_PIPELINE_NAME
    sleep 10
    # Renew token
    renew_cred

    echo "Deploy Step 5 SDLF Dataset" >&2
    cd $DIRNAME
    git clone https://git-codecommit.$REGION.amazonaws.com/v1/repos/sdlf-$TEAM_NAME-dataset
    cd sdlf-$TEAM_NAME-dataset
    cp ${DIRNAME}/../ats-customer-config/sdlf-ats-dataset/amcdataset/parameters-${ENV}.json ${DIRNAME}/sdlf-$TEAM_NAME-dataset/amcdataset/parameters-${ENV}.json
    deploy_sdlf_components "sdlf-$TEAM_NAME-dataset"
    sleep 5m
    echo "Get Step 5 SDLF Dataset sdlf-$TEAM_NAME-cicd-dataset pipeline status" >&2
    check-pipeline-status "sdlf-$TEAM_NAME-cicd-dataset"
    sleep 10
    # Renew token
    renew_cred

    echo "Deploy Step 6 SDLF Transformations" >&2
    cd $DIRNAME
    git clone https://git-codecommit.$REGION.amazonaws.com/v1/repos/sdlf-$TEAM_NAME-datalakeLibrary
    cd sdlf-$TEAM_NAME-datalakeLibrary
    cp ${DIRNAME}/../ats-customer-config/sdlf-ats-datalakeLibrary/dataset_mappings.json ${DIRNAME}/sdlf-$TEAM_NAME-datalakeLibrary/python/datalake_library/transforms/dataset_mappings.json
    deploy_sdlf_components "sdlf-$TEAM_NAME-datalakeLibrary"
    sleep 5m
    echo "Get Step 6 SDLF Transformations sdlf-$TEAM_NAME-datalake-lib-layer pipeline status" >&2
    check-pipeline-status "sdlf-$TEAM_NAME-datalake-lib-layer"
    echo "Get Step 6 SDLF Transformations $STAGEA_SM_PIPELINE_NAME pipeline status" >&2
    check-pipeline-status $STAGEA_SM_PIPELINE_NAME
    echo "Get Step 6 SDLF Transformations $STAGEB_SM_PIPELINE_NAME pipeline status" >&2
    check-pipeline-status $STAGEB_SM_PIPELINE_NAME
fi
