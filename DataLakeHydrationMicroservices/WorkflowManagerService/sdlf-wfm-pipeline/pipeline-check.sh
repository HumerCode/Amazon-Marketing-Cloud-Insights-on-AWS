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
    -f -- Checks for WFM Code pipeline status

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

ENV=$(sed -e 's/^"//' -e 's/"$//' <<<"$(aws ssm get-parameter --name /SDLF/Misc/pEnv --profile $DEVOPS_PROFILE --query "Parameter.Value")")
echo $ENV
TEAM=$(sed -e 's/^"//' -e 's/"$//' <<<"$(jq '.[] | select(.ParameterKey=="pTeamName") | .ParameterValue' $DIRNAME/parameters-$ENV.json)")
set +e
while true;
do
  if ! aws codepipeline get-pipeline-state --name sdlf-${TEAM}-cicd-wfm-pipeline; then
    sleep 5
    echo "Waiting for Pipeline to get created.."
  else
    break
  fi
done
set -e
COUNT=0
while [ "$COUNT" -lt 30 ] && [ "$(jq -r '.stageStates[1].latestExecution.status' \
<<<"$(aws codepipeline get-pipeline-state --name sdlf-${TEAM}-cicd-wfm-pipeline)")" = InProgress ]
do
sleep 60
COUNT=$((COUNT+1))
echo "${COUNT} Checking Code Pipeline status..."
done