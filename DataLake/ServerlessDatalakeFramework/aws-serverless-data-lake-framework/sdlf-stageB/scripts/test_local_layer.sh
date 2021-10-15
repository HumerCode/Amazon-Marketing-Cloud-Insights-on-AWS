#!/bin/bash
fflag=false
pflag=false
eflag=false
tflag=false

usage () { echo "
    -h -- Opens up this help message.
    -p -- AWS profile to use to run the AWS commands in this script. Uses 'default', if unspecified.
    -f -- Path to file containing parameters.
    -e -- Path to event JSON file.
    -t -- Path to CloudFormation template used for testing.
    
    IMPORTANT: All directories provided to this script refer to a ROOT_DIR environment variable, which reference the parent directory of an individual Lambda function.
"; }

options=':p:f:e:t:h'
while getopts $options option
do
    case "$option" in
        f  ) fflag=true; FILEPATH=${OPTARG};;
        p  ) pflag=true; PROFILE=${OPTARG};;
        e  ) eflag=true; EVENT=${OPTARG};;
        t  ) tflag=true; TEST_TEMPLATE=${OPTARG};;
        h  ) usage; exit;;
        \? ) echo "Unknown option: -$OPTARG" >&2; exit 1;;
        :  ) echo "Missing option argument for -$OPTARG" >&2; exit 1;;
        *  ) echo "Unimplemented option: -$OPTARG" >&2; exit 1;;
    esac
done
if ((OPTIND == 1))
then
    usage; exit;
fi
shift $((OPTIND - 1))
if ! $fflag
then
    echo "-f must be specified" >&2
    exit 1
fi
if ! $pflag
then
    echo "Using default profile..."
    PROFILE="default"
fi
if ! $eflag
then
    echo "-e must be specified" >&2
    exit 1
fi
if ! $tflag
then
    echo "-t must be specified" >&2
    exit 1
fi

function_name=$(cat $ROOT_DIR/$TEST_TEMPLATE | yq .Resources | yq -r 'to_entries[] | select (.value.Type | contains("AWS::Serverless::Function")) | .key')
param_string=$(cat $ROOT_DIR/$FILEPATH)
sam local invoke -t $ROOT_DIR/$TEST_TEMPLATE --profile $PROFILE --event $ROOT_DIR/$EVENT --parameter-overrides $(printf "\'%s\'" $param_string) $function_name