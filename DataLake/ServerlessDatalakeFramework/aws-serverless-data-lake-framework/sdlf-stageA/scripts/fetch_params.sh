#!/bin/bash
pflag=false
oflag=false
tflag=false
LAMBDA_DIR=$PWD

usage () { echo "
    -h -- Opens up this help message.
    -t -- Path to CloudFormation template used for testing.
    -o -- Format to output parameters in. [J] for JSON or [C] for CLI.
    -p -- AWS profile to use to run the AWS commands in this script. [OPTIONAL: Uses 'default', if unspecified.]

    IMPORTANT: All directories provided to this script refer to a ROOT_DIR environment variable, which reference the parent directory of an individual Lambda function.
    
REQUIRED: Use -o to specify the format of the output. [J] for JSON or [C] for CLI.
"; }

options=':o:p:t:h'
while getopts $options option
do
    case "$option" in
        o  ) oflag=true; OUTPUT=${OPTARG};;
        p  ) pflag=true; PROFILE=${OPTARG};;
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
if ! $oflag
then
    echo "-o must be specified" >&2
    exit 1
fi
if ! $pflag
then
    echo "Using default profile..."
    PROFILE="default"
fi
if ! $tflag
then
    echo "-t must be specified" >&2
    exit 1
fi

cd $ROOT_DIR
keylist=()
while IFS='' read -r line; do
    keylist+=("$line")
done < <(cat $TEST_TEMPLATE | yq .Parameters | yq -r 'to_entries[] | select(.value.Type | contains("SSM::Parameter")) | .key')

arr=()
while IFS='' read -r line; do
    arr+=("$line")
done < <(cat $TEST_TEMPLATE | yq .Parameters | yq ['.[] | select(.Type | contains("SSM::Parameter"))'] | yq -r '.[] | .Default')
# printf "%s\n" ${arr[@]}

json () {
    echo "Generating JSON parameter file..."
    filepath="$LAMBDA_DIR/test/resources/parameters.json"
    len=${#arr[@]}
    > $filepath
    for ((i=0; i<$len; i++))
    do
        if [[ "$i" == 0 ]];then
            printf '[\n  {' >> $filepath
        else
            printf '\n  },\n  {' >> $filepath
        fi

        param=${keylist[i]}
        elem=${arr[i]}
        ssmparam=$(aws ssm --profile $PROFILE get-parameter --name $elem | jq .Parameter.Value)
        printf "\n    \"ParameterKey\": \"%s\",\n    \"ParameterValue\": %s" $param $ssmparam >> $filepath

        if [[ "$i" == "$((len-1))" ]];then
            printf ''
            printf '\n  }\n]' >> $filepath
        fi
    done
    echo "JSON parameter file successfully created!"
}

cli () {
    echo "Generating .cfg parameter file..."
    filepath="$LAMBDA_DIR/test/resources/parameters.cfg"
    len=${#arr[@]}
    > $filepath
    for ((i=0; i<$len; i++))
    do
        param=${keylist[i]}
        elem=${arr[i]}
        ssmparam=$(aws ssm --profile $PROFILE get-parameter --name $elem | jq .Parameter.Value)
        printf "ParameterKey=%s,ParameterValue=%s " $param $ssmparam >> $filepath
    done
    echo "CLI parameter file successfully created!"
}

if [ $OUTPUT == "J" ] || [ $OUTPUT == "j" ]; then
    > $LAMBDA_DIR/test/resources/parameters.json
    echo "Generating parameters in JSON format"
    json;
elif [ $OUTPUT == "C" ] || [ $OUTPUT == "c" ]; then
    > $LAMBDA_DIR/test/resources/parameters.cfg
    echo "Generating parameters for use with the CLI"
    cli;
else
    echo "Format unrecognized. Aborted."
    exit 1
fi
