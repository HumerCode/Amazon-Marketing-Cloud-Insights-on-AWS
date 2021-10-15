#!/bin/bash
fflag=false
pflag=false
eflag=false
tflag=false
oflag=false
lflag=false

usage () { echo "
    -h -- Opens up this help message.
    -e -- Path to event JSON file.
    -t -- Path to CloudFormation template used for testing.
    -l -- Lambda directory to run test on (e.g. stage-a-process-object/).
    -o -- Format to output parameters in. [OPTIONAL: Outputs to .cfg file by default for use with CLI.]
    -p -- AWS profile to use to run the AWS commands in this script. [OPTIONAL: Uses 'default', if unspecified.]
    -f -- Path to file containing parameters. [OPTIONAL: Uses auto-generated .cfg file by default.]

"; }

options=':p:f:e:t:o:l:h'
while getopts $options option
do
    case "$option" in
        f  ) fflag=true; FILEPATH=${OPTARG};;
        p  ) pflag=true; PROFILE=${OPTARG};;
        e  ) eflag=true; EVENT=${OPTARG};;
        t  ) tflag=true; TEST_TEMPLATE=${OPTARG};;
        o  ) oflag=true; OUTPUT=${OPTARG};;
        l  ) lflag=true; LAMBDA=${OPTARG};;
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
    echo "Parameter file not specified. Using generated .cfg file by default..."
    no_slash=$(echo $LAMBDA | sed 's:/*$::')
    echo "NOTE: Generated file will be in the following directory: $no_slash/test/resources"
    FILEPATH="$no_slash/test/resources/parameters.cfg"
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
if ! $oflag
then
    echo "Parameter output format not specified. Creating .cfg file by default..."
    OUTPUT="C"
fi
if ! $lflag
then
    echo "-l must be specified" >&2
    exit 1
fi

root_dir=$PWD
cd $LAMBDA
ROOT_DIR=$root_dir ../scripts/fetch_params.sh -p $PROFILE -o $OUTPUT -t $TEST_TEMPLATE

ROOT_DIR=$root_dir $root_dir/scripts/test_local_layer.sh -p $PROFILE -f $FILEPATH -e $EVENT -t $TEST_TEMPLATE