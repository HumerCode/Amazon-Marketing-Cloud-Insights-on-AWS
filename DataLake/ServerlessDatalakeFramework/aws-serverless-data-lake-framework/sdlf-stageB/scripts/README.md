# Running Tests with Lambda Layers Locally
This `README` assumes that your file structure looks like the following, and that you will be calling `test_layer.sh` from `pre-state-machine/`:

    pre-state-machine/
    ├── README.md
    ├── scripts
    │   ├── README.md
    │   ├── fetch_params.sh
    │   └── test_local_layer.sh
    ├── ...
    ├── stage-a-process-object
    │   ├── README.md
    │   ├── src
    │   │   └── lambda_function.py
    │   ├── template.yaml
    │   └── test
    │       ├── lambda_payloads.json
    │       ├── resources
    │       │   ├── input.json
    │       │   ├── parameters.cfg
    │       │   └── parameters.json
    │       └── test_template.yaml
    ├── template.yaml
    └── test_layer.sh

## Prerequisites
Install SAM Local, referring to the [documentation](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install-linux.html).

Once SAM has been installed, please ensure that each Lambda function has a test CloudFormation template that SAM Local will use to generate your Lambda function locally. The template should look like the following for each individual Lambda function:

    AWSTemplateFormatVersion: '2010-09-09'
    Transform: 'AWS::Serverless-2016-10-31'

    Parameters:
        pDynamoObjectMetadata:
            Description: "DynamoDB object catalog"
            Type: "AWS::SSM::Parameter::Value<String>"
            Default: "/SDLF/Dynamo/ObjectCatalog"
        <...>

    Resources:
        RequestsLayer:
            Type: 'AWS::Serverless::LayerVersion'
            Properties:
            LayerName: datalake_library
            CompatibleRuntimes:
                - python3.6
                - python3.7
            ContentUri: ../../../../datalake_library

        # NOTE: Please ensure this Logical ID is alphanumeric and contains no symbols!
        TransformA:
            Type: 'AWS::Serverless::Function'
            Properties:
            CodeUri: ../src
            Environment:
                Variables:
                DYNAMO_OBJECT_CATALOG: !Ref pDynamoObjectMetadata
                <...>
            Handler: lambda_function.lambda_handler
            MemorySize: 128
            Role: <...>
            Runtime: python3.6
            Timeout: 3
            Layers: 
                - !Ref RequestsLayer

## Usage
To run your test with local Lambda Layers, and to view the output from your function, simply run the following command:

    ./test_layer.sh -p <AWS_PROFILE> -e <PATH_TO_INPUT_EVENT> -l <PATH_T0_LAMBDA_FUNCTION> -t <PATH_TO_TEST_LAMBDA_TEMPLATE>

As an example:

    ./test_layer.sh -p default -e stage-a-process-object/test/resources/input.json -l stage-a-process-object/ -t stage-a-process-object/test/test_template.yaml 

The script takes the following flags:

- `-h` - Opens up the help message.
- `-e` - Path to input event.
- `-t` - Path to CloudFormation template used for testing.
- `-l` - Lambda directory to run test on (e.g. `stage-a-process-object/`).
- `-o` - Format to create output parameters in. "C" for `.cfg` file, and "J" for `.json`. *[OPTIONAL: Outputs to `.cfg` file by default for use with CLI.]*
- `-p` - AWS profile to use to run the AWS commands in this script. *[OPTIONAL: Uses `default`, if unspecified.]*
- `-f` - Path to file containing parameters. *[OPTIONAL: Uses auto-generated `.cfg` file by default.]*

The script will auto-generate a parameters file, either in JSON or `.cfg`, by querying against SSM for your parameters so that it can be used in the Lambda function.

## Troubleshooting
### Error 400: Bad Request
Please ensure that your AWS credential tokens are up-to-date and that they have not expired.