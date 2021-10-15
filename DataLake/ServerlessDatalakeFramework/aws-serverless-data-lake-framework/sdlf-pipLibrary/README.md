# Pip Libraries
This repository contains the `requirements.txt` files that should be turned into Lambda Layers. These Lambda Layers can be shared across all the Lambda functions owned by a team, and should contain the libraries that are commonly used across the Lambda functions.

The file structure of the repository should look like the following:

    .
    ├── AWSDataWrangler
    │   └── external_layers.json
    ├── Pandas
    │   └── requirements.txt
    ├── README.md
    ├── build.sh
    └── requirements.txt

The `requirements.txt` on the root of the folder will be built into a **default** layer and should only contain the Pip packages that are most commonly shared across the Lambda functions.

***IMPORTANT**: This repository must contain a `requirements.txt` file at the root that builds into a default layer. Failure to include a default layer will result in the **PIPELINE** infrastructure failing to deploy.*

Additional subdirectories can be created in this repository, and these subdirectories will be published as their own layer. As an example, to create a Lambda Layer named `CliUtilities`, a subdirectory should be added to the repository, making the file structure look like the following:

    .
    ├── CliUtilities
    │   └── requirements.txt
    ├── AWSDataWrangler
    │   └── external_layers.json
    ├── Pandas
    │   └── requirements.txt
    ├── README.md
    ├── build.sh
    └── requirements.txt   

The pipeline will trigger upon a `git push` operation to this repository, and `CliUtilities` will automatically be built, packaged and published as a Lambda Layer, giving us a Layer with the following ARN:

    arn:aws:lambda:eu-west-1:012345678910:layer:CliUtilities:1

Note that the name of the subdirectory has become the name of the Lambda Layer. These Lambda Layers can now be referenced directly by Lambda functions.

It is possible to use these additional Layers in other CloudFormation templates. Start by adding a new subdirectory to this repository containing a `requirements.txt`, making sure that the subdirectory has the same name as intended for the Lambda Layer (**case-sensitive**). Wait for the Pipeline to build the new layer. Reference the Lambda Layer like the following:

    Resources:
        function-a:
            Type: 'AWS::Serverless::Function'
            MemorySize: 128
            Role: !Sub arn:aws:iam::${AWS::AccountId}:role/service-role/role
            Timeout: 300
            Layers: 
                - !Sub arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:layer:CliUtilities:1

***IMPORTANT**: Please note that the lambda layer version should be updated each time you decide to rebuild the layer.*

## requirements.txt
The easiest way to make a `requirements.txt` file is to use Python [virtual environments](https://docs.python.org/3/tutorial/venv.html). It is good practice to use virtual environments at all times, and it is worth becoming familiar with the tool. With your virtual environment active, use the following command to generate the file:

    pip freeze > requirements.txt

If virtual environments cannot be used, then the `requirements.txt` file can be created manually, and should look like the following:

    numpy==1.17.2
    pandas==0.24.0
    yq==2.7.2

This guarantees that the Pip libraries are of a consistent version across the Lambda functions. 

***IMPORTANT**: Please pay close attention to the size of these libraries, as described in further detail in the Limits section of this document.*

## external_layers.json
It's also possible to add pre-existing zipped layers without having to build them first. In this case, an `external_layers.json` specifying a URL link into the zipped layer is used instead of a `requirements.txt` file. Refer to the `AWSDataWrangler` repository for an example.

## external_wheels.json
It's also possible to add pre-existing wheel files for external dependencies. These can be used with Glue Python Shell and/or Glue PySpark jobs. In this case, an `external_wheels.json` specifying a URL link into the wheel files is used instead of a `requirements.txt` or `external_layers.json` file. Refer to the `AWSDataWrangler` repository for an example.

## Limits
Due to the limits placed on the size of packages uploaded to Lambda, the combined size of the Pip libraries listed in each `requirements.txt` must not exceed 50MB when zipped, and no more than 250Mb unzipped. 

For example, a `requirements.txt` file may contain more than one Pip library like the following:

    isort==4.3.21
    typed-ast==1.4.0
    xmltodict==0.12.0
    yq==2.7.2

However, the size of the Pip libraries `isort`, `typed-ast`, `xmltodict` and `yq` must not exceed 50MB when added together.