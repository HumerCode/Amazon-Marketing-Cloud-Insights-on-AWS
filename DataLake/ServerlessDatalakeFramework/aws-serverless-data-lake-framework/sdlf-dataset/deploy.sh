#!/bin/bash
set -e

PROFILE='new-profile'
aws configure --profile $PROFILE set credential_source EcsContainer
REGION=$AWS_REGION

for directory in *; do
    if [ -d ${directory} ]; then
        # Will not run if no directories are available
        cd $directory
        echo $directory
        echo "---------------------------------------------------------------------------------"
        echo "Start deploying $directory resources..."
        echo "---------------------------------------------------------------------------------"
            chmod +x ./deploy.sh
            ./deploy.sh -p new-profile
        cd ../
        echo "---------------------------------------------------------------------------------"
        echo "End deploying $directory resources..."
        echo "---------------------------------------------------------------------------------"
    fi
done