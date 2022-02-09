SHELL=/bin/bash
CICD=default
CHILD=default
REGION=$(shell aws configure get region --profile ${CICD})
VPC=""
NUM_AZS=2
ENV=dev

.PHONY: create_repositories delete_repositories create_vpc bootstrap_accounts ca_login install_libraries deploy_artifacts deploy_satellite deploy_all insert_tps_records create_workflows

all: create_repositories bootstrap_accounts ca_login install_libraries deploy_satellite insert_tps_records create_workflows

delete_all: empty_buckets delete_satellite delete_artifacts delete_bootstrap delete_repositories delete_all_items

help:
	@echo "Helper for the Orion MakeFile";
	@echo "For a fresh install execute the following command";
	@echo "make CICD=\"<your_cicd_profile>\" CHILD=\"<your_child_profile>\" REGION=\"<the_deployment_region>\" ENV=\"<the_environment>\"";
	@echo "-------------------------------------------\n";
	@echo "For installing libraries, execute the following command";
	@echo "make install_libraries";
	@echo "-------------------------------------------\n";
	@echo "For creating repositories, execute the following command";
	@echo "make create_repositories CICD=\"<your_cicd_profile>\" REGION=\"<the_deployment_region>\"";
	@echo "-------------------------------------------\n";
	@echo "For deleting repositories execute the following command";
	@echo "make delete_repositories CICD=\"<your_cicd_profile>\" REGION=\"<the_deployment_region>\"";
	@echo "-------------------------------------------\n";
	@echo "For creating VPC execute the following command";
	@echo "make create_vpc PROFILE=\"<your_aws_profile>\" REGION=\"<the_deployment_region>\" ENV=\"<the_environment>\" NUM_AZS=\"<number_of_azs>\"";
	@echo "-------------------------------------------\n";
	@echo "To bootstrap the accounts, execute the following command"
	@echo "make bootstrap_accounts CICD=\"<your_cicd_profile>\" CHILD=\"<your_child_profile>\" REGION=\"<the_deployment_region>\" ENV=\"<the_environment>\" VPC=\"<vpc_id>\"";
	@echo "-------------------------------------------\n";
	@echo "To create repositories and bootstrap the accounts, execute the following command";
	@echo "make bootstrap CICD=\"<your_cicd_profile>\" CHILD=\"<your_child_profile>\" REGION=\"<the_deployment_region>\" ENV=\"<the_environment>\"";
	@echo "-------------------------------------------\n";
	@echo "To deploy Artifacts, execute the following command";
	@echo "make deploy_artifacts CICD=\"<your_cicd_profile>\"";
	@echo "-------------------------------------------\n";
	@echo "To deploy Satellite, execute the following command";
	@echo "make deploy_satellite CICD=\"<your_cicd_profile>\"";
	@echo "-------------------------------------------\n";
	@echo "To deploy everything, execute the following command";
	@echo "make deploy_all CICD=\"<your_cicd_profile>\"";
	@echo "-------------------------------------------\n";

create_repositories:
	./scripts/deploy_scripts/create_repositories.sh -s ${CICD} -t ${CHILD} -r ${REGION}

delete_repositories:
	./scripts/cleanup_scripts/delete_repositories.sh -s ${CICD} -t ${CHILD} -r ${REGION}

create_vpc:
	./scripts/deploy_scripts/create_vpc.sh -p ${PROFILE} -r ${REGION} -e ${ENV} -a ${NUM_AZS}

bootstrap_accounts:
	./scripts/deploy_scripts/bootstrap_accounts.sh -s ${CICD} -t ${CHILD} -r ${REGION} -e ${ENV} -v ${VPC} -f -c

ca_login:
	./scripts/deploy_scripts/codeartifact_login.sh -s ${CICD} -r ${REGION}

install_libraries:
	pip install -r requirements-dev.txt
	pip install -r ./orion-commons/requirements.txt
	pip install -r ./orion-artifacts/requirements.txt 
	pip install -r ./orion-satellite/requirements.txt 

deploy_artifacts:
	pushd orion-artifacts; npx cdk deploy --require-approval never --profile ${CICD}; popd;

deploy_satellite:
	pushd orion-satellite; npx cdk deploy --require-approval never --profile ${CICD}; popd;
	sleep 300
	./scripts/deploy_scripts/wait_for_satellite.sh

deploy_all: deploy_artifacts deploy_satellite

bootstrap: create_repositories bootstrap_accounts ca_login

insert_tps_records: 
	./scripts/microservice_scripts/insert_tps_records.sh -s ${CICD} -t ${CHILD} -r ${REGION}

create_workflows: 
	pushd scripts/microservice_scripts; python3 ./initial-load-AmcWorkflowLibrary.py wfm-demoteam-dlhs-AMCWorkflowLibrary ${REGION} ${CHILD}; popd;

empty_buckets:	
	pushd scripts/cleanup_scripts; python3 ./list_items_to_delete.py; popd;
	pushd scripts/cleanup_scripts; python3 ./empty_buckets.py; popd;
	
delete_satellite:
	pushd orion-satellite; cdk destroy orion-dev-satellite/orion-foundations \
	orion-dev-satellite/orion-sdlf-pipeline \
	orion-dev-satellite/orion-sdlf-datasets \
	orion-dev-microservices/orion-platform-manager \
	orion-dev-microservices/orion-tps \
	orion-dev-microservices/orion-wfm \
	orion-cicd-satellite/orion-cicd-event-rule \
	orion-satellite-pipeline --force --profile ${CICD}; popd;
	
delete_artifacts:
	pushd orion-artifacts; cdk destroy orion-cicd-artifacts-pipeline \
	orion-dev-artifacts/base \
	orion-dev-artifacts/layers \
	orion-dev-artifacts/glue --force --profile ${CICD}; popd;
	
delete_bootstrap:
	aws cloudformation delete-stack --stack-name orion-dev-bootstrap --profile ${CICD}
	aws cloudformation delete-stack --stack-name orion-cicd-bootstrap --profile ${CICD}
	
delete_all_items:
	sleep 300
	pushd scripts/cleanup_scripts; python3 ./list_items_to_delete.py; popd;
	pushd scripts/cleanup_scripts; python3 ./delete_script.py; popd;






