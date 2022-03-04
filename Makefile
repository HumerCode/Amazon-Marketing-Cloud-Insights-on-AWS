SHELL=/bin/bash
CICD=quickstart
CHILD=quickstart
REGION=$(shell aws configure get region --profile ${CICD})
ENV=dev

.PHONY: create_repositories delete_repositories create_vpc bootstrap_accounts ca_login install_libraries deploy_artifacts deploy_satellite deploy_all insert_tps_records create_workflows

delete_all: empty_buckets delete_adk delete_bootstrap delete_repositories delete_all_items

help:
	@echo "Helper for the aws-ddk MakeFile";
	@echo "For clean up of the solution execute the following command";
	@echo "make delete_all CICD=\"<your_cicd_profile>\" REGION=\"<the_deployment_region>\"";
	@echo "-------------------------------------------\n";
	@echo "To deploy analytics delivery kit, execute the following command";
	@echo "make deploy_adk CICD=\"<your_cicd_profile>\"";
	@echo "-------------------------------------------\n";


delete_repositories:
	./scripts/cleanup_scripts/delete_repositories.sh -s ${CICD} -t ${CHILD} -r ${REGION} -d ddk-amc-quickstart

empty_buckets:	
	pushd scripts/cleanup_scripts; python3 ./list_items_to_delete.py; popd;
	pushd scripts/cleanup_scripts; python3 ./empty_buckets.py; popd;
	
delete_adk:
	cdk destroy ddk-amc-quickstart-pipeline \
	AMC-${ENV}-QuickStart/amc-foundations \
	AMC-${ENV}-QuickStart/amc-data-lake-pipeline \
	AMC-${ENV}-QuickStart/amc-platform-manager \
	AMC-${ENV}-QuickStart/amc-tps \
	AMC-${ENV}-QuickStart/amc-wfm \
	AMC-${ENV}-QuickStart/amc-data-lake-datasets --force --profile ${CICD};

	
delete_bootstrap:
	aws cloudformation delete-stack --stack-name DdkDevBootstrap --profile ${CICD}

delete_all_items:
	sleep 180
	pushd scripts/cleanup_scripts; python3 ./list_items_to_delete.py; popd;
	pushd scripts/cleanup_scripts; python3 ./delete_script.py; popd;

insert_tps_records: 
	./scripts/microservice_scripts/insert_tps_records.sh -s ${CICD} -t ${CHILD} -r ${REGION} -e ${ENV}

create_workflows: 
	pushd scripts/microservice_scripts; python3 ./initial-load-AmcWorkflowLibrary.py  ${REGION} ${CHILD} ${ENV}; popd;




