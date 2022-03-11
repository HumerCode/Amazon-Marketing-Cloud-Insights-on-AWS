# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
from aws_cdk.aws_kms import Key, IKey
from aws_cdk.aws_lambda import Code, LayerVersion, StartingPosition, Runtime
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk.aws_iam import Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal
import aws_cdk as cdk
from aws_ddk_core.base import BaseStack
from aws_cdk.aws_ssm import StringParameter
from aws_cdk.aws_sns import Topic, Subscription, SubscriptionProtocol
import aws_cdk.aws_dynamodb as DDB
from aws_cdk.aws_athena import CfnWorkGroup
from aws_cdk.aws_s3 import Bucket, IBucket
from aws_cdk.aws_events import CfnRule
from aws_ddk_core.resources import KMSFactory, LambdaFactory


def get_ssm_value(scope, id: str, parameter_name: str) -> str:
    return StringParameter.from_string_parameter_name(
        scope,
        id=id,
        string_parameter_name=parameter_name,
    ).string_value

class WorkFlowManagerService(BaseStack):
    def __init__(
        self,
        scope,
        construct_id: str,
        environment_id: str,
        team: str,
        microservice: str,
        resource_prefix: str,
        sns_email: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, environment_id, **kwargs)

        self._environment_id: str = environment_id
        self._region = f"{cdk.Aws.REGION}"
        self._microservice_name = microservice
        self._team = team
        self._resource_prefix = resource_prefix
        self._sns_email = sns_email

        self._athena_bucket_key: IKey = Key.from_key_arn(
            self,
            "athena-bucket-key",
            key_arn=get_ssm_value(
                self,
                "athena-bucket-key-arn-ssm",
                parameter_name="/AMC/KMS/AthenaBucketKeyArn",
            ),
        )

        self._athena_bucket: IBucket = Bucket.from_bucket_arn(
            self,
            "athena-bucket",
            bucket_arn=get_ssm_value(self, "athena-bucket-arn-ssm", parameter_name="/AMC/S3/AthenaBucketArn"),
        )

        self._wfm_masker_key = KMSFactory.key(
            self,
            id=f"{self._microservice_name}-{self._team}-master-key",
            environment_id = self._environment_id,
            description=f"{self._resource_prefix} WFM Service Master Key",
            alias=f"{self._resource_prefix}-{self._microservice_name}-{self._team}-master-key",
            enable_key_rotation=True,
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )
        StringParameter(
            self,
            f"{self._resource_prefix}-{self._microservice_name}-{self._team}-master-key-arn-ssm",
            parameter_name=f"/AMC/KMS/{self._microservice_name}/{self._team}/MasterKey",
            string_value=self._wfm_masker_key.key_arn,
        )
        
        # Athena WorkGroup
        self._create_athena_workgroup(workgroup_name=f"{self._microservice_name}-{self._team}-AthenaWorkGroup-{self._environment_id}")
        
        # DDB Tables
        self._customer_config_table = self._create_ddb_table(
            name=f"{self._microservice_name}-{self._team}-CustomerConfig",
            ddb_props={"partition_key": DDB.Attribute(name="customerId", type=DDB.AttributeType.STRING)},
        )

        self._amc_workflows_table = self._create_ddb_table(
            name=f"{self._microservice_name}-{self._team}-AMCWorkflows",
            ddb_props={
                "partition_key": DDB.Attribute(name="customerId", type=DDB.AttributeType.STRING),
                "sort_key": DDB.Attribute(name="workflowId", type=DDB.AttributeType.STRING)
                },
        )

        self._amc_workflow_library_table = self._create_ddb_table(
            name=f"{self._microservice_name}-{self._team}-AMCWorkflowLibrary",
            ddb_props={
                "partition_key": DDB.Attribute(name="workflowId", type=DDB.AttributeType.STRING),
                "sort_key": DDB.Attribute(name="version", type=DDB.AttributeType.NUMBER)
                },
        )

        self._amc_workflow_schedules_table = self._create_ddb_table(
            name=f"{self._microservice_name}-{self._team}-AMCWorkflowSchedules",
            ddb_props={
                "partition_key": DDB.Attribute(name="customerId", type=DDB.AttributeType.STRING),
                "sort_key": DDB.Attribute(name="Name", type=DDB.AttributeType.STRING)
                },
        )

        self._amc_execution_status_table = self._create_ddb_table(
            name=f"{self._microservice_name}-{self._team}-AMCExecutionStatus",
            ddb_props={
                "partition_key": DDB.Attribute(name="customerId", type=DDB.AttributeType.STRING),
                "sort_key": DDB.Attribute(name="workflowExecutionId", type=DDB.AttributeType.STRING)
                },
        )

        # SNS Topic Creation
        self._sns_topic = self._create_sns_topic(topic_name_prefix=f"{self._microservice_name}-{self._team}")


        # IAM Role Creation for Lambdas
        self._create_iam_policies()
        
        # Create Lambda Layers
        function_prefix = f"{self._microservice_name}-{self._team}"

        self._wfm_helper_layer = LayerVersion(
            self,
            "WFMHelperLayer",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "data_lake_hydration_service/lambda-layers/wfm-layer/")),
            layer_version_name = f"{function_prefix}-wfm-layer",
            compatible_runtimes=[Runtime.PYTHON_3_6,Runtime.PYTHON_3_7,Runtime.PYTHON_3_8]
        )

        self._powertools_layer = LayerVersion.from_layer_version_arn(
            self,
            id="lambda-powertools",
            layer_version_arn=f"arn:aws:lambda:{self._region}:017000801446:layer:AWSLambdaPowertoolsPython:7"
        )

        # Lambda Function Creation
        self._create_lambdas(function_name_prefix=function_prefix)

        # EventBridge Rules 
        self._rule_execute_workflow_consumer = self._create_cloudwatch_event(
            name = f"{self._microservice_name}-TriggerExecutionQueueConsumer",
            description="Runs the Workflow Execution Queue Consumer Lambda function every 10 minutes",
            schedule = "rate(1 minutes)",
            target_input = '{ "method": "syncExecutionStatuses" }',
            target_function = self._execution_queue_consumer
        )

        self._rule_get_glue_status = self._create_cloudwatch_event(
            name = f"{self._microservice_name}-syncExecutionStatuses",
            description="Runs the amc api interface lambda function every 10 minutes to get all execution statuses",
            schedule = "rate(10 minutes)",
            target_input = '{ "method": "syncExecutionStatuses" }',
            target_function = self._lambda_amc_api_interface
        )

        self._rule_hourly_custom_scheduler = self._create_cloudwatch_event(
            name = f"{self._microservice_name}-CustomSchedulerOnHourly",
            description="Runs the CustomScheduler lambda function on hourly",
            schedule = "rate(1 hour)",
            target_input = '{"query": "custom(H * *)" }',
            target_function = self._lambda_custom_scheduler
        )

        self._rule_daily_custom_scheduler = self._create_cloudwatch_event(
            name = f"{self._microservice_name}-CustomSchedulerOnDaily",
            description="Runs the CustomScheduler lambda function on daily",
            schedule = "rate(1 hour)",
            target_input = '{"query": "custom(D * {H})" }',
            target_function = self._lambda_custom_scheduler
        )

        self._rule_weekly_custom_scheduler = self._create_cloudwatch_event(
            name = f"{self._microservice_name}-CustomSchedulerOnWeekly",
            description="Runs the CustomScheduler lambda function on weekly",
            schedule = "rate(1 hour)",
            target_input = '{"query": "custom(W {D} {H})" }',
            target_function = self._lambda_custom_scheduler
        )

        self._rule_weekly_custom_scheduler = self._create_cloudwatch_event(
            name = f"{self._microservice_name}-CustomSchedulerOnMonthly",
            description="Runs the CustomScheduler lambda function on m onthyl",
            schedule = "rate(1 hour)",
            target_input = '{"query": "custom(M {D} {H})" }',
            target_function = self._lambda_custom_scheduler
        )
    
    # SNS Topic Creation
    def _create_sns_topic(self, topic_name_prefix):

        # Add to WFM Master KMS Key Policy
        self._wfm_masker_key.add_to_resource_policy(
            PolicyStatement(
                effect=Effect.ALLOW,
                actions=[
                    "kms:Encrypt",
                    "kms:Decrypt",
                    "kms:ReEncrypt*",
                    "kms:GenerateDataKey*",
                    "kms:CreateGrant",
                    "kms:DescribeKey"
                ],
                resources=["*"],
                principals=[ServicePrincipal("sns.amazonaws.com")]
            )
        )
        topic_name = f"{topic_name_prefix}-SNSTopic-{self._environment_id}"
        sns_topic = Topic(
            self,
            topic_name,
            topic_name=topic_name,
            master_key=self._wfm_masker_key
        )

        Subscription(
            self,
            "Subscription",
            topic = sns_topic,
            endpoint=self._sns_email,
            protocol=SubscriptionProtocol.EMAIL
        )
        return sns_topic
    
    # Athena WorkGroup
    def _create_athena_workgroup(self, workgroup_name):
        self._athena_workgroup = CfnWorkGroup(
            self,
            id=workgroup_name,
            name=workgroup_name,
            description=f"AthenaWorkgroup used by the {self._microservice_name} {self._team} Service",
            recursive_delete_option=True,
            state="ENABLED",
            work_group_configuration=CfnWorkGroup.WorkGroupConfigurationProperty(
                bytes_scanned_cutoff_per_query=200000000,
                enforce_work_group_configuration=False,
                publish_cloud_watch_metrics_enabled=False,
                requester_pays_enabled=True,
                result_configuration=CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{self._athena_bucket.bucket_name}/{self._microservice_name}-{self._team}-athenaresults/",
                    encryption_configuration=CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_KMS",
                        kms_key=self._athena_bucket_key.key_arn
                    )
                )
            )
        )
        self._athena_workgroup.apply_removal_policy(
            policy = cdk.RemovalPolicy.DESTROY
        )


    # DDB Tables
    def _create_ddb_table(self, name: str, ddb_props: Dict[str, Any]) -> DDB.Table:

        if name.split("-")[-1] == "AMCWorkflowSchedules":
            table: DDB.Table = DDB.Table(
                self,
                f"{name}-{self._environment_id}-table",
                table_name=f"{name}-{self._environment_id}",
                encryption=DDB.TableEncryption.CUSTOMER_MANAGED,
                encryption_key=self._wfm_masker_key,
                stream=DDB.StreamViewType.NEW_AND_OLD_IMAGES,
                billing_mode=DDB.BillingMode.PAY_PER_REQUEST,
                removal_policy= cdk.RemovalPolicy.DESTROY,
                **ddb_props
            )
            table.add_global_secondary_index(
                index_name="custom-schdl-index",
                partition_key=DDB.Attribute(name="ScheduleExpression", type=DDB.AttributeType.STRING),
                sort_key=DDB.Attribute(name="State", type=DDB.AttributeType.STRING)
            )

        elif name.split("-")[-1] == "AMCExecutionStatus":
            table: DDB.Table = DDB.Table(
                self,
                f"{name}-{self._environment_id}-table",
                table_name=f"{name}-{self._environment_id}",
                encryption=DDB.TableEncryption.CUSTOMER_MANAGED,
                encryption_key=self._wfm_masker_key,
                stream=DDB.StreamViewType.NEW_AND_OLD_IMAGES,
                billing_mode=DDB.BillingMode.PAY_PER_REQUEST,
                removal_policy= cdk.RemovalPolicy.DESTROY,
                **ddb_props
            )
              
            table.add_global_secondary_index(
                index_name="executionStatus-workflowId-index",
                partition_key=DDB.Attribute(name="customerId", type=DDB.AttributeType.STRING),
                sort_key=DDB.Attribute(name="executionStatus", type=DDB.AttributeType.STRING),
                non_key_attributes=[
                    "workflowExecutionId",
                    "workflowId",
                    "createTime",
                    "invalidationOffsetSecs",
                    "lastUpdatedTime",
                    "outputS3URI",
                    "timeWindowStart",
                    "timeWindowStartOriginal",
                    "timeWindowEnd",
                    "timeWindowEndOriginal",
                    "parameterValues",
                    "timeWindowType",
                    "statusReason"
                ],
                projection_type=DDB.ProjectionType.INCLUDE
            )
        else:
            table: DDB.Table = DDB.Table(
                self,
                f"{name}-{self._environment_id}-table",
                table_name=f"{name}-{self._environment_id}",
                encryption=DDB.TableEncryption.CUSTOMER_MANAGED,
                encryption_key=self._wfm_masker_key,
                stream=DDB.StreamViewType.NEW_AND_OLD_IMAGES,
                billing_mode=DDB.BillingMode.PAY_PER_REQUEST,
                removal_policy= cdk.RemovalPolicy.DESTROY,
                **ddb_props,
            )

        StringParameter(
            self,
            f"{name}-table-arn-ssm",
            parameter_name=f"/AMC/DynamoDB/{name.title()}TableArn",
            string_value=table.table_arn,
        )

        return table


    def _create_cloudwatch_event(self, name, description, schedule, target_input, target_function):
        
        event_rule = CfnRule(
            self,
            f"{name}-rule",
            state = "ENABLED",
            description = description,
            name = name,
            schedule_expression=schedule,
            targets=[CfnRule.TargetProperty(
                arn=target_function.function_arn,
                id=name,
                input = target_input
            )]
        )
        
        target_function.add_permission(
            f"{name}",
            principal=ServicePrincipal("events.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=event_rule.attr_arn
        )

        return event_rule


    # Create Lambda Functions
    def _create_lambdas(self, function_name_prefix):
        
        # SyncWorkflowStatuses
        lambda_sync_workflow_status = LambdaFactory.function(
            self,
            f"{function_name_prefix}-SyncWorkflowStatuses-{self._environment_id}",
            environment_id = self._environment_id,
            function_name=f"{function_name_prefix}-SyncWorkflowStatuses-{self._environment_id}",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "data_lake_hydration_service/lambdas/sync_workflow_status")),
            handler="handler.lambda_handler",
            description="Synchronizes workflow execution statues from AMC to a dynamoDB Table",
            memory_size=2048,
            timeout=cdk.Duration.minutes(15),
            runtime = Runtime.PYTHON_3_8,
            layers = [self._wfm_helper_layer, self._powertools_layer],
            environment={
                "CUSTOMERS_DYNAMODB_TABLE": self._customer_config_table.table_name,
                "DEFAULT_DYNAMODB_RECORD_UPDATE_BATCH_SIZE": "50",
                "DEFAULT_DYNAMODB_BATCH_DELAY_SECONDS": "3"
            },
            role=self._sync_workflow_status_role
        )

        # GenerateDateRangeValues
        LambdaFactory.function(
            self,
            f"{function_name_prefix}-GenerateDateRangeValues-{self._environment_id}",
            environment_id=self._environment_id,
            function_name=f"{function_name_prefix}-GenerateDateRangeValues-{self._environment_id}",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "data_lake_hydration_service/lambdas/generate_data_range")),
            handler="handler.lambda_handler",
            description="Generates date range values which can be used in submitting multiple workflow executions",
            memory_size=2048,
            timeout=cdk.Duration.minutes(15),
            runtime = Runtime.PYTHON_3_8,
            layers = [self._wfm_helper_layer, self._powertools_layer],
        )

        # GenerateExecutionResubmissions
        LambdaFactory.function(
            self,
            f"{function_name_prefix}-GenerateExecutionResubmissions-{self._environment_id}",
            environment_id = self._environment_id,
            function_name=f"{function_name_prefix}-GenerateExecutionResubmissions-{self._environment_id}",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "data_lake_hydration_service/lambdas/generate_execution_resubmission")),
            handler="handler.lambda_handler",
            description="Generates execution resubmissions",
            memory_size=2048,
            timeout=cdk.Duration.minutes(15),
            runtime = Runtime.PYTHON_3_8,
            layers = [self._wfm_helper_layer, self._powertools_layer],
            environment={
                "CUSTOMERS_DYNAMODB_TABLE": self._customer_config_table.table_name,
                "EXECUTION_STATUS_TABLE": self._amc_execution_status_table.table_name
            },
            role=self._generate_resubmission_role
        )

        # WorkflowStatusTableTrigger
        workflow_status_trigger = LambdaFactory.function(
            self,
            f"{function_name_prefix}-WorkflowStatusTrigger-{self._environment_id}",
            environment_id = self._environment_id,
            function_name=f"{function_name_prefix}-WorkflowStatusTrigger-{self._environment_id}",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "data_lake_hydration_service/lambdas/workflow_status_trigger")),
            handler="handler.lambda_handler",
            description="A lambda function that process a DynamoDB Stream of workflow statuses to generate alerts",
            memory_size=2048,
            timeout=cdk.Duration.minutes(15),
            runtime = Runtime.PYTHON_3_8,
            layers = [self._wfm_helper_layer, self._powertools_layer],
            environment={
                "CUSTOMERS_DYNAMODB_TABLE": self._customer_config_table.table_name,
                "EXECUTION_STATUS_TABLE": self._amc_execution_status_table.table_name,
                "IGNORE_STATUS_LIST": "PENDING,RUNNING,SUCCEEDED,PUBLISHING"
            },
            role=self._workflow_status_trigger_role
        )

        workflow_status_trigger.add_event_source_mapping(
            "lambda-ddb-event-source-mapping",
            batch_size=5,
            event_source_arn=self._amc_execution_status_table.table_stream_arn,
            starting_position=StartingPosition.TRIM_HORIZON,
            retry_attempts=1
        )

        # AMC API Interface
        self._lambda_amc_api_interface = LambdaFactory.function(
            self,
            f"{function_name_prefix}-AmcApiInterface-{self._environment_id}",
            environment_id = self._environment_id,
            function_name=f"{function_name_prefix}-AmcApiInterface-{self._environment_id}",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "data_lake_hydration_service/lambdas/amc_api_interface")),
            handler="handler.lambda_handler",
            description="A lambda interface that acts as a wrapper for the AMC REST API",
            memory_size=2048,
            timeout=cdk.Duration.minutes(15),
            runtime = Runtime.PYTHON_3_8,
            layers = [self._wfm_helper_layer, self._powertools_layer],
            environment={
                "CUSTOMERS_DYNAMODB_TABLE": self._customer_config_table.table_name,
                "SYNC_WORKFLOW_STATUSES_LAMBDA_FUNCTION_NAME": lambda_sync_workflow_status.function_name
            },
            role=self._amc_api_interface_role
        )

        # WorkflowTableTrigger
        workflow_table_trigger = LambdaFactory.function(
            self,
            f"{function_name_prefix}-WorkflowTableTrigger-{self._environment_id}",
            environment_id = self._environment_id,
            function_name=f"{function_name_prefix}-WorkflowTableTrigger-{self._environment_id}",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "data_lake_hydration_service/lambdas/workflow_table_trigger")),
            handler="handler.lambda_handler",
            description="Synchronizes workflow table records from the workflow DyanmoDB table to AMC",
            memory_size=128,
            timeout=cdk.Duration.minutes(5),
            runtime = Runtime.PYTHON_3_8,
            layers = [self._wfm_helper_layer, self._powertools_layer],
            environment={
                "AMC_API_INTERFACE_FUNCTION_NAME": self._lambda_amc_api_interface.function_name,
                "WORKFLOWS_TABLE_NAME": self._amc_workflows_table.table_name
            },
            role=self._workflow_table_trigger_role
        )

        workflow_table_trigger.add_event_source_mapping(
            "lambda-ddb-event-source-mapping",
            batch_size=5,
            event_source_arn=self._amc_workflows_table.table_stream_arn,
            starting_position=StartingPosition.TRIM_HORIZON,
            retry_attempts=1
        )

        # WorkflowExecutionQueueConsumer
        self._execution_queue_consumer = LambdaFactory.function(
            self,
            f"{function_name_prefix}-WorkflowExecutionQueueConsumer-{self._environment_id}",
            environment_id = self._environment_id,
            function_name=f"{function_name_prefix}-WorkflowExecutionQueueConsumer-{self._environment_id}",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "data_lake_hydration_service/lambdas/workflow_queue_consumer")),
            handler="handler.lambda_handler",
            description="Consumes from the Workflow Execution SQS queue and submits them to the AMC API Endpoint as a new exeuction",
            memory_size=2048,
            timeout=cdk.Duration.minutes(15),
            runtime = Runtime.PYTHON_3_8,
            layers = [self._wfm_helper_layer, self._powertools_layer],
            environment={
                "CUSTOMERS_DYNAMODB_TABLE": self._customer_config_table.table_name
            },
            role=self._event_queue_consumer_role
        )

        # Lambda Workflow Execution Queue Producer
        lambda_events_queue_producer = LambdaFactory.function(
            self,
            f"{function_name_prefix}-ExecutionQueueProducer-{self._environment_id}",
            environment_id = self._environment_id,
            function_name=f"{function_name_prefix}-ExecutionQueueProducer-{self._environment_id}",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "data_lake_hydration_service/lambdas/execution_queue_producer")),
            handler="handler.lambda_handler",
            description="Queues a workflow exeuction in SQS to be submitted to the AMC API Interface",
            memory_size=2048,
            timeout=cdk.Duration.minutes(15),
            runtime = Runtime.PYTHON_3_8,
            layers = [self._wfm_helper_layer, self._powertools_layer],
            environment={
                "CUSTOMERS_DYNAMODB_TABLE": self._customer_config_table.table_name
            },
            role=self._event_queue_producer_role 
        )
        lambda_events_queue_producer.add_permission(
            "EventsInvokeProducerLambda",
            principal=ServicePrincipal("events.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"arn:aws:events:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:rule/*"
        )

        # RunWorkflowByCampaign
        lambda_run_by_campaign = LambdaFactory.function(
            self,
            f"{function_name_prefix}-ExecuteWorkflowByCampaign-{self._environment_id}",
            environment_id = self._environment_id,
            function_name=f"{function_name_prefix}-ExecuteWorkflowByCampaign-{self._environment_id}",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "data_lake_hydration_service/lambdas/execute_workflow_by_campaign")),
            handler="handler.lambda_handler",
            description="execute the specified workflow and pass campaignID as a parameter from a specified Athena Table",
            memory_size=2048,
            timeout=cdk.Duration.minutes(15),
            runtime = Runtime.PYTHON_3_8,
            layers = [self._wfm_helper_layer, self._powertools_layer],
            environment={
                "CUSTOMERS_DYNAMODB_TABLE": self._customer_config_table.table_name,
                "ATHENA_WORKGROUP":self._athena_workgroup.name,
                "QUEUE_WORKFLOW_EXECUTION_LAMBDA_FUNCTION_NAME": lambda_events_queue_producer.function_name
            },
            role=self._run_workflow_campaign_role
        )
        lambda_run_by_campaign.add_permission(
            "EventsInvokeRunByCampaignLambda",
            principal=ServicePrincipal("events.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"arn:aws:events:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:rule/*"
        )

        # WorkflowScheduleTrigger
        workflow_schedule_trigger = LambdaFactory.function(
            self,
            f"{function_name_prefix}-WorkflowScheduleTrigger-{self._environment_id}",
            environment_id = self._environment_id,
            function_name=f"{function_name_prefix}-WorkflowScheduleTrigger-{self._environment_id}",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "data_lake_hydration_service/lambdas/workflow_schedule_trigger")),
            handler="handler.lambda_handler",
            description="A Trigger creating Cloudwatch Rules to submit workflow executions to the workflow exeuction queue producer based on records inserted into the WorkflowSchedule DynamoDB Table",
            memory_size=2048,
            timeout=cdk.Duration.minutes(15),
            runtime = Runtime.PYTHON_3_8,
            layers = [self._wfm_helper_layer, self._powertools_layer],
            environment={
                "EXECUTION_QUEUE_PRODUCER_LAMBA_ARN": lambda_events_queue_producer.function_arn,
                "RUN_WORKFLOW_BY_CAMPAIGN_LAMBDA_ARN": lambda_run_by_campaign.function_arn
            },
            role=self._workflow_schedule_trigger_role
        )

        workflow_schedule_trigger.add_event_source_mapping(
            "lambda-ddb-event-source-mapping",
            batch_size=5,
            retry_attempts=1,
            event_source_arn=self._amc_workflow_schedules_table.table_stream_arn,
            starting_position=StartingPosition.TRIM_HORIZON
        )

        # Lambda Workflow Library Trigger
        workflow_library_trigger = LambdaFactory.function(
            self,
            f"{function_name_prefix}-WorkflowLibraryTrigger-{self._environment_id}",
            environment_id = self._environment_id,
            function_name=f"{function_name_prefix}-WorkflowLibraryTrigger-{self._environment_id}",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "data_lake_hydration_service/lambdas/workflow_library_trigger")),
            handler="handler.lambda_handler",
            description="A Trigger that is invoked when records are modified in the Workflow Library table, this will create workflows and schedule them to run based on their default schedule configuration",
            memory_size=2048,
            timeout=cdk.Duration.minutes(15),
            runtime = Runtime.PYTHON_3_8,
            layers = [self._wfm_helper_layer, self._powertools_layer],
            environment={
                "CUSTOMERS_DYNAMODB_TABLE":self._customer_config_table.table_name,
                "WORKFLOW_LIBRARY_DYNAMODB_TABLE":self._amc_workflow_library_table.table_name,
                "WORKFLOWS_TABLE_NAME":self._amc_workflows_table.table_name,
                "WORKFLOW_SCHEDULE_TABLE":self._amc_workflow_schedules_table.table_name,
                "CLOUDWATCH_RULE_NAME_PREFIX": self._microservice_name
            },
            role= self._workflow_library_trigger_role
        )

        workflow_library_trigger.add_event_source_mapping(
            "lambda-ddb-event-source-mapping",
            batch_size=5,
            event_source_arn=self._amc_workflow_library_table.table_stream_arn,
            starting_position=StartingPosition.TRIM_HORIZON,
            retry_attempts=1
        )

        # Lambda Workflow Customer Config Trigger
        customer_config_trigger = LambdaFactory.function(
            self,
            f"{function_name_prefix}-CustomerConfigTrigger-{self._environment_id}",
            environment_id = self._environment_id,
            function_name=f"{function_name_prefix}-CustomerConfigTrigger-{self._environment_id}",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "data_lake_hydration_service/lambdas/customer_config_trigger")),
            handler="handler.lambda_handler",
            description="A Trigger that is invoked when records are modified in the customer config table, this will create an AMC Executions SQS Queue and update the AMC API Invoke policy to include the customer's AMC instance",
            memory_size=2048,
            timeout=cdk.Duration.minutes(15),
            runtime = Runtime.PYTHON_3_8,
            layers = [self._wfm_helper_layer, self._powertools_layer],
            environment={
                "EXECUTION_QUEUE_PRODUCER_LAMBA_ARN": lambda_events_queue_producer.function_arn,
                "WORKFLOW_LIBRARY_TRIGGER_LAMBDA_FUNCTION_NAME": workflow_library_trigger.function_arn,
                "CUSTOMERS_DYNAMODB_TABLE":self._customer_config_table.table_name,
                "KMS_MASTER_KEY":self._wfm_masker_key.key_arn, 
                "TEAM":self._team,
                "MICROSERVICE":self._microservice_name,
                "ENV":self._environment_id,
                "AMC_ENDPOINT_IAM_POLICY_ARN":self._invoke_amc_api_policy.managed_policy_arn
            },
            role=self._customer_config_trigger_role
        )

        customer_config_trigger.add_event_source_mapping(
            "lambda-ddb-event-source-mapping",
            batch_size=5,
            event_source_arn=self._customer_config_table.table_stream_arn,
            starting_position=StartingPosition.TRIM_HORIZON,
            retry_attempts=1
        )

        # Custom Scheduler
        self._lambda_custom_scheduler = LambdaFactory.function(
            self,
            f"{function_name_prefix}-CustomScheduler-{self._environment_id}",
            environment_id = self._environment_id,
            function_name=f"{function_name_prefix}-CustomScheduler-{self._environment_id}",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "data_lake_hydration_service/lambdas/custom_scheduler")),
            handler="handler.lambda_handler",
            description="This function will query workflows based on their frequency from AMCWorkflowSchedules table and pass payload to WorkflowExecutionQueueProducer Lambda",
            memory_size=2048,
            timeout=cdk.Duration.minutes(15),
            runtime = Runtime.PYTHON_3_8,
            layers = [self._wfm_helper_layer, self._powertools_layer],
            environment={
                "EXECUTION_QUEUE_PRODUCER_LAMBA_ARN": lambda_events_queue_producer.function_arn,
                "RUN_WORKFLOW_BY_CAMPAIGN_LAMBDA_ARN":lambda_run_by_campaign.function_arn,
                "WORKFLOW_SCHEDULE_TABLE":self._amc_workflow_schedules_table.table_name,
                "CLOUDWATCH_RULE_NAME_PREFIX": self._microservice_name
            },
            role= self._custom_scheduler_role
        )


    # IAM POLICIES
    def _create_iam_policies(self):
        name_prefix = f"{self._microservice_name}-{self._team}"
        
        # Athena Policies - Allow Workgroup Access
        athena_workgroup_access_policy = ManagedPolicy(
            self,
            f"{name_prefix}-WFM-Athena-WorkgroupAccess-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-WFM-Athena-WorkgroupAccess-1",
            description= "Allow access to Athena Workgroups",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "athena:GetNamedQuery",
                            "athena:CancelQueryExecution",
                            "athena:StartQueryExecution",
                            "athena:StopQueryExecution",
                            "athena:GetWorkGroup",
                            "athena:GetQueryResults",
                            "athena:GetQueryExecution",
                            "athena:BatchGetQueryExecution",
                            "athena:ListQueryExecutions",
                            "athena:GetQueryResultsStream"
                        ],
                        resources=[f"arn:aws:athena:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:workgroup/{name_prefix}-AthenaWorkGroup-{self._environment_id}"]
                    )
                ]
            )
        )

        # Glue Policies - Allow AMC Glue Catalog Access
        glue_catalog_access_policy = ManagedPolicy(
            self,
            f"{name_prefix}-WFM-Glue-CatalogAccess-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-WFM-Glue-CatalogAccess-1",
            description= "Allow access to Glue Catalog",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=["glue:Get*"],
                        resources=[f"arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:catalog"],
                    ), 
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "glue:Get*",
                            "glue:List*"
                        ],
                        resources=[
                            f"arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:database/*",
                            f"arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/*"
                        ]
                    ) 
                ]
            )
        )

        # SQS Policies - Execustion Queue Read Write Policy
        sqs_execution_queue_policy = ManagedPolicy(
            self,
            f"{name_prefix}-SQS-ExecutionQueue-RW-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-SQS-ExecutionQueue-RW-1",
            description= "Allows access to Read and write to the AMC Workflow Execution SQS Queue",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "sqs:List*",
                            "sqs:ReceiveMessage",
                            "sqs:SendMessage*",
                            "sqs:DeleteMessage*",
                            "sqs:GetQueue*"
                        ],
                        resources=[f"arn:aws:sqs:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:{name_prefix}-{self._environment_id}-workflowExecution*"]
                    )
                ]
            )
        ) 

        # S3 Policy - Athena S3 Results
        s3_athena_results_policy = ManagedPolicy(
            self,
            f"{name_prefix}-WFM-S3-AthenaResults-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-WFM-S3-AthenaResults-1",
            description= "Allows access to Read Athena Results From S3",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "s3:PutObject",
                            "s3:AbortMultipartUpload",
                            "s3:ListMultipartUploadParts",
                            "s3:Get*",
                            "s3:List*"
                        ],
                        resources=[f"{self._athena_bucket.bucket_arn}/{name_prefix}-athenaresults/*"],
                    ),
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "s3:GetBucketLocation",
                            "s3:ListBucket"
                        ],
                        resources=[self._athena_bucket.bucket_arn]
                    )
                ]
            )
        ) 

        # KMS Policy - Decrypt SNS SQS Key
        kms_decrypt_snssqs_key_policy = ManagedPolicy(
            self,
            f"{name_prefix}-Workflowmgr-KMS-DecryptSNSSQS-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-Workflowmgr-KMS-DecryptSNSSQS-1",
            description= "Allows using the SNSSQS KMS Key for Decryption",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "kms:DescribeKey",
                            "kms:Encrypt",
                            "kms:Decrypt",
                            "kms:ReEncrypt*",
                            "kms:GenerateDataKey*",
                            "kms:CreateGrant"
                        ],
                        resources=["*"],
                        conditions={
                            "ForAnyValue:StringLike":{
                                "kms:ResourceAliases": [f"alias/{self._resource_prefix}-*","alias/tps-*"]
                            }
                        }
                    )
                ]
            )
        )

        # IAM - Invoke AMC API
        self._invoke_amc_api_policy = ManagedPolicy(
            self,
            f"{name_prefix}-ApiGateway-AMCAPIInvoke-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-ApiGateway-AMCAPIInvoke-1",
            description= "Allows API Invoke on the AMC API Endpoints",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=["execute-api:Invoke"],
                        resources=[f"arn:aws:execute-api:placeholder:{cdk.Aws.ACCOUNT_ID}:placeholder/*"]
                    )
                ]
            )
        )

        # IAM - Modify AMC API Invoke Policy
        modify_amc_api_invoke_policy = ManagedPolicy(
            self,
            f"{name_prefix}-Workflowmgr-DynamoDB-ModifyAMCAPIInvokeIAMPolicy-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-Workflowmgr-DynamoDB-ModifyAMCAPIInvokeIAMPolicy-1",
            description= "Allows updating the ModifyAMCAPIInvokeIAMPolicy policy",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "iam:GetPolicyVersion",
                            "iam:GetPolicy",
                            "iam:ListPolicyVersions",
                            "iam:CreatePolicyVersion",
                            "iam:DeletePolicyVersion"
                        ],
                        resources=[self._invoke_amc_api_policy.managed_policy_arn]
                    )
                ]
            )
        )

        # LakeFormation - GetData Policy
        lakeformation_get_data_policy = ManagedPolicy(
            self,
            f"{name_prefix}-LakeFormation-GetDataAccess-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-LakeFormation-GetDataAccess-1",
            description= "Allows Lake Formation Get Data Access",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=["lakeformation:GetDataAccess"],
                        resources=["*"]
                    )
                ]
            )
        )

        # Lambda - Invoke Execution Queue Producer
        lambda_invoke_execution_producer = ManagedPolicy(
            self,
            f"{name_prefix}-Lambda-ExecutionQueueProducerInvoke-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-Lambda-ExecutionQueueProducerInvoke-1",
            description= "Allows API Invoke on the AMC API Endpoints",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=["lambda:InvokeFunction"],
                        resources=[f"arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:function:{name_prefix}-ExecutionQueueProducer-{self._environment_id}"]
                    )
                ]
            )
        )

        # Lambda - Invoke AMC API Interface
        lambda_invoke_amc_api_interface = ManagedPolicy(
            self,
            f"{name_prefix}-Lambda-AMCApiInterfaceInvoke-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-Lambda-AMCApiInterfaceInvoke-1",
            description= "Allows API Invoke on the AMC API Lambda",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=["lambda:InvokeFunction"],
                        resources=[f"arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:function:{name_prefix}-AmcApiInterface-{self._environment_id}"]
                    )
                ]
            )
        )

        # Lambda - Invoke Workflow Execution Producer
        lambda_invoke_execution_consumer = ManagedPolicy(
            self,
            f"{name_prefix}-Lambda-WorkflowExecutionConsumerInvoke-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-Lambda-WorkflowExecutionConsumerInvoke-1",
            description= "Allows API Invoke on the AMC API Lambda",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=["lambda:InvokeFunction"],
                        resources=[f"arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:function:{name_prefix}-WorkflowExecutionQueueConsumer-{self._environment_id}"]
                    )
                ]
            )
        )

        # Lambda - Invoke Sync Workflow
        lambda_invoke_sync_workflow_policy = ManagedPolicy(
            self,
            f"{name_prefix}-Lambda-SyncWorkflowStatuses-invoke-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-Lambda-SyncWorkflowStatuses-invoke-1",
            description= "Allows Invoke on SyncWorkflow Statuses",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=["lambda:InvokeFunction"],
                        resources=[f"arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:function:{name_prefix}-SyncWorkflowStatuses-{self._environment_id}"]
                    )
                ]
            )
        )

        # Lambda - Update Lambda Permissions Policy
        lambda_rw_policy = ManagedPolicy(
            self,
            f"{name_prefix}-Lambda-permissions-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-Lambda-permissions-1",
            description= "Allows API Invoke on the AMC API Lambda",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "lambda:AddPermission",
                            "lambda:RemovePermission",
                            "lambda:GetPolicy"
                        ],
                        resources=[f"arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:function:{self._microservice_name}-*"]
                    )
                ]
            )
        )

        # SNS - Publish To SNS Policy
        sns_publish_policy = ManagedPolicy(
            self,
            f"{name_prefix}-WFM-SNSPublish-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-WFM-SNSPublish-1",
            description= "Allows Publish to SNS Topic",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=["sns:Publish"],
                        resources=[self._sns_topic.topic_arn]
                    )
                ]
            )
        )

        # DDB - Read AMC Execution Status DynamoDB
        ddb_read_execution_policy = ManagedPolicy(
            self,
            f"{name_prefix}-WFM-DynamoDB-ReadExecutionStatus-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-WFM-DynamoDB-ReadExecutionStatus-1",
            description= "Allows Read DDB Execution Status",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "dynamodb:ListTables",
                            "dynamodb:ListGlobalTables",
                            "dynamodb:Query",
                            "dynamodb:Scan",
                            "dynamodb:GetRecords",
                            "dynamodb:GetShardIterator",
                            "dynamodb:DescribeStream",
                            "dynamodb:ListStreams",
                            "dynamodb:ListShards"
                        ],
                        resources=[
                            self._amc_execution_status_table.table_arn,
                            f"{self._amc_execution_status_table.table_arn}/*"
                        ]
                    )
                ]
            )
        )

        # DDB - Read AMC Execution Status DynamoDB
        ddb_write_execution_policy = ManagedPolicy(
            self,
            f"{name_prefix}-WFM-DynamoDB-WriteExecutionStatus-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-Workflowmgr-DynamoDB-WriteExecutionStatus-1",
            description= "Allows Write AMC Executions Status DynamoDB Table",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "dynamodb:ListTables",
                            "dynamodb:ListGlobalTables",
                            "dynamodb:DescribeTable",
                            "dynamodb:Query",
                            "dynamodb:Scan",
                            "dynamodb:BatchWriteItem",
                            "dynamodb:PutItem",
                            "dynamodb:UpdateItem"
                        ],
                        resources=[
                            self._amc_execution_status_table.table_arn,
                            f"{self._amc_execution_status_table.table_arn}/*"
                        ]
                    )
                ]
            )
        )

        # DDB - Read AMC Workflows DynamoDB
        ddb_read_workflows_policy = ManagedPolicy(
            self,
            f"{name_prefix}-DynamoDB-AMCWorkflows-Read-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-DynamoDB-AMCWorkflows-Read-1",
            description= "Allows Read AMC Workflows DynamoDB Table",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "dynamodb:ListTables",
                            "dynamodb:ListGlobalTables",
                            "dynamodb:ListShards",
                            "dynamodb:Query",
                            "dynamodb:Scan",
                            "dynamodb:GetRecords",
                            "dynamodb:GetShardIterator",
                            "dynamodb:DescribeStream",
                            "dynamodb:ListStreams"
                        ],
                        resources=[
                            self._amc_workflows_table.table_arn,
                            f"{self._amc_workflows_table.table_arn}/*"
                        ]
                    )
                ]
            )
        )

        # DDB - Write Customer Config DynamoDB
        ddb_write_config_policy = ManagedPolicy(
            self,
            f"{name_prefix}-WFM-DynamoDB-CustomerConfig-RW-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-Workflowmgr-DynamoDB-CustomerConfig-RW-1",
            description= "Allows Read and Write Access to Customer Config DDB Table",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "dynamodb:ListTables",
                            "dynamodb:ListGlobalTables",
                            "dynamodb:DescribeTable",
                            "dynamodb:Query",
                            "dynamodb:Scan",
                            "dynamodb:BatchWriteItem",
                            "dynamodb:PutItem",
                            "dynamodb:UpdateItem",
                            "dynamodb:DeleteItem"
                        ],
                        resources=[
                            self._customer_config_table.table_arn,
                            f"{self._customer_config_table.table_arn}/*"
                        ]
                    )
                ]
            )
        )

        # DDB - Read Customer Config DynamoDB
        ddb_read_config_policy = ManagedPolicy(
            self,
            f"{name_prefix}-Workflowmgr-DynamoDB-ReadCustomerConfig-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-Workflowmgr-DynamoDB-ReadCustomerConfig-1",
            description= "Allows Read, Scan and query access on the Customer Config DynamoDB Table",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "dynamodb:ListTables",
                            "dynamodb:ListGlobalTables",
                            "dynamodb:Query",
                            "dynamodb:Scan",
                            "dynamodb:ListShards",
                            "dynamodb:GetRecords",
                            "dynamodb:GetShardIterator",
                            "dynamodb:DescribeStream",
                            "dynamodb:ListStreams"
                        ],
                        resources=[
                            self._customer_config_table.table_arn,
                            f"{self._customer_config_table.table_arn}/*"
                        ]
                    )
                ]
            )
        )

        # DDB - Write Workflows and Workflow Schedules DynamoDB
        ddb_write_schedules_policy = ManagedPolicy(
            self,
            f"{name_prefix}-WorkflowLibraryQueueConsumer-DynamoDB-WorkflowandWorkflowSchedules-Write-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-WorkflowLibraryQueueConsumer-DynamoDB-WorkflowandWorkflowSchedules-Write-1",
            description= "Allows Write access to the Workflows and Workflow Schedules  DynamoDB Tables",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "dynamodb:ListTables",
                            "dynamodb:ListGlobalTables",
                            "dynamodb:ListShards",
                            "dynamodb:ListStreams",
                            "dynamodb:DescribeTable",
                            "dynamodb:DescribeStream",
                            "dynamodb:GetRecords",
                            "dynamodb:GetShardIterator",
                            "dynamodb:Query",
                            "dynamodb:Scan",
                            "dynamodb:BatchWriteItem",
                            "dynamodb:PutItem",
                            "dynamodb:UpdateItem",
                            "dynamodb:DeleteItem"
                        ],
                        resources=[
                            self._amc_workflows_table.table_arn,
                            f"{self._amc_workflows_table.table_arn}/*",
                            self._amc_workflow_schedules_table.table_arn,
                            f"{self._amc_workflow_schedules_table.table_arn}/*",
                        ]
                    )
                ]
            )
        )

        # DDB - Write Workflow Library DynamoDB
        ddb_write_library_policy = ManagedPolicy(
            self,
            f"{name_prefix}-DynamoDB-WriteWorkflowLibrary-Write-1",
            managed_policy_name=f"{name_prefix}-{cdk.Aws.REGION}-DynamoDB-WriteWorkflowLibrary-Write-1",
            description= "Allows Write access to the WorkflowLibrary DynamoDB Table",
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                        effect=Effect.ALLOW,
                        actions=[
                            "dynamodb:ListTables",
                            "dynamodb:ListGlobalTables",
                            "dynamodb:ListShards",
                            "dynamodb:ListStreams",
                            "dynamodb:DescribeTable",
                            "dynamodb:DescribeStream",
                            "dynamodb:GetRecords",
                            "dynamodb:GetShardIterator",
                            "dynamodb:Query",
                            "dynamodb:Scan",
                            "dynamodb:BatchWriteItem",
                            "dynamodb:PutItem",
                            "dynamodb:UpdateItem",
                            "dynamodb:DeleteItem"
                        ],
                        resources=[
                            self._amc_workflow_library_table.table_arn,
                            f"{self._amc_workflow_library_table.table_arn}/*"
                        ]
                    )
                ]
            )
        )

        # IAM Role WorkflowScheduleTrigger
        self._workflow_schedule_trigger_role = Role(
            self,
            "IAM Role Workflow Schedule Trigger 1",
            description=f"Role for the WorkflowScheduleTrigger Lambda for {name_prefix}",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                ManagedPolicy.from_aws_managed_policy_name("AmazonEventBridgeFullAccess"),
                ddb_write_schedules_policy,
                ddb_read_config_policy,
                ddb_write_execution_policy,
                lambda_rw_policy,
                kms_decrypt_snssqs_key_policy
            ]
        )

        # IAM Role CustomerConfigTrigger
        self._customer_config_trigger_role = Role(
            self,
            "IAM Role Customer Config Trigger 1",
            description=f"Role for the CustomerConfigTrigger Lambda for {name_prefix}",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                ManagedPolicy.from_aws_managed_policy_name("AmazonDynamoDBFullAccess"),
                ManagedPolicy.from_aws_managed_policy_name("AmazonEventBridgeFullAccess"),
                ManagedPolicy.from_aws_managed_policy_name("AmazonSQSFullAccess"),
                ddb_read_config_policy,
                modify_amc_api_invoke_policy,
                kms_decrypt_snssqs_key_policy
            ],
            inline_policies={
                "InvokeLambdaWorkflowLibraryTrigger":PolicyDocument(
                    statements=[
                        PolicyStatement(
                            effect=Effect.ALLOW,
                            actions=["lambda:InvokeFunction"],
                            resources=[f"arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:function:{name_prefix}-WorkflowLibraryTrigger-{self._environment_id}"]
                        )
                    ]
                )
            }
        )

        #IAM Role Workflow Library Trigger
        self._workflow_library_trigger_role = Role(
            self,
            "IAM Role Workflow Library Trigger 1",
            description=f"Role for the WorkflowLibraryTrigger Lambda for {name_prefix}",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                ManagedPolicy.from_aws_managed_policy_name("CloudWatchEventsReadOnlyAccess"),
                ddb_read_config_policy,
                ddb_write_schedules_policy,
                kms_decrypt_snssqs_key_policy,
                ddb_write_library_policy
            ]
        )

        #IAM Role AmcApiInterface
        self._amc_api_interface_role = Role(
            self,
            "IAM Role AmcApiInterface Lambda 1",
            description=f"Role for the AmcApiInterface Lambda for {name_prefix}",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                ddb_read_config_policy,
                kms_decrypt_snssqs_key_policy,
                self._invoke_amc_api_policy,
                ddb_write_execution_policy,
                lambda_invoke_sync_workflow_policy,
                sns_publish_policy
            ]
        )

        # IAM Role CustomerConfigQueueConsumer
        self._customer_config_role = Role(
            self,
            "IAM Role CustomerConfigQueueConsumer 1",
            description=f"Role for the CustomerConfigQueueConsumer Lambda for {name_prefix}",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                kms_decrypt_snssqs_key_policy,
                ddb_write_config_policy,
                sns_publish_policy
            ]
        )

        # IAM Role WorkflowLibraryQueueConsumer
        self._workflow_library_consumer_role = Role(
            self,
            "IAM Role WorkflowLibraryQueueConsumer 1",
            description=f"Role for the WorkflowLibraryQueueConsumer Lambda for {name_prefix}",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                kms_decrypt_snssqs_key_policy,
                ddb_write_library_policy,
                sns_publish_policy
            ]
        )

        # IAM Role SyncWorkflowStatuses
        self._sync_workflow_status_role = Role(
            self,
            "IAM Role SyncWorkflowStatuses 1",
            description=f"Role for the SyncWorkflowStatuses Lambda for {name_prefix}",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                self._invoke_amc_api_policy,
                ddb_write_config_policy,
                ddb_write_execution_policy,
                kms_decrypt_snssqs_key_policy,
                sns_publish_policy
            ]
        )

        # IAM Role WorkflowStatusTrigger
        self._workflow_status_trigger_role = Role(
            self,
            "IAM Role LambdaWorkflowStatusTrigger 1",
            description=f"Role for the LambdaWorkflowStatusTrigger Lambda for {name_prefix}",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                ddb_read_config_policy,
                kms_decrypt_snssqs_key_policy,
                sns_publish_policy,
                ddb_read_execution_policy
            ]
        )

        # IAM Role WorkflowTableTrigger
        self._workflow_table_trigger_role = Role(
            self,
            "IAM Role WorkflowTableTrigger 1",
            description=f"Role for the WorkflowTableTrigger Lambda for {name_prefix}",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                ddb_read_config_policy,
                kms_decrypt_snssqs_key_policy,
                sns_publish_policy,
                lambda_invoke_amc_api_interface,
                ddb_read_workflows_policy
            ]
        )

        # IAM Role GenerateExecutionResubmissions 
        self._generate_resubmission_role = Role(
            self,
            "IAM Role GenerateExecutionResubmissions 1",
            description=f"Role for the GenerateExecutionResubmissions Lambda for {name_prefix}",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                ddb_read_config_policy,
                sns_publish_policy,
                ddb_read_execution_policy,
                kms_decrypt_snssqs_key_policy,
                sqs_execution_queue_policy
            ]
        )

        # IAM Role EventQueueConsumer 
        self._event_queue_consumer_role = Role(
            self,
            "IAM Role EventQueueConsumer 1",
            description=f"Role for the EventQueueConsumer Lambda for {name_prefix}",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                ddb_read_config_policy,
                self._invoke_amc_api_policy,
                ddb_write_execution_policy,
                sqs_execution_queue_policy,
                sns_publish_policy,
                kms_decrypt_snssqs_key_policy,
                lambda_invoke_execution_consumer
            ]
        )

        # IAM Role EventQueueProducer 
        self._event_queue_producer_role = Role(
            self,
            "IAM Role EventQueueProducer 1",
            description=f"Role for the EventQueueProducer Lambda for {name_prefix}",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                ddb_read_config_policy,
                sns_publish_policy,
                sqs_execution_queue_policy,
                kms_decrypt_snssqs_key_policy
            ]
        )

        # IAM Role RunWorkFlowByCampaign 
        self._run_workflow_campaign_role = Role(
            self,
            "IAM Role RunWorkFlowByCampaign 1",
            description=f"Role for the RunWorkFlowByCampaign Lambda for {name_prefix}",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                ddb_read_config_policy,
                athena_workgroup_access_policy,
                glue_catalog_access_policy,
                s3_athena_results_policy,
                lambda_invoke_execution_producer,
                lakeformation_get_data_policy,
                sns_publish_policy,
                kms_decrypt_snssqs_key_policy
            ]
        )

        # IAM Role RoleLambdaAmcWorkflowScheduler 
        self._custom_scheduler_role = Role(
            self,
            "IAM Role CustomScheduler 1",
            description=f"Role for the CustomScheduler Lambda for {name_prefix}",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                ddb_write_schedules_policy,
                lambda_invoke_execution_producer,
                sns_publish_policy,
                kms_decrypt_snssqs_key_policy
            ]
        )
