# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from os import pipe
from typing import Any, Optional
import json
from aws_cdk.aws_sqs import Queue, DeadLetterQueue, QueueEncryption
from aws_cdk.aws_glue import CfnCrawler, Database
from aws_cdk.aws_iam import ManagedPolicy, Role, ServicePrincipal, PolicyDocument, PolicyStatement
from aws_cdk.aws_kms import IKey, Key
from aws_cdk.aws_lakeformation import CfnPermissions
from aws_cdk.aws_s3 import Bucket, IBucket
from aws_cdk.core import Construct, Stack, Duration, RemovalPolicy
from aws_cdk.aws_events import EventPattern, Rule, Schedule, CfnRule
from aws_cdk import (core)
from aws_cdk.aws_lambda import CfnPermission
from aws_cdk.aws_ssm import StringParameter

from ..utils import (
    RegisterConstruct
)

def get_ssm_value(scope: Construct, id: str, parameter_name: str) -> str:
    return StringParameter.from_string_parameter_name(
        scope,
        id=id,
        string_parameter_name=parameter_name,
    ).string_value

class SDLFDatasetStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, environment_id: str, params: dict, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self._environment_id: str = environment_id
        self._params: dict = params
        self._org = self._params.get("org", "aws")
        self._app = self._params.get("app", "datalake")
        self._team = self._params.get("team", "demoteam")
        self._pipeline = self._params.get("pipeline", "adv")
        self._dataset = self._params.get("dataset", "amcdataset")

        # Get analytics bucket props
        self._stage_bucket_key: IKey = Key.from_key_arn(
            self,
            "stage-bucket-key",
            key_arn=get_ssm_value(
                self,
                "stage-bucket-key-arn-ssm",
                parameter_name="/Orion/KMS/StageBucketKeyArn",
            ),
        )
        self._stage_bucket: IBucket = Bucket.from_bucket_arn(
            self,
            "stage-bucket",
            bucket_arn=get_ssm_value(
                self,
                "stage-bucket-arn-ssm",
                parameter_name="/Orion/S3/StageBucketArn",
            ),
        )

        self._glue_job_role_arn = get_ssm_value(
            self,
            f"glue-job-{self._dataset}-role-arn",
            parameter_name=f"/Orion/IAM/{self._team}/{self._dataset}/HeavyTranformGlueRoleARN"
        )

        self._create_dataset(team=self._team, 
                            pipeline=self._pipeline, 
                            name=self._dataset,
                            stage_a_transform="amc_light_transform",
                            stage_b_transform="amc_heavy_transform")
        
  
    def _create_dataset(self, team: str, pipeline: str, name: str, stage_a_transform: Optional[str] = None, stage_b_transform: Optional[str] = None) -> None:
        
        self.stage_a_transform: str = stage_a_transform if stage_a_transform else "light_transform_blueprint"
        self.stage_b_transform: str = stage_b_transform if stage_b_transform else "heavy_transform_blueprint"

        self._props={
                "id":f"{team}-{name}",
                "description":f"{name.title()} dataset",
                "name": f"{team}-{name}",
                "type": "octagon_dataset",
                "pipeline": pipeline,
                "max_items_process": {
                    "stage_b": 100,
                    "stage_c": 100
                },
                "min_items_process" : {
                    "stage_b": 1,
                    "stage_c": 1
                },
                "version": 1,
                "transforms":{
                "stage_a_transform": self.stage_a_transform,
                "stage_b_transform": self.stage_b_transform,
            }
            }


        RegisterConstruct(self, self._props["id"], props=self._props)

        #Glue DB, crawler etc
        database: Database = Database(
            self,
            f"orion-{name}-database",
            database_name=f"aws_datalake_{self._environment_id}_{team}_{name}_db", 
            location_uri=f"s3://{self._stage_bucket.bucket_name}/post-stage/{team}/{name}",
        )

        CfnPermissions(
            self,
            f"orion-{name}-glue-job-database-lakeformation-permissions",
            data_lake_principal=CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=self._glue_job_role_arn
            ),
            resource=CfnPermissions.ResourceProperty(
                database_resource=CfnPermissions.DatabaseResourceProperty(name=database.database_name)
            ),
            permissions=["CREATE_TABLE", "ALTER", "DROP"],
        )

        StringParameter(
            self,
            f"orion-{team}-{name}-stage-catalog",
            parameter_name=f"/Orion/Glue/{team}/{name}/StageDataCatalog",
            string_value=f"aws_datalake_{self._environment_id}_{team}_{name}_db",
        )

        #SQS and DLQ

        #sqs kms key resource
        sqs_key: Key = Key(
            self,
            id=f"orion-{team}-{name}-sqs-key-b",
            description="Orion SQS Key Stage B",
            alias=f"orion-{team}-{name}-sqs-stage-b-key",
            enable_key_rotation=True,
            pending_window=Duration.days(30),
            removal_policy=RemovalPolicy.DESTROY,
        )

        sqs_key_policy = PolicyDocument(
            statements=[PolicyStatement(
                actions=["kms:*"],
                principals=[ServicePrincipal("lambda.amazonaws.com")],
                resources=["*"]
            )]
        )

        #SSM for sqs kms table arn
        StringParameter(
            self,
            f"orion-{team}-{name}-sqs-stage-b-key-arn-ssm",
            parameter_name=f"/Orion/KMS/SQS/{team}/{name}StageBKeyArn",
            string_value=sqs_key.key_arn,
        )

        #SSM for sqs kms table id
        StringParameter(
            self,
            f"orion-{team}-{name}-sqs-stage-b-key-id-ssm",
            parameter_name=f"/Orion/KMS/SQS/{team}/{name}StageBKeyId",
            string_value=sqs_key.key_id,
        )
        routing_dlq = DeadLetterQueue(
            max_receive_count=1, 
            queue=Queue(self, 
                            id=f'orion-{team}-{name}-dlq-b.fifo',
                            queue_name=f'orion-{team}-{name}-dlq-b.fifo', 
                            fifo=True,
                            visibility_timeout=core.Duration.seconds(60),
                            encryption=QueueEncryption.KMS,
                            encryption_master_key=sqs_key))

        StringParameter(
            self,
            f'orion-{team}-{name}-dlq-b.fifo-ssm',
            parameter_name=f"/Orion/SQS/{team}/{name}StageBDLQ",
            string_value=f'orion-{team}-{name}-dlq-b.fifo',
        )


        routing_queue = Queue(
            self, 
            id=f'orion-{team}-{name}-queue-b.fifo', 
            queue_name=f'orion-{team}-{name}-queue-b.fifo', 
            fifo=True,
            visibility_timeout=core.Duration.seconds(60),
            encryption=QueueEncryption.KMS,
            encryption_master_key=sqs_key, 
            dead_letter_queue=routing_dlq)

        StringParameter(
            self,
            f'orion-{team}-{name}-queue-b.fifo-ssm',
            parameter_name=f"/Orion/SQS/{team}/{name}StageBQueue",
            string_value=f'orion-{team}-{name}-queue-b.fifo',
        )


        #Eventbridge and event source mapping
        post_state_rule = CfnRule(
                    self, 
                    f"orion-{team}-{name}-rule-b",
                    name=f"orion-{team}-{name}-rule-b",
                    schedule_expression="cron(*/5 * * * ? *)",
                    state="ENABLED",
                    targets=[CfnRule.TargetProperty(
                        arn=f"arn:aws:lambda:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:function:orion-{team}-{pipeline}-routing-b",
                        id=f"orion-{team}-{name}-rule-b",
                        input=json.dumps({
                                "team": team,
                                "pipeline": pipeline,
                                "pipeline_stage": "StageB",
                                "dataset": name,
                                "org": self._org,
                                "app": self._app,
                                "env": self._environment_id
                            }, indent = 4)
                        )])

        Lambda_permissions = CfnPermission(self, f"orion-{team}-{name}-routing-b",
                        action="lambda:InvokeFunction",
                        function_name=f"orion-{team}-{pipeline}-routing-b",
                        principal="events.amazonaws.com",
                        source_arn=post_state_rule.attr_arn
                    )
 
        