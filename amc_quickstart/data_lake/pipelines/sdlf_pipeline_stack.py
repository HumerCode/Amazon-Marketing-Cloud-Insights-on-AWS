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
from typing import Any

import aws_cdk as cdk
from aws_cdk.aws_events import EventPattern, Rule, Schedule
from aws_ddk_core.pipelines import DataPipeline  
from aws_cdk.aws_ssm import StringParameter
from aws_ddk_core.base import BaseStack
from aws_cdk import ArnFormat

from ..stages import (
    SDLFLightTransform,
    SDLFLightTransformConfig,
    SDLFHeavyTransform,
    SDLFHeavyTransformConfig
)



def get_ssm_value(scope, id: str, parameter_name: str) -> str:
    return StringParameter.from_string_parameter_name(
        scope,
        id=id,
        string_parameter_name=parameter_name,
    ).string_value

class SDLFPipelineStack(BaseStack):
    def __init__(self, scope, construct_id: str, environment_id: str, resource_prefix: str, params: dict, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, environment_id, **kwargs)
        self._environment_id: str = environment_id
        self._params: dict = params
        self._resource_prefix = resource_prefix
        self._team = self._params.get("team", "demoteam")
        self._pipeline = self._params.get("pipeline", "adv")

        # Get raw/stage bucket props
        raw_bucket_arn: str = get_ssm_value(self, "raw-bucket-ssm", "/AMC/S3/RawBucketArn")
        stage_bucket_arn: str = get_ssm_value(self, "stage-bucket-ssm", "/AMC/S3/StageBucketArn")
        analytics_bucket_arn: str = get_ssm_value(self, "analytics-bucket-ssm", "/AMC/S3/AnalyticsBucketArn")

        self._raw_bucket_name: str = self.split_arn(
            arn=raw_bucket_arn, arn_format=ArnFormat.COLON_RESOURCE_NAME
        ).resource
        self._stage_bucket_name: str = self.split_arn(
            arn=stage_bucket_arn, arn_format=ArnFormat.COLON_RESOURCE_NAME
        ).resource
        self._analytics_bucket_name: str = self.split_arn(
            arn=analytics_bucket_arn, arn_format=ArnFormat.COLON_RESOURCE_NAME
        ).resource


        self._create_sdlf_pipeline(team=self._team,pipeline=self._pipeline)  # Simple single-dataset pipeline with static config

    def _create_sdlf_pipeline(self, team, pipeline) -> None:
        """
        Simple single-dataset pipeline example
        :return:
        """
        pipeline_id: str = f"{self._resource_prefix}-{team}-{pipeline}"

        data_lake_light_transform = SDLFLightTransform(
            self,
            id=f"{pipeline_id}-stage-a",
            name=f"{team}-{pipeline}-stage-a",
            prefix = self._resource_prefix,
            description=f"{self._resource_prefix} data lake light transform",
            props={
                "version": 1,
                "status": "ACTIVE",
                "name": f"{team}-{pipeline}-stage-a",
                "type": "octagon_pipeline",
                "description": f"{self._resource_prefix} data lake light transform",
                "id": f"{team}-{pipeline}-stage-a"
            },
            environment_id=self._environment_id,
            config=SDLFLightTransformConfig(
                team=team,
                pipeline=pipeline 
            ),
        )
        


        data_lake_heavy_transform = SDLFHeavyTransform(
            self,
            id=f"{pipeline_id}-stage-b",
            name=f"{team}-{pipeline}-stage-b",
            prefix = self._resource_prefix,
            description=f"{self._resource_prefix} data lake heavy transform",
            props={
                "version": 1,
                "status": "ACTIVE",
                "name": f"{team}-{pipeline}-stage-b",
                "type": "octagon_pipeline",
                "description": f"{self._resource_prefix} data lake heavy transform",
                "id": f"{team}-{pipeline}-stage-b"
            },
            environment_id=self._environment_id,
            config=SDLFHeavyTransformConfig(
                team=team,
                pipeline=pipeline 
            ),
        )
    

        self._data_lake_pipeline: DataPipeline = (
            DataPipeline(
                self, 
                id=pipeline_id,
                name=f"{pipeline_id}-pipeline",
                description=f"{self._resource_prefix} data lake pipeline", 
            )
            .add_stage(data_lake_light_transform, skip_rule=True)
            .add_stage(data_lake_heavy_transform, skip_rule=True)
        )
