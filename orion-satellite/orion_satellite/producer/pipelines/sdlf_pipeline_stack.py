#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import os
from pathlib import Path
from typing import Any

from aws_cdk.aws_events import EventPattern, Rule, Schedule
from aws_cdk.core import Construct, Stack
from orion_commons import PipelineConstruct 
from aws_cdk.aws_ssm import StringParameter

from ..stages import (
    SDLFLightTransform,
    SDLFLightTransformConfig,
    SDLFHeavyTransform,
    SDLFHeavyTransformConfig
)



def get_ssm_value(scope: Construct, id: str, parameter_name: str) -> str:
    return StringParameter.from_string_parameter_name(
        scope,
        id=id,
        string_parameter_name=parameter_name,
    ).string_value

class SDLFPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, environment_id: str, params: dict, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self._environment_id: str = environment_id
        self._params: dict = params

        self._team = self._params.get("team", "demoteam")
        self._pipeline = self._params.get("pipeline", "adv")

        # Get raw/stage bucket props
        raw_bucket_arn: str = get_ssm_value(self, "raw-bucket-ssm", "/Orion/S3/RawBucketArn")
        stage_bucket_arn: str = get_ssm_value(self, "stage-bucket-ssm", "/Orion/S3/StageBucketArn")
        analytics_bucket_arn: str = get_ssm_value(self, "analytics-bucket-ssm", "/Orion/S3/AnalyticsBucketArn")
        self._raw_bucket_name: str = self.parse_arn(raw_bucket_arn).resource
        self._stage_bucket_name: str = self.parse_arn(stage_bucket_arn).resource
        self._analytics_bucket_name: str = self.parse_arn(analytics_bucket_arn).resource

        self._create_sdlf_pipeline(team=self._team,pipeline=self._pipeline)  # Simple single-dataset pipeline with static config

    def _create_sdlf_pipeline(self, team, pipeline) -> None:
        """
        Simple single-dataset pipeline example
        :return:
        """
        pipeline_id: str = f"orion-{team}-{pipeline}"

        orion_sdlf_light_transform = SDLFLightTransform(
            self,
            pipeline_id=pipeline_id,
            id=f"{pipeline_id}-stage-a",
            name=f"{team}-{pipeline}-stage-a",
            description="orion sdlf light transform",
            props={
                "version": 1,
                "status": "ACTIVE",
                "name": f"{team}-{pipeline}-stage-a",
                "type": "octagon_pipeline",
                "description": "orion sdlf light transform",
                "id": f"{pipeline_id}-stage-a"
            },
            environment_id=self._environment_id,
            config=SDLFLightTransformConfig(
                team=team,
                pipeline=pipeline 
            ),
        )
        


        orion_sdlf_heavy_transform = SDLFHeavyTransform(
            self,
            pipeline_id=pipeline_id,
            id=f"{pipeline_id}-stage-b",
            name=f"{team}-{pipeline}-stage-b",
            description="orion sdlf heavy transform",
            props={
                "version": 1,
                "status": "ACTIVE",
                "name": f"{team}-{pipeline}-stage-b",
                "type": "octagon_pipeline",
                "description": "orion sdlf heavy transform",
                "id": f"{pipeline_id}-stage-b"
            },
            environment_id=self._environment_id,
            config=SDLFHeavyTransformConfig(
                team=team,
                pipeline=pipeline 
            ),
        )
    

        self._orion_sdlf_pipeline: PipelineConstruct = (
            PipelineConstruct(
                self, 
                id=pipeline_id,
                name=f"{pipeline_id}-pipeline",
                description="orion sdlf pipeline", 
            )
            .add_stage(orion_sdlf_light_transform, skip_rule=True)
            .add_stage(orion_sdlf_heavy_transform, skip_rule=True)
        )
