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
from orion_commons import PipelineConstruct, get_ssm_value


from ..stages import (
    # S3EventCaptureStage,
    # S3EventCaptureStageConfig,
    SDLFLightTransform,
    SDLFLightTransformConfig,
    SDLFHeavyTransform,
    SDLFHeavyTransformConfig
)


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
        # self._type = "octagon_pipeline"

        self._create_sdlf_pipeline(team=self._team,pipeline=self._pipeline)  # Simple single-dataset pipeline with static config

    def _create_sdlf_pipeline(self, team, pipeline) -> None:
        """
        Simple single-dataset pipeline example
        :return:
        """
        pipeline_id: str = f"orion-{team}-{pipeline}"
        # orion_s3_event_capture_stage = S3EventCaptureStage(
        #     self,
        #     pipeline_id=pipeline_id,
        #     id=f"{pipeline_id}-s3-event-capture",
        #     description="orion sdlf S3 event capture",
        #     config=S3EventCaptureStageConfig(
        #         bucket_name=self._raw_bucket_name,
        #         key_prefix=f"{team}/",
        #         event_names=[
        #             "CompleteMultipartUpload",
        #             "CopyObject",
        #             "PutObject",
        #         ],
        #     ),
        # )

        orion_sdlf_light_transform = SDLFLightTransform(
            self,
            pipeline_id=pipeline_id,
            id=f"{pipeline_id}-stage-a",
            name=f"{team}-{pipeline}-stage-a",
            description="orion sdlf light transform",
            stage_type="octagon_pipeline",
            props={
                "version": 1,
                "status": "ACTIVE"
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
            stage_type="octagon_pipeline",
            props={
                "version": 1,
                "status": "ACTIVE"
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
                pipeline_type="octagon_pipeline",
                props={
                    "version": 1,
                    "status": "ACTIVE"
                }
            )
            # .add_stage(orion_s3_event_capture_stage)
            .add_stage(orion_sdlf_light_transform, skip_rule=True)
            .add_stage(orion_sdlf_heavy_transform, skip_rule=True)
        )
