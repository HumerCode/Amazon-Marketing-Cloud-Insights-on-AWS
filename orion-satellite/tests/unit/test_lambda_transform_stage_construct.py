#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import os
from pathlib import Path

from aws_cdk.aws_events import EventPattern
from aws_cdk.core import Stack
from orion_satellite.producer.stages import LambdaTransformStage, LambdaTransformStageConfig


def test_lambda_transform_stage_construct(test_stack: Stack) -> None:
    stage = LambdaTransformStage(
        test_stack,
        pipeline_id="test",
        id="lambda-transform",
        environment_id="dev",
        config=LambdaTransformStageConfig(
            output_bucket_name="test",
            code=os.path.join(
                f"{Path(__file__).parents[2]}", "orion_satellite/producer/lambdas/legislators_lambda_transform"
            ),
            handler="handler.lambda_handler",
        ),
    )

    assert isinstance(stage, LambdaTransformStage)
    assert isinstance(stage.get_event_pattern(), EventPattern)
    assert len(stage.get_targets()) == 1  # type: ignore
