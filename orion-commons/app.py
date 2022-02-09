#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import logging
from typing import Any

from aws_cdk.core import App, CfnOutput, Construct, Stack, Stage
from orion_commons import Config

_logger: logging.Logger = logging.getLogger(__name__)


class DevStage(Stage):
    def __init__(
        self,
        scope: Construct,
        stage_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, stage_id, **kwargs)
        DevStack(self, "my-stack")


class DevStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self._output = CfnOutput(self, "out", value="test")


app = App()
config = Config()
print(config.get_resource_config(id="test-lambda", environment_id="dev")["memory_size"])
"""
pipeline = (
    CICDPipeline(
        app,
        pipeline_id="test-pipe",
        pipeline_name="TestPipeline",
    )
    .add_artifacts()
    .add_source_action(repository_name="orion-commons")
    .add_synth_action()
    .build()
    .add_stage("dev", DevStage(app, "dev"))
)
"""
app.synth()
