#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import logging
from typing import Any

from aws_cdk.core import App, Construct, Stage
from orion_artifacts.artifacts.base_stack import BaseStack
from orion_artifacts.artifacts.glue_stack import GlueStack
from orion_artifacts.artifacts.layers_stack import LayersStack
from orion_commons import CICDPipeline, Config

_logger: logging.Logger = logging.getLogger(__name__)


class ApplicationStage(Stage):
    def __init__(self, scope: Construct, environment_id: str, **kwargs: Any) -> None:
        super().__init__(scope, f"orion-{environment_id}-artifacts", **kwargs)
        base_stack = BaseStack(self, "base", environment_id=environment_id)
        GlueStack(self, "glue", environment_id=environment_id).add_dependency(base_stack)
        LayersStack(self, "layers")


artifacts_app = App()
config = Config()
pipeline_name = "orion-cicd-artifacts-pipeline"
_logger.debug("Define CICD Pipeline")
(
    CICDPipeline(artifacts_app, pipeline_id=pipeline_name, pipeline_name=pipeline_name)
    .add_artifacts()
    .add_source_action(repository_name="orion-artifacts")
    .add_synth_action()
    .build()
    .add_stage("dev", ApplicationStage(artifacts_app, environment_id="dev", env=config.get_env("dev")))
)
artifacts_app.synth()
