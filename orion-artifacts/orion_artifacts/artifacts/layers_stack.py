#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import os
from pathlib import Path
from typing import Any

from aws_cdk.aws_lambda import Code, LayerVersion, Runtime
from aws_cdk.aws_sam import CfnApplication
from aws_cdk.core import Construct, Stack
from aws_cdk.aws_ssm import StringParameter


class LayersStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self._create_wrangler_layers()
        self._create_orion_library_layer()

    def _create_wrangler_layers(self) -> None:
        CfnApplication(
            self,
            "wrangler-layers",
            location=CfnApplication.ApplicationLocationProperty(
                application_id="arn:aws:serverlessrepo:us-east-1:336392948345:applications/aws-data-wrangler-layer-py3-8",
                semantic_version="2.12.0",
            ),
        )

    def _create_orion_library_layer(self) -> None:
        orion_library_layer = LayerVersion(
            self,
            "library-layer",
            layer_version_name="orion-library",
            code=Code.from_asset(os.path.join(f"{Path(__file__).parents[1]}", "layers/orion_library")),
            compatible_runtimes=[Runtime.PYTHON_3_8],
            description="Orion Library",
            license="Apache-2.0",
        )

        StringParameter(
            self,
            f"orion_library_layer",
            parameter_name=f"/Orion/Layer/OrionLibrary",
            string_value=orion_library_layer.layer_version_arn,
        )
