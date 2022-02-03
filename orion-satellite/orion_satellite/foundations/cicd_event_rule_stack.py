#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import os
from pathlib import Path
from typing import Any, Dict

import aws_cdk.aws_dynamodb as DDB
from aws_cdk.core import Construct, Stack
from aws_cdk.aws_events import CfnRule
from aws_cdk import (core)
from aws_cdk.aws_iam import Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal


class CICDEventRuleStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment_id: str,
        pipeline_name: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self._environment_id: str = environment_id
        self._pipeline_name: str = pipeline_name
        
        self._create_event_rule()
        
    
    def _create_event_rule(self) -> None:

        orion_artifacts_satellite_event_role: Role = Role(
            self,
            f"{self._pipeline_name}-role",
            assumed_by=ServicePrincipal("events.amazonaws.com"),
        )

        ManagedPolicy(
            self,
            f"{self._pipeline_name}-policy",
            roles=[orion_artifacts_satellite_event_role],
            document=PolicyDocument(
                statements=[
                    PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "codepipeline:*"
                    ],
                    resources=["*"],
                )
                ]
            ),
        )

        CfnRule(
                self, 
                f"{self._pipeline_name}-rule",
                name=f"{self._pipeline_name}-rule",
                event_pattern={
                    "source": ["aws.codepipeline"],
                    "detail-type": ["CodePipeline Pipeline Execution State Change"],
                    "detail":
                        { 
                            "state": ["SUCCEEDED"],
                            "pipeline": ["orion-cicd-artifacts-pipeline"]
                        }
                },
                state="ENABLED",
                targets=[CfnRule.TargetProperty(
                    arn=f"arn:aws:codepipeline:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:{self._pipeline_name}",
                    id=f"{self._pipeline_name}-rule",
                    role_arn=orion_artifacts_satellite_event_role.role_arn
                    )])
    

