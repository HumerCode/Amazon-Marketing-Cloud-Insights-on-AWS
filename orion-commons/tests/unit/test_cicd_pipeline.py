#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from typing import Any

from aws_cdk.aws_codepipeline import Artifact, Pipeline
from aws_cdk.aws_codepipeline_actions import GitHubSourceAction
from aws_cdk.core import App, CfnOutput, Construct, DefaultStackSynthesizer, SecretValue, Stack, Stage
from aws_cdk.pipelines import CdkPipeline
from orion_commons import CICDPipeline


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


def test_cicd_pipeline_simple(cdk_app: App) -> None:
    pipeline = (
        CICDPipeline(
            cdk_app,
            pipeline_id="test-pipe-1",
        )
        .add_artifacts()
        .add_source_action(repository_name="orion-commons")
        .add_synth_action()
        .build()
        .add_stage("dev", DevStage(cdk_app, "dev"))
    )
    assert isinstance(pipeline, CICDPipeline)
    assert isinstance(pipeline._pipeline, CdkPipeline)
    assert isinstance(pipeline._pipeline.code_pipeline, Pipeline)

    assert pipeline._pipeline.code_pipeline.stage_count == 6


def test_cicd_pipeline_github_source(cdk_app: App) -> None:
    source_artifact = Artifact()

    github_source_action = GitHubSourceAction(
        oauth_token=SecretValue(value="dummy"),
        output=source_artifact,
        owner="test",
        repo="test",
        action_name="checkout",
    )

    pipeline = (
        CICDPipeline(
            cdk_app,
            pipeline_id="test-pipe-2",
            pipeline_name="TestPipeline2",
            synthesizer=DefaultStackSynthesizer(
                qualifier="my_qualifier",
                deploy_role_arn="arn:${AWS::Partition}:iam::${AWS::AccountId}:role/cdk-${Qualifier}-admin-deploy-role-${AWS::AccountId}-${AWS::Region}",  # noqa
                cloud_formation_execution_role="arn:${AWS::Partition}:iam::${AWS::AccountId}:role/cdk-${Qualifier}-admin-cfn-exec-role-${AWS::AccountId}-${AWS::Region}",  # noqa
            ),
        )
        .add_artifacts(source_artifact=source_artifact)
        .add_source_action(source_action=github_source_action)
        .add_synth_action()
        .build()
    )
    assert isinstance(pipeline, CICDPipeline)
    assert isinstance(pipeline._pipeline, CdkPipeline)
    assert isinstance(pipeline._pipeline.code_pipeline, Pipeline)

    assert pipeline._pipeline.code_pipeline.stage_count == 4
