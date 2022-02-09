# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import logging
from typing import Any, Dict, Optional

from aws_cdk.aws_codepipeline import Artifact
from aws_cdk.aws_codepipeline_actions import Action
from aws_cdk.core import Construct, DefaultStackSynthesizer, Environment, Stack, Stage
from aws_cdk.pipelines import CdkPipeline

from .. import constants
from ..config import Config
from .actions import (
    get_bandit_action,
    get_cfn_nag_action,
    get_code_commit_source_action,
    get_synth_action,
    get_tests_action,
)

_logger: logging.Logger = logging.getLogger(__name__)


class CICDPipeline(Stack):
    def __init__(
        self,
        scope: Construct,
        pipeline_id: str,
        pipeline_name: Optional[str] = None,
        env: Optional[Environment] = None,
        synthesizer: Optional[DefaultStackSynthesizer] = DefaultStackSynthesizer(
            qualifier=constants.QUALIFIER,
            deploy_role_arn=constants.DEPLOY_ROLE_ARN,
            cloud_formation_execution_role=constants.CFN_EXEC_ROLE_ARN,
        ),
        **kwargs: Any,
    ) -> None:
        _logger.debug("Instantiate CICD Pipeline")
        self._config = Config()
        super().__init__(scope, pipeline_id, env=env or self._config.get_env("cicd"), synthesizer=synthesizer, **kwargs)

        self._pipeline_id = pipeline_id
        self._pipeline_name = pipeline_name

    def add_artifacts(
        self,
        source_artifact: Optional[Artifact] = None,
        cloud_assembly_artifact: Optional[Artifact] = None,
    ) -> "CICDPipeline":
        self._source_artifact = source_artifact or Artifact()
        self._cloud_assembly_artifact = cloud_assembly_artifact or Artifact()

        return self

    def add_source_action(
        self,
        repository_name: Optional[str] = None,
        source_action: Optional[Action] = None,
    ) -> "CICDPipeline":
        self._source_action = source_action or get_code_commit_source_action(
            self, repository_name=repository_name, output_artifact=self._source_artifact  # type: ignore
        )
        return self

    def add_synth_action(
        self,
        synth_action: Optional[Action] = None,
        codeartifact_repository: Optional[str] = None,
        codeartifact_domain: Optional[str] = None,
        codeartifact_domain_owner: Optional[str] = None,
    ) -> "CICDPipeline":
        artifactory_config = self._config.get_env_config(environment_id="cicd").get("codeartifact", {})
        if not codeartifact_repository:
            codeartifact_repository = artifactory_config.get("repository")
        if not codeartifact_domain:
            codeartifact_domain = artifactory_config.get("domain")
        if not codeartifact_domain_owner:
            codeartifact_domain_owner = artifactory_config.get("domain_owner")

        self._synth_action = synth_action or get_synth_action(
            self,
            source_artifact=self._source_artifact,
            cloud_assembly_artifact=self._cloud_assembly_artifact,
            cdk_version=self._config.get_cdk_version(),
            codeartifact_repository=codeartifact_repository,
            codeartifact_domain=codeartifact_domain,
            codeartifact_domain_owner=codeartifact_domain_owner,
        )
        return self

    def add_security_lint_stage(
        self,
        stage_name: Optional[str] = None,
        cloud_assembly_artifact: Optional[Artifact] = None,
    ) -> "CICDPipeline":
        self._pipeline.add_stage(stage_name or "SecurityLint").add_actions(
            get_cfn_nag_action(
                artifacts=[cloud_assembly_artifact if cloud_assembly_artifact else self._cloud_assembly_artifact]
            ),
            get_bandit_action(
                artifacts=[cloud_assembly_artifact if cloud_assembly_artifact else self._cloud_assembly_artifact]
            ),
        )
        return self

    def build(self) -> "CICDPipeline":
        _logger.debug("Build CICD Pipeline")
        self._pipeline = CdkPipeline(
            self,
            "cdk-pipeline",
            cloud_assembly_artifact=self._cloud_assembly_artifact,
            pipeline_name=self._pipeline_name,
            source_action=self._source_action,
            synth_action=self._synth_action,
            cdk_cli_version=self._config.get_cdk_version(),
        )
        # CDK Pipelines does not enable KMS key rotation by default
        self._pipeline_key = (
            self._pipeline.code_pipeline.artifact_bucket.encryption_key.node.default_child  # type: ignore
        )
        self._pipeline_key.enable_key_rotation = True  # type: ignore
        if self._config.get_env_config("cicd").get("execute_security_lint"):
            self.add_security_lint_stage()
        return self

    def add_stage(
        self,
        stage_id: str,
        stage: Stage,
        manual_approvals: Optional[bool] = False,
        execute_tests: Optional[bool] = True,
    ) -> "CICDPipeline":
        _logger.debug(f"Add {stage_id} stage")
        stage_config: Dict[str, Any] = self._config.get_env_config(stage_id)
        manual_approvals = manual_approvals or stage_config.get("manual_approvals")

        if execute_tests or stage_config.get("execute_tests"):
            self._pipeline.add_stage("Test").add_actions(get_tests_action(artifacts=[self._source_artifact]))

        self._pipeline.add_application_stage(stage, manual_approvals=manual_approvals)
        return self
