from typing import Any, List, Optional, Sequence

from aws_cdk.aws_codecommit import Repository
from aws_cdk.aws_codepipeline import Artifact
from aws_cdk.aws_codepipeline_actions import Action, CodeCommitSourceAction
from aws_cdk.aws_iam import Effect, PolicyStatement
from aws_cdk.aws_ssm import StringParameter
from aws_cdk.core import Construct
from aws_cdk.pipelines import ShellScriptAction, SimpleSynthAction


def get_code_commit_source_action(
    scope: Construct,
    repository_name: str,
    output_artifact: Artifact,
    branch: str = "main",
    **source_props: Any,
) -> Action:
    return CodeCommitSourceAction(
        action_name="CodeCommit",
        repository=Repository.from_repository_name(scope, repository_name, repository_name),
        output=output_artifact,
        branch=branch,
        **source_props,
    )


def get_synth_action(
    scope: Construct,
    source_artifact: Artifact,
    cloud_assembly_artifact: Artifact,
    cdk_version: Optional[str],
    role_policy_statements: List[PolicyStatement] = [],
    codeartifact_repository: Optional[str] = None,
    codeartifact_domain: Optional[str] = None,
    codeartifact_domain_owner: Optional[str] = None,
) -> SimpleSynthAction:
    install_commands: List[str] = [
        f"npm install -g aws-cdk@{cdk_version if cdk_version else ''}",
    ]
    if all([codeartifact_repository, codeartifact_domain, codeartifact_domain_owner]):
        role_policy_statements.extend(
            [
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "codeartifact:DescribeDomain",
                        "codeartifact:GetAuthorizationToken",
                        "codeartifact:ListRepositoriesInDomain",
                    ],
                    resources=[
                        StringParameter.value_from_lookup(scope, "/Orion/CodeArtifact/DomainArn"),
                    ],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "codeartifact:GetRepositoryEndpoint",
                        "codeartifact:ReadFromRepository",
                    ],
                    resources=[
                        StringParameter.value_from_lookup(scope, "/Orion/CodeArtifact/RepositoryArn"),
                    ],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=["sts:GetServiceBearerToken"],
                    resources=["*"],
                    conditions={"StringEquals": {"sts:AWSServiceName": "codeartifact.amazonaws.com"}},
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "kms:Decrypt",
                        "kms:DescribeKey",
                        "kms:Encrypt",
                        "kms:GenerateDataKey*",
                        "kms:ReEncrypt*",
                    ],
                    resources=[
                        StringParameter.value_from_lookup(scope, "/Orion/KMS/CICDAssetsEncryptionKeyArn"),
                    ],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "ssm:GetParameter",
                        "ssm:GetParameters",
                    ],
                    resources=[
                        scope.format_arn(resource="parameter", service="ssm", resource_name="Orion/*")  # type: ignore
                    ],
                ),
            ]
        )
        install_commands.append(
            f"aws codeartifact login "
            f"--tool pip --repository {codeartifact_repository} "
            f"--domain {codeartifact_domain} "
            f"--domain-owner {codeartifact_domain_owner}",
        )
    install_commands.append("pip install -r requirements.txt")
    return SimpleSynthAction(
        source_artifact=source_artifact,
        cloud_assembly_artifact=cloud_assembly_artifact,
        install_commands=install_commands,
        role_policy_statements=role_policy_statements or None,
        synth_command="cdk synth",
    )


def get_cfn_nag_action(artifacts: Optional[Sequence[Artifact]]) -> ShellScriptAction:
    return ShellScriptAction(
        action_name="CFNNag",
        commands=[
            "gem install cfn-nag",
            "fnames=$(find ./ -type f -name '*.template.json')",
            "for f in $fnames; do cfn_nag_scan --input-path $f; done",
        ],
        additional_artifacts=artifacts,
    )


def get_bandit_action(artifacts: Optional[Sequence[Artifact]]) -> ShellScriptAction:
    return ShellScriptAction(
        action_name="Bandit",
        commands=[
            "pip install bandit",
            "bandit -r ./orion*",
        ],
        additional_artifacts=artifacts,
    )


def get_tests_action(artifacts: Optional[Sequence[Artifact]]) -> ShellScriptAction:
    return ShellScriptAction(
        action_name="Test",
        commands=[
            "./test.sh",
        ],
        additional_artifacts=artifacts,
    )
