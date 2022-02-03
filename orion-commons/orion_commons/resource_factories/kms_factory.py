#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import logging
from typing import Any, Dict

from aws_cdk.aws_kms import Key
from aws_cdk.core import Construct, Duration, RemovalPolicy

from ..config import Config
from ..utils import get_removal_policy

_logger: logging.Logger = logging.getLogger(__name__)


class KMSFactory:
    @staticmethod
    def key(
        scope: Construct,
        environment_id: str,
        id: str,
        enable_key_rotation: bool = True,
        pending_window: Duration = Duration.days(30),
        removal_policy: RemovalPolicy = RemovalPolicy.RETAIN,
        **kms_props: Any,
    ) -> Key:
        config = Config()

        kms_config: Dict[str, Any] = config.get_resource_config(
            id,
            environment_id=environment_id,
        )

        _logger.debug(f"KMS config {id}: {kms_config}")

        removal_policy_config = kms_config.get("removal_policy")
        if removal_policy_config:
            removal_policy = get_removal_policy(removal_policy_config)

        return Key(
            scope,
            id,
            enable_key_rotation=kms_config.get("enable_key_rotation") or enable_key_rotation,
            pending_window=pending_window,
            removal_policy=removal_policy,
            **kms_props,
        )
