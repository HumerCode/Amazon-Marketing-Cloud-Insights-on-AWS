#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import logging
from typing import Any, Dict, Optional

from aws_cdk.aws_iam import IRole
from aws_cdk.aws_lambda import Code, Function, Runtime
from aws_cdk.aws_sqs import IQueue
from aws_cdk.core import Construct

from ..config import Config

_logger: logging.Logger = logging.getLogger(__name__)


class LambdaFactory:
    @staticmethod
    def function(
        scope: Construct,
        environment_id: str,
        id: str,
        code: Code,
        handler: str,
        runtime: Runtime = Runtime.PYTHON_3_8,
        role: Optional[IRole] = None,
        dead_letter_queue_enabled: bool = False,
        dead_letter_queue: Optional[IQueue] = None,
        memory_size: int = 256,
        **function_props: Any,
    ) -> Function:
        config = Config()

        lambda_config: Dict[str, Any] = config.get_resource_config(
            id,
            environment_id=environment_id,
        )

        _logger.debug(f"Lambda config {id}: {lambda_config}")

        return Function(
            scope,
            id,
            code=code,
            handler=handler,
            runtime=runtime,
            role=role,
            dead_letter_queue_enabled=dead_letter_queue_enabled,
            dead_letter_queue=dead_letter_queue,
            memory_size=lambda_config.get("memory_size") or memory_size,
            **function_props,
        )
