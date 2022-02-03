#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import logging
from typing import Any, Dict, Optional

from aws_cdk.aws_events import EventBus, IEventBus
from aws_cdk.core import Construct, RemovalPolicy

from ..config import Config
from ..utils import get_removal_policy, get_ssm_value

_logger: logging.Logger = logging.getLogger(__name__)


class EventBusFactory:
    @staticmethod
    def eventbus(
        scope: Construct,
        environment_id: str,
        id: str,
        event_bus_name: Optional[str] = None,
        removal_policy: RemovalPolicy = RemovalPolicy.RETAIN,
        **eventbus_props: Any,
    ) -> EventBus:
        config = Config()

        eventbus_config: Dict[str, Any] = config.get_resource_config(id=id, environment_id=environment_id)

        _logger.debug(f"EventBus config {id}: {eventbus_config}")

        event_bus = EventBus(
            scope=scope,
            id=id,
            event_bus_name=event_bus_name,
            **eventbus_props,
        )

        removal_policy_config = eventbus_config.get("removal_policy")
        if removal_policy_config:
            removal_policy = get_removal_policy(removal_policy_config)

        event_bus.apply_removal_policy(removal_policy)

        return event_bus

    @staticmethod
    def from_ssm(scope: Construct, id: str, parameter_name: str) -> IEventBus:
        event_bus_arn: str = get_ssm_value(scope, id=f"{id}-ssm", parameter_name=parameter_name)
        return EventBusFactory.from_arn(scope, id=id, even_bus_arn=event_bus_arn)

    @staticmethod
    def from_arn(scope: Construct, id: str, even_bus_arn: str) -> IEventBus:
        return EventBus.from_event_bus_arn(
            scope,
            id=id,
            event_bus_arn=even_bus_arn,
        )
