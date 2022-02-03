#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from aws_cdk.aws_events import EventBus
from aws_cdk.core import Stack
from orion_commons import EventBusFactory


def test_get_eventbus(test_stack: Stack) -> None:
    _eventbus = EventBusFactory.eventbus(
        scope=test_stack,
        environment_id="dev",
        id="test-eventbus",
        event_bus_name="test-eventbus",
    )

    assert isinstance(_eventbus, EventBus)
