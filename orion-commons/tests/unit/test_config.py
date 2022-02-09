#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from orion_commons import Config


def test_get_env_config(config_context: Config) -> None:
    assert len(config_context.get_env_config("dev")) > 0


def test_get_resource_config_context(config_context: Config) -> None:
    assert config_context.get_resource_config("test-lambda", environment_id="dev")["memory_size"] == 512
    assert config_context.get_resource_config("test-bucket", environment_id="dev")["versioned"] is False
    assert config_context.get_resource_config("test-key", environment_id="dev")["enable_key_rotation"] is True
    assert config_context.get_resource_config("test-ddbtable", environment_id="dev")["removal_policy"] == "retain"
    assert config_context.get_resource_config("test-eventbus", environment_id="dev")["removal_policy"] == "retain"


def test_get_resource_config_json(config_json: Config) -> None:
    assert config_json.get_resource_config("test-lambda", environment_id="dev")["memory_size"] == 512
    assert config_json.get_resource_config("test-bucket", environment_id="dev")["versioned"] is False
    assert config_json.get_resource_config("test-key", environment_id="dev")["enable_key_rotation"] is True
    assert config_json.get_resource_config("test-ddbtable", environment_id="dev")["removal_policy"] == "retain"
    assert config_json.get_resource_config("test-eventbus", environment_id="dev")["removal_policy"] == "retain"
