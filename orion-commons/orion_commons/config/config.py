#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import json
from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Any, Dict, Optional

from aws_cdk.core import ConstructNode, Environment


class ConfigStrategy(ABC):
    @abstractmethod
    def get_config(self, key: str) -> Any:
        pass


class CDKContextConfigStrategy(ConfigStrategy):
    def __init__(
        self,
        node: ConstructNode,
    ) -> None:
        self._node = node

    def get_config(self, key: str) -> Any:
        return self._node.try_get_context(key=key) or {}


class CDKJSONConfigStrategy(ConfigStrategy):
    def __init__(
        self,
        path: str = "./cdk.json",
    ) -> None:
        self._path = path
        with open(path) as f:
            self._config_file = json.load(f)

    def get_config(self, key: str) -> Any:
        return self._config_file.get("context", {}).get(key, {})


class Config:
    def __init__(
        self,
        config_strategy: Optional[ConfigStrategy] = None,
    ) -> None:
        self._config_strategy = config_strategy or CDKJSONConfigStrategy()

    @lru_cache(maxsize=None)
    def get_env_config(
        self,
        environment_id: str,
    ) -> Dict[str, Any]:
        return self._config_strategy.get_config(key="environments").get(environment_id, {})  # type: ignore

    def get_env(
        self,
        environment_id: str,
    ) -> Environment:
        env_config: Dict[str, Any] = self.get_env_config(environment_id=environment_id)
        return Environment(
            account=env_config.get("account"),
            region=env_config.get("region"),
        )

    def get_resource_config(
        self,
        id: str,
        environment_id: str,
    ) -> Dict[str, Any]:
        return self.get_env_config(environment_id=environment_id).get("resource_config", {}).get(id, {})  # type: ignore

    def get_cdk_version(self) -> str:
        return self._config_strategy.get_config(key="cdk_version")  # type: ignore
