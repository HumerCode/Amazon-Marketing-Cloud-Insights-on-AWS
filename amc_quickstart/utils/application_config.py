# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any, Dict
import json

class GetApplicationParameters():
    def __init__(
        self,
        environment_id: str,
    ) -> None:

        with open("./ddk.json") as f:
            self._config_file = json.load(f)

        self._environment_id: str = environment_id
        self._config = self._config_file.get("environments", {}).get(environment_id, {})
        
    def get_data_pipeline_params(self) -> Dict[str, Any]:
        return self._config.get("data_pipeline_parameters", {})
        
    def get_resource_prefix(self) -> str:
        return self._config.get("resource_prefix", "ddk")

    def get_cicd_repository(self) -> Dict[str, Any]:
        return self._config.get("repository", "ddk-amc-quickstart")