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

import aws_cdk as cdk

from aws_ddk_core.cicd import CICDPipelineStack
from amc_quickstart.utils.application_config import GetApplicationParameters
from amc_quickstart.foundations.foundations_stack import FoundationsStack
from amc_quickstart.data_lake.pipelines import SDLFPipelineStack
from amc_quickstart.data_lake.datasets import SDLFDatasetStack

from amc_quickstart.microservices.platform_management_notebooks import PlatformManagerSageMaker
from amc_quickstart.microservices.customer_management_service import TenantProvisiongService
from amc_quickstart.microservices.data_lake_hydration_service import WorkFlowManagerService
from aws_ddk_core.config import Config

class AMCDeliveryKit(cdk.Stage):
    def __init__(
        self,
        scope,
        environment_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, f"AMC-{environment_id}-QuickStart", **kwargs)

        self._environment_id = environment_id
        params = GetApplicationParameters(environment_id=self._environment_id)
        self._resource_prefix = params.get_resource_prefix()
        self._sdlf_params = params.get_data_pipeline_params()
        self._team = self._sdlf_params.get("team", "demoteam")
        self._dataset = self._sdlf_params.get("dataset", "amcdataset")
        self._pipeline = self._sdlf_params.get("pipeline", "adtech")
        self._app = self._sdlf_params.get("app", "datalake")
        self._org = self._sdlf_params.get("org", "aws")

        # Foundations
        foundations_stack = FoundationsStack(self, f"{self._resource_prefix}-foundations", environment_id=environment_id, resource_prefix=self._resource_prefix, app=self._app, org=self._org)

        # Pipelines
       
        sdlf_pipeline_stack = SDLFPipelineStack(self, f"{self._resource_prefix}-data-lake-pipeline", environment_id=environment_id, resource_prefix=self._resource_prefix, params=self._sdlf_params)
        
        sdlf_pipeline_stack.add_dependency(
            foundations_stack
        )

        # MICROSERVICES
        # WFM
        # self._wfm_params = params.get_wfm_params()
        # self._wfm_team = self._wfm_params.get("team", "demoteam")
        # self._wfm_pipeline = self._wfm_params.get("pipeline", "dlhs")
        # self._wfm_dataset = self._wfm_params.get("dataset", "amcdataset")
        wfm_stack = WorkFlowManagerService(self, f"{self._resource_prefix}-wfm", environment_id=environment_id, microservice="wfm", team=self._team, resource_prefix=self._resource_prefix)
        wfm_stack.add_dependency(
            foundations_stack
        ) 

        # TPS
        # self._tps_params = params.get_tps_params()
        # self._tps_team = self._tps_params.get("team", "demoteam")
        # self._tps_pipeline = self._tps_params.get("pipeline", "cmpl")
        tps_stack = TenantProvisiongService(self, f"{self._resource_prefix}-tps", environment_id=environment_id, microservice="tps", team=self._team, resource_prefix=self._resource_prefix)
        tps_stack.add_dependency(
            foundations_stack
        ) 

        # PMN
        pmn_stack = PlatformManagerSageMaker(self, f"{self._resource_prefix}-platform-manager", environment_id=environment_id, microservice="platform-manager", resource_prefix=self._resource_prefix)
        pmn_stack.add_dependency(
            foundations_stack
        ) 
        
        # Datasets 
        SDLFDatasetStack(self, f"{self._resource_prefix}-data-lake-datasets", environment_id=environment_id, resource_prefix=self._resource_prefix, params=self._sdlf_params).add_dependency(
            sdlf_pipeline_stack
        )

satellite_app = cdk.App()
config = Config()

cicd_repository_name = GetApplicationParameters(environment_id="cicd").get_cicd_repository()
pipeline_name = "ddk-amc-quickstart-pipeline"
pipeline = (
    CICDPipelineStack(satellite_app, id=pipeline_name, environment_id="dev",  pipeline_name=pipeline_name)
    .add_source_action(repository_name=cicd_repository_name)
    .add_synth_action()
    .build()
    .add_stage("dev", AMCDeliveryKit(satellite_app, environment_id="dev", env=config.get_env("dev")))
)   

satellite_app.synth()