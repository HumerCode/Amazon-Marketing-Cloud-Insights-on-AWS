#!/usr/bin/env python3
# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from typing import Any, Dict
import json

from aws_cdk.core import App, Construct, Stage, Fn
from orion_commons import CICDPipeline, Config
from orion_satellite.foundations.foundations_stack import FoundationsStack
from orion_satellite.foundations.cicd_event_rule_stack import CICDEventRuleStack
from orion_satellite.producer.datasets import SDLFDatasetStack
from orion_satellite.producer.pipelines import SDLFPipelineStack
from orion_satellite.microservices.PlatformManagementNotebooks import PlatformManagerSageMaker
from orion_satellite.microservices.CustomerManagementService import TenantProvisiongService
from orion_satellite.microservices.DataLakeHydrationMicroservices import WorkFlowManagerService


class GetApplicationParameters():
    def __init__(
        self,
        environment_id: str,
    ) -> None:

        with open("./cdk.json") as f:
            self._config_file = json.load(f)

        self._environment_id: str = environment_id
        self._config = self._config_file.get("context", {}).get("environments", {}).get(environment_id, {})
        
    def get_data_pipeline_params(self) -> Dict[str, Any]:
        return self._config.get("data_pipeline_parameters", {})

    def get_tps_params(self) -> Dict[str, Any]:
        return self._config.get("tps_parameters", {})

    def get_wfm_params(self) -> Dict[str, Any]:
        return self._config.get("wfm_parameters", {})

    def get_platform_manager_params(self) -> Dict[str, Any]:
        return self._config.get("platform_manager_parameters", {})

class DataPipelineStage(Stage):
    def __init__(
        self,
        scope: Construct,
        environment_id: str,
        params: Dict[str, Any],
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, f"orion-{environment_id}-satellite", **kwargs)

        self._params = params
        self._app = self._params.get("app", "datalake")
        self._org = self._params.get("org", "aws")
        
        # Foundations
        foundations_stack = FoundationsStack(self, "orion-foundations", environment_id=environment_id, app=self._app, org=self._org)

        # Pipelines
       
        sdlf_pipeline_stack = SDLFPipelineStack(self, "orion-sdlf-pipeline", environment_id=environment_id, params=self._params)
        
        sdlf_pipeline_stack.add_dependency(
            foundations_stack
        )
        
        # Datasets 
        SDLFDatasetStack(self, "orion-sdlf-datasets", environment_id=environment_id,  params=self._params).add_dependency(
            sdlf_pipeline_stack
        )


class EventStage(Stage):
    def __init__(
        self,
        scope: Construct,
        environment_id: str,
        pipeline_name: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, f"orion-{environment_id}-satellite", **kwargs)
    
        # Gets deployed in cicd account to trigger satellite pipeline after artifacts pipeline
        CICDEventRuleStack(self, "orion-cicd-event-rule", environment_id=environment_id, pipeline_name=pipeline_name)

class MicroserviceStage(Stage):
    def __init__(
        self,
        scope: Construct,
        environment_id: str,
        tps_params: Dict[str, Any],
        wfm_params: Dict[str, Any],
        pmn_params: Dict[str, Any],
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, f"orion-{environment_id}-microservices", **kwargs)
        
        # WFM
        self._wfm_params = wfm_params
        self._wfm_team = self._wfm_params.get("team", "demoteam")
        self._wfm_pipeline = self._wfm_params.get("pipeline", "dlhs")
        self._wfm_dataset = self._wfm_params.get("dataset", "amcdataset")
        WorkFlowManagerService(self, "orion-wfm", environment_id=environment_id, team=self._wfm_team, microservice="wfm", pipeline=self._wfm_pipeline, dataset=self._wfm_dataset)
        
        # TPS
        self._tps_params = tps_params
        self._tps_team = self._tps_params.get("team", "demoteam")
        self._tps_pipeline = self._tps_params.get("pipeline", "cmpl")
        TenantProvisiongService(self, "orion-tps", environment_id=environment_id, team=self._tps_team, microservice="tps", pipeline=self._tps_pipeline)

        # PMN
        self._pmn_params = pmn_params
        self._pmn_team = self._pmn_params.get("team", "demoteam")
        PlatformManagerSageMaker(self, "orion-platform-manager", environment_id=environment_id, team=self._pmn_team, microservice="platform-manager")


satellite_app = App()
config = Config()

params = GetApplicationParameters(environment_id="dev")

pipeline_name = "orion-satellite-pipeline"
(
    CICDPipeline(satellite_app, pipeline_id=pipeline_name, pipeline_name=pipeline_name)
    .add_artifacts()
    .add_source_action(repository_name="orion-satellite")
    .add_synth_action()
    .build()
    .add_stage("dev", DataPipelineStage(satellite_app, environment_id="dev", env=config.get_env("dev"), params=params.get_data_pipeline_params()))
    .add_stage("dev", MicroserviceStage(satellite_app, environment_id="dev", env=config.get_env("dev"), tps_params=params.get_tps_params(), wfm_params=params.get_wfm_params(), pmn_params=params.get_platform_manager_params()), execute_tests=False)
    .add_stage("cicd", EventStage(satellite_app, environment_id="cicd", pipeline_name=pipeline_name, env=config.get_env("cicd")), execute_tests=False)
)   
satellite_app.synth()