# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from typing import Any, Dict

from aws_cdk.core import Construct, CustomResource
from aws_cdk.aws_ssm import StringParameter

def get_ssm_value(scope: Construct, id: str, parameter_name: str) -> str:
    return StringParameter.from_string_parameter_name(
        scope,
        id=id,
        string_parameter_name=parameter_name,
    ).string_value

class RegisterConstruct(Construct):
    def __init__(self, scope: Construct, id: str, props: Dict[str, Any]) -> None:
        super().__init__(scope, f"{id}-{props['type']}-register")
        CustomResource(
            self,
            f"{id}-{props['type']}-custom-resource",
            service_token=get_ssm_value(
                self,
                id=f"{id}-{props['type']}-provider-ssm",
                parameter_name="/Orion/Lambda/RegisterProviderServiceToken",
            ),
            properties={"RegisterProperties": props},
        )
