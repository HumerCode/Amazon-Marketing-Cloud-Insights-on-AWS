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

from aws_cdk.aws_ssm import StringParameter
import aws_cdk as cdk
from constructs import Construct



def get_ssm_value(scope, id: str, parameter_name: str) -> str:
    return StringParameter.from_string_parameter_name(
        scope,
        id=id,
        string_parameter_name=parameter_name,
    ).string_value

class RegisterConstruct(Construct):
    def __init__(self, scope, id: str, props: Dict[str, Any]) -> None:
        super().__init__(scope, f"{id}-{props['type']}-register")
        cdk.CustomResource(
            self,
            f"{id}-{props['type']}-custom-resource",
            service_token=get_ssm_value(
                self,
                id=f"{id}-{props['type']}-provider-ssm",
                parameter_name="/AMC/Lambda/RegisterProviderServiceToken",
            ),
            properties={"RegisterProperties": props},
        )
