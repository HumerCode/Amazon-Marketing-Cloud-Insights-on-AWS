# Copyright (c) 2021 Amazon.com, Inc. or its affiliates
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from typing import Any, Dict, Optional

from aws_cdk.core import Construct

from .register import RegisterConstruct


class DatasetConstruct(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        transforms: Optional[Dict[str, str]] = None,
        dataset_type: Optional[str] = None,
        props: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Dataset constructor

        :param scope: Scope
        :param id: Identifier of the dataset
        """
        super().__init__(scope, f"{id}-dataset")

        self.id: str = id
        self.name: str = name if name else ""
        self.description: str = description if description else ""
        self.transforms: Dict[str, str] = transforms if transforms else {}
        self.type: str = dataset_type if dataset_type else "dataset"
        props = {} if not props else props
        props["id"] = self.id
        props["name"] = self.name
        props["description"] = self.description
        props["transforms"] = self.transforms
        props["type"] = self.type
        RegisterConstruct(self, self.id, props=props)
