from abc import abstractmethod
from typing import Any, Dict, List, Optional

from aws_cdk.aws_events import EventPattern, IRuleTarget
from aws_cdk.core import Construct

from .register import RegisterConstruct


class StageConstruct(Construct):
    def __init__(
        self,
        scope: Construct,
        pipeline_id: str,
        id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        stage_type: Optional[str] = None,
        props: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Stage constructor

        :param scope: Scope
        :param id: Identifier of the stage
        :param name: Name of the stage
        :param description: Description of the stage
        :param props: Properties of the stage
        """
        super().__init__(scope, f"{pipeline_id}-{id}-stage")

        self.pipeline_id: str = pipeline_id
        self.id: str = id
        self.name: str = name if name else ""
        self.description: str = description if description else ""
        self.type: str = stage_type if stage_type else "stage"
        props = {} if not props else props
        props["id"] = self.id
        props["name"] = self.name
        props["description"] = self.description
        props["type"] = self.type
        RegisterConstruct(self, self.id, props=props)

    @abstractmethod
    def get_targets(self) -> Optional[List[IRuleTarget]]:
        """
        Get input targets of the stage.
        """
        pass

    @abstractmethod
    def get_event_pattern(self) -> Optional[EventPattern]:
        """
        Get output event pattern of the stage.
        """
        pass
