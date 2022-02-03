from typing import Any, Dict, List, Optional

from aws_cdk.aws_events import EventPattern, IRuleTarget, Rule
from aws_cdk.core import Construct

from .register import RegisterConstruct
from .stage import StageConstruct


class PipelineConstruct(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        pipeline_type: Optional[str] = None,
        props: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Pipeline constructor

        :param scope: Scope
        :param id: Identifier of the pipeline
        :param name: Name of the pipeline
        :param description: Description of the pipeline
        :param props: Properties of the pipeline
        """
        super().__init__(scope, f"{id}-pipeline")

        self.id: str = id
        self.name: str = name if name else ""
        self.description: str = description if description else ""
        props = {} if not props else props
        props["id"] = self.id
        props["name"] = self.name
        props["description"] = self.description
        props["type"] = pipeline_type if pipeline_type else "pipeline"
        RegisterConstruct(self, self.id, props=props)
        self._prev_stage: Optional[StageConstruct] = None
        self._rules: List[Rule] = []

    def add_stage(
        self, stage: StageConstruct, skip_rule: bool = False, override_rule: Optional[Rule] = None
    ) -> "PipelineConstruct":
        """
        Add stage to the pipeline.

        :param stage: Stage implementation
        :param skip_rule: Skip creation of the default rule automatically linking previous stage with the next one
        :param override_rule: Override the default rule by the rule specified in this parameter
        :return: self
        """
        if override_rule:
            self.add_rule(override_rule=override_rule)
        elif self._prev_stage and not skip_rule:
            self.add_rule(
                id=f"{self.id}-{stage.id}-rule",
                event_pattern=self._prev_stage.get_event_pattern(),
                event_targets=stage.get_targets(),
            )
        self._prev_stage = stage
        return self

    def add_rule(
        self,
        id: Optional[str] = None,
        event_pattern: Optional[EventPattern] = None,
        event_targets: Optional[List[IRuleTarget]] = None,
        override_rule: Optional[Rule] = None,
    ) -> "PipelineConstruct":
        """
        Create a rule that connects event pattern of the previous stage to the target of the current stage.

        :param id: Identifier of the rule
        :param event_pattern: Event pattern of the rule
        :param event_targets: Target of the rule - usually previous_stage.get_targets()
        :param override_rule: Custom rule
        :return: self
        """
        self._rules.append(
            override_rule
            if override_rule
            else Rule(
                self,
                id,  # type: ignore
                event_pattern=event_pattern,
                targets=event_targets,
            )
        )
        return self
