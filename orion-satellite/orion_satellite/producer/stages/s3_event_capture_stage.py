from dataclasses import dataclass
from typing import Any, List, Optional

from aws_cdk.aws_events import EventPattern, IRuleTarget
from aws_cdk.core import Construct
from orion_commons import StageConstruct



@dataclass
class S3EventCaptureStageConfig:
    bucket_name: Optional[str]
    key_prefix: Optional[str]
    event_names: Optional[List[str]]


class S3EventCaptureStage(StageConstruct):
    """
    Stage sole purpose is to trigger next stages in the pipeline based on S3 events.
    """

    def __init__(
        self,
        scope: Construct,
        pipeline_id: str,
        id: str,
        config: S3EventCaptureStageConfig,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, pipeline_id, id, **kwargs)
        self._config: S3EventCaptureStageConfig = config

    def get_event_pattern(self) -> Optional[EventPattern]:
        return EventPattern(
            source=["aws.s3"],
            detail={
                "eventSource": ["s3.amazonaws.com"],
                "eventName": self._config.event_names,
                "requestParameters": {
                    "bucketName": [self._config.bucket_name],
                    "key": [{"prefix": self._config.key_prefix}],
                },
            },
        )

    def get_targets(self) -> Optional[List[IRuleTarget]]:
        return None
