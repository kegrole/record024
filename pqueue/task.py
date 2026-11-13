"""Task model."""

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class Task:
    """A single collection task.

    priority: higher value means more urgent.
    allowed_domains / allowed_path_prefixes: the access scope the task is
    authorized for. Any request outside this scope is a security event.
    """

    id: str
    key: str
    priority: int
    url: str
    allowed_domains: List[str]
    allowed_path_prefixes: List[str]
    payload: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Task":
        return cls(
            id=str(data["id"]),
            key=str(data["key"]),
            priority=int(data["priority"]),
            url=str(data["url"]),
            allowed_domains=list(data.get("allowed_domains", [])),
            allowed_path_prefixes=list(data.get("allowed_path_prefixes", [])),
            payload=dict(data.get("payload", {})),
        )
