from dataclasses import dataclass


@dataclass
class Task:
    """Describes the properties of a pipeline task."""
    name: str
    group: str
    execution_time: int
    dependencies: set[str]

    def has_group(self) -> bool:
        return len(self.group) > 0


@dataclass
class ScheduledTask:
    """
    Describes how the task is scheduled (i.e. it's start time, end time and core)
    """
    task: Task
    core: int
    start: int
