from pipeline_planner.task import Task
from pipeline_planner.pipeline_planning_error import PipelinePlanningError


class TaskParser:
    """
    The class parses raw string lines into Task objects
    """

    def parse(self, tasks_raw: list[str]) -> list[Task]:
        if len(tasks_raw) < 5 or (len(tasks_raw) - 1) % 4 != 0:
            raise PipelinePlanningError(
                f'Invalid pipeline task definition. Unexpected line count: {len(tasks_raw)}'
            )
        
        if tasks_raw[-1] != 'END':
            raise PipelinePlanningError(
                f'Invalid pipeline task definition. Last line should read "END", instead found: {tasks_raw[-1]}'
            )

        parsed_tasks = []
        for i in range(0, len(tasks_raw) - 4, 4):
            parsed_tasks.append(TaskParser.__parse_task(*tasks_raw[i:i + 4]))

        if len(parsed_tasks) != len(set([p_task.name for p_task in parsed_tasks])):
            raise PipelinePlanningError(
                f'Invalid pipeline task definition. Encountered duplicate task names.'
            )

        return parsed_tasks

    @classmethod
    def __parse_task(
       cls, name: str, execution_time: str, group: str, deps: str
    ) -> Task:
        parsed_name = name.strip()

        if len(parsed_name) == 0:
            raise PipelinePlanningError(f'Encountered a task with an invalid name: "{name}"')
        
        try:
            parsed_execution_time = int(execution_time)
            if parsed_execution_time < 1:
                raise Exception
        except Exception:
            raise PipelinePlanningError(
                f'Encountered a task with an invalid execution time! Task: "{name}", execution time: "{execution_time}"'
            )
        
        parsed_deps = [
            dep.strip() for dep in deps.split(",") if len(dep.strip()) > 0
        ]
        
        return Task(
            parsed_name, group.strip(), parsed_execution_time, set(parsed_deps)
        )
