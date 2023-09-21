import collections
from tabulate import tabulate
from pipeline_planner.task import ScheduledTask


class TaskScheduleReport:
    def generate(self, scheduled_tasks: list[ScheduledTask]) -> str:
        execution_time = max(s_task.start + s_task.task.execution_time for s_task in scheduled_tasks)
        summary = f'Minimum Execution Time = {execution_time} minute(s)\n'

        timestamps_to_tasks = collections.defaultdict(list[ScheduledTask])
        for s_task in scheduled_tasks:
            for task_timestamp in range(s_task.start, s_task.start + s_task.task.execution_time):
                timestamps_to_tasks[task_timestamp].append(s_task)

        table_data = [
            [
                ts + 1,
                ','.join(sorted([s_task.task.name for s_task in scheduled_tasks])),
                ','.join(set(s_task.task.group for s_task in scheduled_tasks if len(s_task.task.group) > 0))
            ] for ts, scheduled_tasks in sorted(timestamps_to_tasks.items())
        ]

        table = tabulate(
            table_data,
            headers=['Time', 'Tasks being Executed', 'Group Name'],
            tablefmt="github",
            numalign='left'
        ).replace('|\n', '\n')[:-1]

        return summary + table
