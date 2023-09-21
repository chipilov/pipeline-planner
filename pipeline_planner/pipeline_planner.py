import collections
import logging
import itertools as it
from ortools.sat.python import cp_model
from pipeline_planner.task import Task, ScheduledTask
from pipeline_planner.pipeline_planning_error import PipelinePlanningError


def _task_pair_key(l_task: str, r_task: str) -> str:
    return f'{l_task}_{r_task}' if l_task < r_task else f'{r_task}_{l_task}'


class PipelinePlanner:
    """
    The main class implementing the scheduling of the tasks.

    In addition to scheduling, the class also performs sanity checks on the
    tasks:
        - ensures there are no tasks with duplicated names;
        - ensures there are no circular dependencies;
        - ensures all dependencies are also exist as tasks;
    """

    __tasks: dict[str, Task]
    __groups: dict[str, list[Task]]
    __horizon: int

    def __init__(self, tasks: list[Task]):
        self.__tasks = {}
        self.__groups = collections.defaultdict(list[Task])
        self.__horizon = sum(task.execution_time for task in tasks)

        self.__build_associativity(tasks)

    def __build_associativity(self, tasks: list[Task]) -> None:
        self.__tasks = {task.name: task for task in tasks}

        if len(tasks) != len(self.__tasks):
            raise PipelinePlanningError(
                'Pipeline tasks contain duplicated task names.'
            )

        for task in tasks:
            if task.has_group():
                self.__groups[task.group].append(task)

                for dep_name in task.dependencies:
                    if dep_name not in self.__tasks:
                        raise PipelinePlanningError(
                            f'Task {task.name} has non-existent dependency: {dep_name}'
                        )

    def plan(self, cpu_cores: int) -> list[ScheduledTask]:
        """
        The plan is a slight variation on the flexible jobshop problem: https://github.com/google/or-tools/blob/master/examples/python/flexible_job_shop_sat.py
        The only modifications needed are:
            - each task takes the same amount of time, regardless of the CPU
              core on which it is executed;
            - the task dependencies can be between any task, not only between
              tasks of a single job. This can be modeled by simply enforcing
              the deps between any 2 tasks, regardless of the group they belong
              to;
            - ensure that tasks from different groups cannot be executed
              simultaneously. This can be modeled by ensuring that tasks from
              different group do NOT overlap. See: https://github.com/google/or-tools/blob/554cbccaa95b4c11eced13f145de1bf468e1f919/ortools/sat/samples/no_overlap_sample_sat.py#L50
        """

        logging.debug(f'Tasks horizon = {self.__horizon}')

        intervals_per_core = collections.defaultdict(list)  # indexed by core id
        intervals = {}  # indexed by task name
        starts = {}  # indexed by task name
        ends = {}  # indexed by task name
        presences = {}  # indexed by (task name, cpu core id).

        model = cp_model.CpModel()

        # Create the relevant variables and intervals
        for task in self.__tasks.values():
            # Create main interval for the task
            suffix_name = task.name
            start = model.NewIntVar(0, self.__horizon, f'start_{suffix_name}')
            end = model.NewIntVar(0, self.__horizon, f'end_{suffix_name}')
            interval = model.NewIntervalVar(
                start, task.execution_time, end, f'interval_{suffix_name}'
            )

            intervals[task.name] = interval
            starts[task.name] = start
            ends[task.name] = end

            # Create alternative intervals for the different cpu cores
            if cpu_cores > 1:
                l_presences = []
                for core in range(cpu_cores):
                    alt_suffix = f'_{task.name}_{core}'
                    l_presence = model.NewBoolVar(f'presence_{alt_suffix}')
                    l_start = model.NewIntVar(0, self.__horizon, f'start_{alt_suffix}')
                    l_end = model.NewIntVar(0, self.__horizon, f'end_{alt_suffix}')
                    l_interval = model.NewOptionalIntervalVar(
                        l_start, task.execution_time, l_end, l_presence, f'interval_{alt_suffix}'
                    )
                    l_presences.append(l_presence)

                    # Link the master variables with the local ones
                    model.Add(start == l_start).OnlyEnforceIf(l_presence)
                    model.Add(end == l_end).OnlyEnforceIf(l_presence)

                    # Add the local interval to the relevant cpu core
                    intervals_per_core[core].append(l_interval)

                    presences[(task.name, core)] = l_presence

                model.AddExactlyOne(l_presences)
            else:
                intervals_per_core[0].append(interval)
                presences[(task.name, 0)] = model.NewConstant(1)

        # Enforce task dependencies
        for task in self.__tasks.values():
            for dep in task.dependencies:
                model.Add(starts[task.name] >= ends[dep])

        # Ensure tasks from different groups cannot run simultaneously
        for g1_tasks, g2_tasks in it.combinations(self.__groups.values(), 2):
            for g1_task, g2_task in it.product(g1_tasks, g2_tasks):
                model.AddNoOverlap(
                    [intervals[g1_task.name], intervals[g2_task.name]]
                )

        # Ensure each CPU core can run a single task at a time
        for core in range(cpu_cores):
            intervals = intervals_per_core[core]
            if len(intervals) > 1:
                model.AddNoOverlap(intervals)

        # Define the objective
        makespan = model.NewIntVar(0, self.__horizon, 'makespan')
        model.AddMaxEquality(makespan, [end for end in ends.values()])
        model.Minimize(makespan)

        solver = cp_model.CpSolver()
        status = solver.StatusName(solver.Solve(model))

        if status == 'INFEASIBLE':
            raise PipelinePlanningError('Impossible to schedule tasks - check for circular dependencies.')

        if status != 'OPTIMAL':
            raise PipelinePlanningError('Failed to find an optimal solution.')

        # Generate the final result
        scheduled_tasks = []
        for task in self.__tasks.values():
            task_start = solver.Value(starts[task.name])
            selected_core = next(
                (core for core in range(cpu_cores) if solver.Value(presences[(task.name, core)])), -1
            )

            scheduled_tasks.append(ScheduledTask(task, selected_core, task_start))

            logging.debug(
                f'Task {task.name} starts at {task_start} on cpu core {selected_core} (duration={task.execution_time})'
            )

        logging.debug(f'Found optimal solution in {solver.WallTime()} second(s).')

        return scheduled_tasks
