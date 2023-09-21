import unittest
from pipeline_planner.pipeline_planner import PipelinePlanner
from pipeline_planner.pipeline_planning_error import PipelinePlanningError
from pipeline_planner.task import Task, ScheduledTask


class TestPlanner(unittest.TestCase):

    def test_tiny_plan_with_one_core(self):
        tasks = [
            Task('A', 'feature', 2, set()),
            Task('B', 'feature', 1, set()),
            Task('C', 'model', 2, {'B'})
        ]

        scheduled_tasks = PipelinePlanner(tasks).plan(cpu_cores=1)

        self.assertEqual(len(scheduled_tasks), 3)
        self.assertEqual(scheduled_tasks[0], ScheduledTask(tasks[0], core=0, start=1))
        self.assertEqual(scheduled_tasks[1], ScheduledTask(tasks[1], core=0, start=0))
        self.assertEqual(scheduled_tasks[2], ScheduledTask(tasks[2], core=0, start=3))

    def test_tiny_plan_with_two_cores(self):
        tasks = [
            Task('A', 'feature', 2, set()),
            Task('B', 'feature', 1, set()),
            Task('C', 'model', 2, {'B'})
        ]

        scheduled_tasks = PipelinePlanner(tasks).plan(cpu_cores=2)

        self.assertEqual(len(scheduled_tasks), 3)
        self.assertEqual(scheduled_tasks[0], ScheduledTask(tasks[0], core=1, start=0))
        self.assertEqual(scheduled_tasks[1], ScheduledTask(tasks[1], core=0, start=0))
        self.assertEqual(scheduled_tasks[2], ScheduledTask(tasks[2], core=1, start=2))

    def test_small_plan_with_two_and_three_cores(self):
        tasks = [
            Task('A', 'raw', 48, set()),
            Task('A1', 'raw', 5, {'A'}),
            Task('B', 'feature', 26, {'A'}),
            Task('C', 'feature', 55, {'B'}),
            Task('D', 'raw', 4, set()),
            Task('E', 'feature', 20, {'C', 'D'}),
            Task('F', 'model', 24, {'C'}),
            Task('G', 'model', 40, {'B', 'F'}),
            Task('H', 'feature', 29, set()),
            Task('Z', 'model', 58, {'H'})
        ]

        planner = PipelinePlanner(tasks)

        scheduled_tasks_2_cores = planner.plan(cpu_cores=2)

        self.assertEqual(len(scheduled_tasks_2_cores), 10)
        self.assertEqual(scheduled_tasks_2_cores[0], ScheduledTask(tasks[0], core=1, start=0))
        self.assertEqual(scheduled_tasks_2_cores[1], ScheduledTask(tasks[1], core=1, start=48))
        self.assertEqual(scheduled_tasks_2_cores[2], ScheduledTask(tasks[2], core=1, start=53))
        self.assertEqual(scheduled_tasks_2_cores[3], ScheduledTask(tasks[3], core=0, start=79))
        self.assertEqual(scheduled_tasks_2_cores[4], ScheduledTask(tasks[4], core=0, start=0))
        self.assertEqual(scheduled_tasks_2_cores[5], ScheduledTask(tasks[5], core=1, start=134))
        self.assertEqual(scheduled_tasks_2_cores[6], ScheduledTask(tasks[6], core=1, start=154))
        self.assertEqual(scheduled_tasks_2_cores[7], ScheduledTask(tasks[7], core=1, start=178))
        self.assertEqual(scheduled_tasks_2_cores[8], ScheduledTask(tasks[8], core=1, start=79))
        self.assertEqual(scheduled_tasks_2_cores[9], ScheduledTask(tasks[9], core=0, start=154))

        time_with_2_cores = max([s_task.start + s_task.task.execution_time for s_task in scheduled_tasks_2_cores])
        time_with_3_cores = max([s_task.start + s_task.task.execution_time for s_task in planner.plan(cpu_cores=3)])

        # Three cores make no difference because of the dependencies and grouping of the tasks
        self.assertEqual(time_with_2_cores, time_with_3_cores)

    def test_modified_small_plan_with_two_and_three_cores(self):
        tasks = [
            Task('A', 'raw', 48, set()),
            Task('A1', 'raw', 5, {'A'}),
            Task('B', 'feature', 26, {'A'}),
            Task('C', 'feature', 10, {'B'}),
            Task('D', 'raw', 4, set()),
            Task('E', 'feature', 20, {'D'}),
            Task('F', 'model', 24, {'C'}),
            Task('G', 'model', 40, {'B', 'F'}),
            Task('H', 'feature', 29, set()),
            Task('Z', 'model', 58, {'H'})
        ]

        planner = PipelinePlanner(tasks)

        time_with_2_cores = max([s_task.start + s_task.task.execution_time for s_task in planner.plan(cpu_cores=2)])
        time_with_3_cores = max([s_task.start + s_task.task.execution_time for s_task in planner.plan(cpu_cores=3)])

        self.assertEqual(time_with_2_cores, 163)
        self.assertEqual(time_with_3_cores, 153)

    def test_infeasible_due_to_circular_dependency(self):
        with self.assertRaisesRegex(
                PipelinePlanningError, 'Impossible to schedule tasks - check for circular dependencies'
        ):
            PipelinePlanner([
                Task('A', 'feature', 2, {'B'}),
                Task('B', 'feature', 2, {'C'}),
                Task('C', 'feature', 2, {'A'})
            ]).plan(cpu_cores=2)

    def test_tasks_without_group_can_run_in_parallel_with_tasks_from_groups(self):
        tasks = [
            Task('A', 'group', 4, set()),
            Task('B', '', 4, set()),
        ]

        planner = PipelinePlanner(tasks)

        self.assertEqual(max([s_task.start + s_task.task.execution_time for s_task in planner.plan(cpu_cores=1)]), 8)
        self.assertEqual(max([s_task.start + s_task.task.execution_time for s_task in planner.plan(cpu_cores=2)]), 4)

    def test_tasks_from_group_cannot_run_in_parallel_with_tasks_from_other_groups(self):
        tasks = [
            Task('A', 'group1', 4, set()),
            Task('B', 'group2', 4, set()),
        ]

        planner = PipelinePlanner(tasks)

        self.assertEqual(max([s_task.start + s_task.task.execution_time for s_task in planner.plan(cpu_cores=1)]), 8)
        self.assertEqual(max([s_task.start + s_task.task.execution_time for s_task in planner.plan(cpu_cores=2)]), 8)

    def test_scale_linearly_when_no_dependencies(self):
        tasks = [
            Task('A', '', 4, set()),
            Task('B', '', 4, set()),
            Task('C', '', 4, set()),
            Task('D', '', 4, set()),
            Task('E', '', 4, set()),
            Task('F', '', 4, set()),
            Task('G', '', 4, set()),
            Task('H', '', 4, set()),
        ]

        planner = PipelinePlanner(tasks)

        self.assertEqual(max([s_task.start + s_task.task.execution_time for s_task in planner.plan(cpu_cores=1)]), 32)
        self.assertEqual(max([s_task.start + s_task.task.execution_time for s_task in planner.plan(cpu_cores=2)]), 16)
        self.assertEqual(max([s_task.start + s_task.task.execution_time for s_task in planner.plan(cpu_cores=4)]), 8)
        self.assertEqual(max([s_task.start + s_task.task.execution_time for s_task in planner.plan(cpu_cores=8)]), 4)

    def test_cannot_scale_due_to_explicit_dependencies(self):
        tasks = [
            Task('A', '', 4, set()),
            Task('B', '', 4, {'A'}),
            Task('C', '', 4, {'B'}),
            Task('D', '', 4, {'C'}),
            Task('E', '', 4, {'D'}),
            Task('F', '', 4, {'E'}),
            Task('G', '', 4, {'F'}),
            Task('H', '', 4, {'G'}),
        ]

        planner = PipelinePlanner(tasks)

        self.assertEqual(max([s_task.start + s_task.task.execution_time for s_task in planner.plan(cpu_cores=1)]), 32)
        self.assertEqual(max([s_task.start + s_task.task.execution_time for s_task in planner.plan(cpu_cores=2)]), 32)
        self.assertEqual(max([s_task.start + s_task.task.execution_time for s_task in planner.plan(cpu_cores=4)]), 32)
        self.assertEqual(max([s_task.start + s_task.task.execution_time for s_task in planner.plan(cpu_cores=8)]), 32)


if __name__ == '__main__':
    unittest.main()
