import unittest
from pipeline_planner.task_parser import TaskParser
from pipeline_planner.task import Task
from pipeline_planner.pipeline_planning_error import PipelinePlanningError


class TestParsing(unittest.TestCase):

    def test_valid_content(self):
        parsed = TaskParser().parse(
            ['A ', '2', ' feature ', '', 'B', '1', 'feature', '', 'C', '2', 'model', 'B', 'END']
        )

        self.assertEqual(len(parsed), 3)
        self.assertEqual(parsed[0], Task('A', 'feature', 2, set()))
        self.assertEqual(parsed[1], Task('B', 'feature', 1, set()))
        self.assertEqual(parsed[2], Task('C', 'model', 2, {'B'}))

    def test_invalid_line_count(self):
        with self.assertRaisesRegex(
            PipelinePlanningError, 'Unexpected line count'
        ):
            TaskParser().parse(
                ['A ', '2', ' feature ', '', 'B', '1', 'feature', '', 'C', '2', 'model']
            )

    def test_invalid_ending(self):
        with self.assertRaisesRegex(PipelinePlanningError, 'Last line should read'):
            TaskParser().parse(['A ', '2', ' feature ', '', 'EENDDD'])

    def test_empty_task_name(self):
        with self.assertRaisesRegex(
            PipelinePlanningError, 'Encountered a task with an invalid name'
        ):
            TaskParser().parse(['  ', '2', ' feature ', '', 'END'])

    def test_invalid_execution_time(self):
        with self.assertRaisesRegex(
            PipelinePlanningError, 'Encountered a task with an invalid execution time'
        ):
            TaskParser().parse(['A', 'NAN', ' feature ', '', 'END'])

        with self.assertRaisesRegex(
                PipelinePlanningError, 'Encountered a task with an invalid execution time'
        ):
            TaskParser().parse(['A', '0', ' feature ', '', 'END'])

        with self.assertRaisesRegex(
                PipelinePlanningError,
                'Encountered a task with an invalid execution time'
        ):
            TaskParser().parse(['A', '-1', ' feature ', '', 'END'])

    def test_empty_deps(self):
        parsed = TaskParser().parse(
            ['A ', '2', ' feature ', ',,', 'B', '1', 'feature', ',  ,', 'END']
        )

        self.assertEqual(len(parsed), 2)
        self.assertEqual(parsed[0], Task('A', 'feature', 2, set()))
        self.assertEqual(parsed[1], Task('B', 'feature', 1, set()))

    def test_duplicate_deps(self):
        parsed = TaskParser().parse(
            ['A ', '2', ' feature ', '', 'B', '1', 'feature', 'A, A', 'END']
        )

        self.assertEqual(len(parsed), 2)
        self.assertEqual(parsed[0], Task('A', 'feature', 2, set()))
        self.assertEqual(parsed[1], Task('B', 'feature', 1, {'A'}))

    def test_duplicate_task_names(self):
        with self.assertRaisesRegex(
                PipelinePlanningError, 'Encountered duplicate task names'
        ):
            TaskParser().parse(['A ', '2', ' group ', '', 'A', '1', '', '', 'END'])


if __name__ == '__main__':
    unittest.main()
