import unittest
from pipeline_planner.task_schedule_report import TaskScheduleReport
from pipeline_planner.task import Task, ScheduledTask


class TestReport(unittest.TestCase):

    def test_report_generation(self):
        report = TaskScheduleReport().generate([
            ScheduledTask(Task('T2', '', 1, set()), core=0, start=0),
            ScheduledTask(Task('T1', 'G1', 3, set()), core=1, start=0),
            ScheduledTask(Task('T3', '', 4, set()), core=2, start=0),
        ])

        self.assertEqual(
            report,
"""
Minimum Execution Time = 4 minute(s)
| Time   | Tasks being Executed   | Group Name   
|--------|------------------------|--------------
| 1      | T1,T2,T3               | G1           
| 2      | T1,T3                  | G1           
| 3      | T1,T3                  | G1           
| 4      | T3                     |              
"""[1:-1]
        )

        report = TaskScheduleReport().generate([
            ScheduledTask(Task('T2', 'G1', 1, set()), core=0, start=0),
            ScheduledTask(Task('T1', 'G1', 2, set()), core=0, start=1),
            ScheduledTask(Task('T3', '', 2, set()), core=0, start=3),
        ])

        self.assertEqual(
            report,
"""
Minimum Execution Time = 5 minute(s)
| Time   | Tasks being Executed   | Group Name   
|--------|------------------------|--------------
| 1      | T2                     | G1           
| 2      | T1                     | G1           
| 3      | T1                     | G1           
| 4      | T3                     |              
| 5      | T3                     |              
"""[1:-1]
        )

    def test_parallel_tasks_from_group_and_no_group(self):
        report = TaskScheduleReport().generate([
            ScheduledTask(Task('T1', 'G1', 2, set()), core=0, start=0),
            ScheduledTask(Task('T2', '', 2, set()), core=0, start=0),
        ])

        self.assertEqual(
            report,
"""
Minimum Execution Time = 2 minute(s)
| Time   | Tasks being Executed   | Group Name   
|--------|------------------------|--------------
| 1      | T1,T2                  | G1           
| 2      | T1,T2                  | G1           
"""[1:-1]
        )

    def test_sorted_timestamps(self):
        report1 = TaskScheduleReport().generate([
            ScheduledTask(Task('T2', 'G1', 1, set()), core=0, start=0),
            ScheduledTask(Task('T1', 'G1', 2, set()), core=0, start=1),
            ScheduledTask(Task('T3', '', 2, set()), core=0, start=3),
        ])

        # Same as above but shuffled
        report2 = TaskScheduleReport().generate([
            ScheduledTask(Task('T3', '', 2, set()), core=0, start=3),
            ScheduledTask(Task('T2', 'G1', 1, set()), core=0, start=0),
            ScheduledTask(Task('T1', 'G1', 2, set()), core=0, start=1),
        ])

        self.assertEqual(report1, report2)

    def test_sorted_task_names(self):
        report1 = TaskScheduleReport().generate([
            ScheduledTask(Task('T1', 'G1', 2, set()), core=0, start=0),
            ScheduledTask(Task('T2', '', 2, set()), core=0, start=0),
        ])

        # Same as above but shuffled
        report2 = TaskScheduleReport().generate([
            ScheduledTask(Task('T2', '', 2, set()), core=0, start=0),
            ScheduledTask(Task('T1', 'G1', 2, set()), core=0, start=0),
        ])

        self.assertEqual(report1, report2)


if __name__ == '__main__':
    unittest.main()
