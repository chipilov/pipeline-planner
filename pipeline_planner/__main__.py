import argparse
import logging
from pipeline_planner.task_parser import TaskParser
from pipeline_planner.pipeline_planner import PipelinePlanner
from pipeline_planner.task_schedule_report import TaskScheduleReport


__MIN_CORES = 1
__MAX_CORES = 32
__LOG_LEVELS = {
    'error': logging.ERROR,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG
}


parser = argparse.ArgumentParser(description='Data Pipeline Planner')

parser.add_argument(
    '--cpu_cores', 
    type=int,
    required=True,
    action='append',
    help=f'the number of available CPU cores (must be between {__MIN_CORES} and {__MAX_CORES})'
)

parser.add_argument(
    '--pipeline',
    type=str,
    required=True,
    action='append',
    help='path to the file containing the pipeline tasks'
)

parser.add_argument(
    '--log',
    default='info',
    choices=['error', 'warning', 'info', 'debug'],
    help='Define the logging level. Default is "warning"',
)

args = parser.parse_args()

if (level := __LOG_LEVELS.get(args.log.lower())) is not None:
    logging.basicConfig(level=level)

if len(args.cpu_cores) != 1:
    parser.error('--cpu_cores argument must be specified exactly once')

if len(args.pipeline) != 1:
    parser.error('--pipeline argument must be specified exactly once')

__cpu_cores, __pipeline_path = args.cpu_cores[0], args.pipeline[0]

if __cpu_cores < __MIN_CORES or __cpu_cores > __MAX_CORES:
    parser.error(f'--cpu_cores argument be in the range [{__MIN_CORES}, {__MAX_CORES}]')

__task_lines = []
try:
    with open(__pipeline_path, 'r') as pipeline_file:
        __task_lines = [line for line in pipeline_file.read().split('\n')]
except Exception as e:
    parser.error(f'Pipeline tasks file "{__pipeline_path}" does NOT exist. Error: {e}')
finally:
    if len(__task_lines) < 5:
        parser.error(f'Pipeline tasks file "{__pipeline_path}" needs at least 5 lines of content.')

try:
    __tasks = TaskParser().parse(__task_lines)
    __scheduled_tasks = PipelinePlanner(__tasks).plan(__cpu_cores)
    print(TaskScheduleReport().generate(__scheduled_tasks))
except Exception as e:
    logging.error(f'Failed to generate a plan. Error: {e}')
