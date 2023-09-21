# Overview

This Python package provides a solution to the problem of scheduling computation
tasks across ```n``` CPU cores while taking into account any grouping or
dependencies between the tasks.

The runner of the pacakge is in ```__main.py__``` while the main algorithm is
implemented in ```pipeline_planner.py```.

### Install dependencies
python -m pip install -r requirements.txt

### Run the tests
python -m unittest

### Run the package
python -m pipeline_planner --cpu_cores 2 --pipeline test_data/pipeline_tiny.txt 
