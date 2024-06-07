#!/bin/bash
export PYTHONPATH=$(pwd)
python3 -m unittest discover -s . -p "test_*.py"
