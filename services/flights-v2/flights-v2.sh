#!/bin/bash

if [ "$1" = "setup" ]; then
    python3 -m venv env
    env/bin/python3 -m pip install --upgrade pip
    env/bin/pip install -r requirements.txt
elif [ "$1" = "start" ]; then
    env/bin/python run.py
elif [ "$1" = "test" ]; then
    env/bin/coverage run -m unittest discover
    env/bin/coverage report -m app/*.py app/errors/*.py app/routes/*.py app/services/*.py
fi
