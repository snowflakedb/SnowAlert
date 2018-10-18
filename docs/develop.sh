#!/bin/bash

VENV_DIR=".venv"

if [[ ! -d $VENV_DIR ]]; then
	python3 -m venv $VENV_DIR
fi

source $VENV_DIR/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# default doctress value makes autobuild spin in a loop & waste CPU
sphinx-autobuild src build -d doctrees
