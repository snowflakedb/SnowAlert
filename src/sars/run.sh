#!/bin/bash
./snowalert/src/sars/run.py | ../sars/temp.R | ../sars/writeBack.py
