#!/usr/bin/bash
./snowalert/src/sars/run.py | ./snowalert/src/sars/temp.R | ./snowalert/src/sars/writeBack.py
