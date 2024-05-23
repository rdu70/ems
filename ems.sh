#!/bin/sh
cd /opt/ems
/opt/ems/venv/bin/python3 /opt/ems/ems3.py | tee -a /opt/ems/log.txt
