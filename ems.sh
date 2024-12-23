#!/bin/sh
cd /opt/ems
/opt/ems/venv/bin/python3 /opt/ems/ems4.py | tee -a /opt/ems/log.txt
