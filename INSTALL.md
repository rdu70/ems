# Create a new Python virtual environment and install needed libs

```console
# mkdir /opt/ems
# python3 -m venv /opt/ems/venv
# cd /opt/ems/venv/bin 
# ./pip3 install pvoutput
# ./pip3 install pandas
# ./pip3 install pymodbus
# ./pip3 install dsmr_parser
# ./pip3 install paho-mqtt
# ./pip3 install pyarrow
```

# Update config file

Config file use toml format (https://toml.io/en/).  It has to be in the same directory as ems.py file.  Be careful as there is currently very little check on configuration validity.

```console
# cp ems.toml.sample ems.toml
# nano ems.toml
- edit config to enable/disable modules
- set modules hosts IP
- set credentiel where needed
- update optional parameters
```

# Run EMS

```console
# cd /opt/ems
# /opt/ems/venv/bin/python3 /opt/ems/ems3.py
```

or use tee in shell script to save logs to file (see ems.sh):

```console
# /opt/ems/venv/bin/python3 /opt/ems/ems3.py | tee -a /opt/ems/log.txt
```