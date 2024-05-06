# ems

This is a custom Energy Management System written in Python 3

Multiple thread are used :
* DSMR : the goal of this thread is to collect the grid data from a DSMR digital smartmeter using P1 protocol over Telnet.  An ESP8266 device is used on the meter to read serial P1 port and make it availlable through Telnet over TCP/IP network.  The msartmeter provide voltage, current, power for each phases (3N400), energy consumption and injection, as well as quarter hour consumption.
* EVSE : this thread collect data from an Alfen Pro Line EV charging station using ModBus protocol (the charging station has to be configured in EMS mode through Alfen software).  Voltage, Current, power and Energy meter are available (see Alfen Modbus documentation).  The Maximum current provided to the car can be selected as well as 1-Phase or 3-Phases charging mode.
* Inverter : this thread collect data from Kostal Solar Piko inverter : voltage, current, power, energy, temperature, ... on DC and AC side.
* PVOutput : this thread publish data to PVOutput portal through their API every 5 minutes
* MQTT : this thread communicate all data to MQTT server to be use e.g. in home automation.  EVSE command are also accepted through this channel.
* Main thread : general data management

* Future thread : Home battery management

  
