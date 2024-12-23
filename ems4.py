#!/usr/bin/env python3

# EMS - Energy Management System for home automation and monitoring
# Author  : Romuald Dufour
# License : GPL v3
# Release : 2024.05

# 2024.04 - Add PVOuput publishing, add logger
# 2024.05 - Add config from toml file, add more comments, some code refactoring
#           Add solar tracking for EVSE
#           Add Piko temperature control
# 2024.11 - Add Piko try/catch to resume comm lost
# 2024.12 - Rename Piko to Invr
#           update invr comm threat to connect to Huawei modbus inverter

RELEASE = '2024.12.21'

# general imports
import sys
import logging
import threading
import time
from datetime import datetime
import math

# Initiate logger
logger = logging.getLogger("EMS")
logger.setLevel(logging.INFO)

fmt = logging.Formatter(
    "%(asctime)s: %(message)s",
     datefmt="%H:%M:%S"
)

stdout = logging.StreamHandler(stream=sys.stdout)
stdout.setLevel(logging.INFO)
stdout.setFormatter(fmt)
logger.addHandler(stdout)
logger.info("Main - Release %s" % RELEASE)

# Read configurations from TOML file in current dir and set global vars
import tomllib
try:
  with open("ems.toml", "rb") as f:
    cfg = tomllib.load(f)
    logger.info('CFG  - Config file parsed')
    grid_maxcurrent = cfg['grid']['maxcurrent']
    if ((grid_maxcurrent) < 6): grid_maxcurrent = 6
    grid_loadbalancing = grid_maxcurrent
except:
  logger.error("CFG  - Config file error")
  quit()


# import libs for data manipulation
import pandas as pd
import copy
import json

# import libs for dataclasses
from dataclasses import dataclass, field

# import libs for DSMR - P1 socket
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# import libs for DSMR - DSMR parser (OBIS)
from dsmr_parser import telegram_specifications
from dsmr_parser.clients import SocketReader

# import libs for modbus (EVSE & inverter)
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.client import ModbusTcpClient

# import libs for MQTT
import paho.mqtt.client as paho

# import libs for PVO
from pvoutput import PVOutput

# define some constants
EVSE_MODE_OFF = 0
EVSE_MODE_MANUAL = 1
EVSE_MODE_SOLAR_MIN = 2
EVSE_MODE_SOLAR_MEAN = 3
EVSE_MODE_SOLAR_MAX = 4


INV_STATUS_TXT = {
    0x0000: "Standby: initializing",
    0x0001: "Standby: detecting insulation resistance",
    0x0002: "Standby: detecting irradiation",
    0x0003: "Standby: grid detecting",
    0x0100: "Starting",
    0x0200: "On-grid",
    0x0201: "Grid Connection: power limited",
    0x0202: "Grid Connection: self-derating",
    0x0203: "Off-grid mode: running",
    0x0300: "Shutdown: fault",
    0x0301: "Shutdown: command",
    0x0302: "Shutdown: OVGR",
    0x0303: "Shutdown: communication disconnected",
    0x0304: "Shutdown: power limited",
    0x0305: "Shutdown: manual startup required",
    0x0306: "Shutdown: DC switches disconnected",
    0x0307: "Shutdown: rapid cutoff",
    0x0308: "Shutdown: input underpowered",
    0x0401: "Grid scheduling: cosphi-P curve",
    0x0402: "Grid scheduling: Q-U curve",
    0x0403: "Grid scheduling: PF-U curve",
    0x0404: "Grid scheduling: dry contact",
    0x0405: "Grid scheduling: Q-P curve",
    0x0500: "Spot-check ready",
    0x0501: "Spot-checking",
    0x0600: "Inspecting",
    0x0700: "AFCI self check",
    0x0800: "I-V scanning",
    0x0900: "DC input detection",
    0x0A00: "Running: off-grid charging",
    0xA000: "Standby: no irradiation",
}


# Threats data definition
# Thread can have data class, an access lock and dataframe
evse_lock = threading.Lock()
evse_hb = 0
@dataclass
class evse_class:
        Valid: bool=False
        U: list[float] = field(default_factory=list)
        I: list[float] = field(default_factory=list)
        P_Delivered: int=0
        E_Delivered: int=0
        State: str=""
        I_Max_Applied: int=0
        I_Max_Requested: int=0
        I_Max_Activated: int=0
        I_Max_Safe: int=0
        I_Max_Validtime: int=0
        I_LB_Limit: int=grid_maxcurrent
        Phases_Requested: int=1
        Send_Timeout: int=0
        Send_I_Max: int=0
        Send_Phases: int=0
evse = evse_class()
evse_main = evse_class()
evse_df = pd.DataFrame({'U1': pd.Series(dtype='float'),
                     'U2': pd.Series(dtype='float'),
                     'U3': pd.Series(dtype='float'),
                     'I1': pd.Series(dtype='float'),
                     'I2': pd.Series(dtype='float'),
                     'I3': pd.Series(dtype='float'),
                     'P_Delivered': pd.Series(dtype='int'),
                     'E_Delivered': pd.Series(dtype='int'),
                     'I_Max': pd.Series(dtype='int'),
                     'Phases': pd.Series(dtype='int')
                    })

dsmr_lock = threading.Lock()
dsmr_hb = 0
@dataclass
class dsmr_class:
        Valid: bool=False
        Time: datetime = datetime.now()
        U: list[float] = field(default_factory=list)
        I: list[float] = field(default_factory=list)
        P_Cons: list[float] = field(default_factory=list)
        P_Inj: list[float] = field(default_factory=list)
        P_Tot_Cons: int=0
        P_Tot_Inj: int=0
        P_QH_Current: int=0
        P_AC: int=0
        P_R: int=0
        P_PF: float=1.0
        P_QH_Last: int=0
        P_QH_Max: int=0
        P_QH_MonthMax: int=0
        E_T1_Cons: int=0
        E_T2_Cons: int=0
        E_Tot_Cons: int=0
        E_T1_Inj: int=0
        E_T2_Inj: int=0
        E_Tot_Inj: int=0
dsmr = dsmr_class()
dsmr_main = dsmr_class()
dsmr_df = pd.DataFrame({'U1': pd.Series(dtype='float'),
                     'U2': pd.Series(dtype='float'),
                     'U3': pd.Series(dtype='float'),
                     'I1': pd.Series(dtype='float'),
                     'I2': pd.Series(dtype='float'),
                     'I3': pd.Series(dtype='float'),
                     'P_Cons': pd.Series(dtype='int'),
                     'P_Inj': pd.Series(dtype='int'),
                     'P': pd.Series(dtype='int'),
                     'P_R': pd.Series(dtype='int'),
                     'PF': pd.Series(dtype='float'),
                     'P_QH_Current': pd.Series(dtype='int'),
                     'P_QH_Last': pd.Series(dtype='int'),
                     'P_QH_Month': pd.Series(dtype='int'),
                     'E_Cons': pd.Series(dtype='int'),
                     'E_Inj': pd.Series(dtype='int')
                    })

invr_lock = threading.Lock()
invr_hb = 0
@dataclass
class invr_class:
        Valid: bool=False
        U: list[float] = field(default_factory=list)
        I: list[float] = field(default_factory=list)
        P_AC: int=0
        P_R: int=0
        T_AC: int=0
        E_Day: int=0
        E_Tot: int=0
        U_DC: list[float] = field(default_factory=list)
        I_DC: list[float] = field(default_factory=list)
        P_DC: int=0
        T_DC: int=0
        PwrFactor: float=1.0
        Eff:  float=0.0
        Freq: float=50.0
        Status: int=0
invr = invr_class()
invr_main = invr_class()
invr_df = pd.DataFrame({'U1': pd.Series(dtype='float'),
                     'U2': pd.Series(dtype='float'),
                     'U3': pd.Series(dtype='float'),
                     'I1': pd.Series(dtype='float'),
                     'I2': pd.Series(dtype='float'),
                     'I3': pd.Series(dtype='float'),
                     'P_Inj': pd.Series(dtype='int'),
                     'E_Inj': pd.Series(dtype='int'),
                     'E_Day': pd.Series(dtype='int'),
                     'U1_DC': pd.Series(dtype='float'),
                     'U2_DC': pd.Series(dtype='float'),
                     'I1_DC': pd.Series(dtype='float'),
                     'I2_DC': pd.Series(dtype='float'),
                     'T_DC': pd.Series(dtype='float'),
                     'T_AC': pd.Series(dtype='float'),
                     'P_DC': pd.Series(dtype='float'),
                     'P_R': pd.Series(dtype='float'),
                     'PF': pd.Series(dtype='float'),
                     'Freq': pd.Series(dtype='float'),
                     'Eff': pd.Series(dtype='float')
                    })

house_P: int = 0

mqtt_lock = threading.Lock()
mqtt_hb = 0
@dataclass
class mqtt_class:
        Valid: bool=False
        Meter_U: list[float] = field(default_factory=list)
        Meter_I: list[float] = field(default_factory=list)
        Meter_P: int=0
        Meter_P_Tot_Cons: int=0
        Meter_P_Tot_Inj: int=0
        Meter_E_T1_Cons: int=0
        Meter_E_T2_Cons: int=0
        Meter_E_Tot_Cons: int=0
        Meter_E_T1_Inj: int=0
        Meter_E_T2_Inj: int=0
        Meter_E_Tot_Inj: int=0
        Meter_P_QH_Last: int=0
        Meter_P_QH_Month: int=0
        house_P: int=0
        Inv_U: list[float] = field(default_factory=list)
        Inv_I: list[float] = field(default_factory=list)
        Inv_P: list[float] = field(default_factory=list)
        Inv_P_Inj: int=0
        Inv_E_Day: int=0
        Inv_E_Tot: int=0
        Inv_U_DC: list[float] = field(default_factory=list)
        Inv_I_DC: list[float] = field(default_factory=list)
        Inv_P_DC: int=0
        Inv_T_DC: float=0
        Inv_T_AC: float=0
        Inv_PF: float=0
        Inv_Eff: float=0
        Inv_Status: int=0
        evse_U: list[float] = field(default_factory=list)
        evse_I: list[float] = field(default_factory=list)
        evse_P_Delivered: int=0
        evse_E_Delivered: int=0
        evse_State: str=""
        evse_I_Max_Applied: int=0
        evse_I_Max_Requested: int=0
        evse_I_Max_Activated: int=0
        evse_I_Max_Safe: int=0
        evse_I_Max_Validtime: int=0
        evse_Phases_Requested: int=1
mqtt = mqtt_class()
mqtt_last_sent = mqtt_class()

ctrl_lock = threading.Lock()
ctrl_hb = 0
@dataclass
class ctrl_class:
        evse_set_I_max: int = -1
        evse_set_Phases: int = -1
        evse_loadbalancing_limit: int = -1
        evse_mode: int = EVSE_MODE_OFF
        evse_mode_changed: bool = True
        evse_solar_target: int = -1
        evse_solar_margin_min: int = -1
        evse_solar_margin_mean: int = -1
        evse_solar_margin_max: int = -1
        evse_solar_margin_updated: bool = False
ctrl = ctrl_class()

pvo_lock = threading.Lock()
pvo_hb = 0


# EVSE thread
#  Target : Alfen Pro Line EV charging station with EMS mode activated
#  - Maintain modbus connectivity to the charging station
#  - Get status, voltage, current, power, energy meter and misc. data every second
#  - Write current limit and phases count
def evse_process():
  evse_connected = False
  while (True):
    client = ModbusTcpClient(cfg['evse']['host']) # Create client object
    client.connect() # connect to device, reconnect automatically
    evse_connected = client.connected

    while (evse_connected):
      evse_connected = client.connected
      # read floats
      try:
        result  = client.read_holding_registers(306, 72, slave=1)
        if (result.function_code < 0x80):
          decoder = BinaryPayloadDecoder.fromRegisters(result.registers, Endian.BIG, wordorder=Endian.BIG)

          with evse_lock:
            # L1-N L2-N L3-N voltages
            evse.U = [round(decoder.decode_32bit_float(),1), round(decoder.decode_32bit_float(),1), round(decoder.decode_32bit_float(),1)]
            # I1 I2 I3 currents
            decoder.skip_bytes(16);
            evse.I = [round(decoder.decode_32bit_float(),2), round(decoder.decode_32bit_float(),2), round(decoder.decode_32bit_float(),2)]
            # Power
            decoder.skip_bytes(36);
            evse.P_Delivered = round(decoder.decode_32bit_float(),0)
            # Energy 
            decoder.skip_bytes(56);
            evse.E_Delivered = round(decoder.decode_64bit_float(),0)
            # State & Load balancing
            result  = client.read_holding_registers(1201, 15, slave=1)
            if (result.isError() == False):
              decoder = BinaryPayloadDecoder.fromRegisters(result.registers, Endian.BIG, wordorder=Endian.BIG)
              evse.State = str(decoder.decode_string(size=10).decode()).rstrip('\0')
              evse.I_Max_Applied = int(decoder.decode_32bit_float())
              evse.I_Max_Validtime = int(decoder.decode_32bit_uint())
              evse.I_Max_Requested = int(decoder.decode_32bit_float())
              evse.I_Max_Safe = int(decoder.decode_32bit_float())
              evse.I_Max_Activated = int(decoder.decode_16bit_uint())
              evse.Phases_Requested = int(decoder.decode_16bit_uint())
              evse.Valid = True
            else: evse_connected = False

            with ctrl_lock:
              if ((ctrl.evse_set_I_max >=0) and (ctrl.evse_set_I_max <= 32)):
                 evse.Send_I_Max = ctrl.evse_set_I_max
                 if (evse.Send_I_Max > grid_maxcurrent): evse.Send_I_Max = grid_maxcurrent
                 ctrl.evse_set_I_max = -1
                 evse.Send_Timeout = 1
              if ((ctrl.evse_set_Phases ==1) or (ctrl.evse_set_Phases == 3)):
                 evse.Send_Phases = ctrl.evse_set_Phases
                 ctrl.evse_set_Phases = -1
                 evse.Send_Timeout = 1
              if (ctrl.evse_loadbalancing_limit != evse.I_LB_Limit):
                 evse.I_LB_Limit = ctrl.evse_loadbalancing_limit
                 evse.Send_Timeout = 1

              if (ctrl.evse_mode_changed):
                 ctrl.evse_mode_changed = False
                 evse.Send_Timeout = 1
              if (ctrl.evse_solar_margin_updated):
                 ctrl.evse_solar_margin_updated = False
                 evse.Send_Timeout = 1

              if (evse.Send_Timeout <= 0):
                evse.Send_Timeout = 60
                # write IMax
                if (evse.Send_I_Max >=0) and (evse.Send_I_Max<=32):

                  amp_ratio = 230 * max(evse.Phases_Requested, 1)
                  ctrl.evse_solar_target = round(ctrl.evse_solar_margin_mean / amp_ratio)
                  if (ctrl.evse_mode == EVSE_MODE_SOLAR_MIN): ctrl.evse_solar_target = round(ctrl.evse_solar_margin_min / amp_ratio)
                  if (ctrl.evse_mode == EVSE_MODE_SOLAR_MAX): ctrl.evse_solar_target = round((ctrl.evse_solar_margin_max + (amp_ratio/3)) / amp_ratio)

                  if (evse.Phases_Requested == 3):
                    solar_I_start = cfg['evse']['solar_start_3ph']
                    solar_I_min = cfg['evse']['solar_min_3ph']
                    solar_I_max = cfg['evse']['solar_max_3ph']
                  else:
                    solar_I_start = cfg['evse']['solar_start_1ph']
                    solar_I_min = cfg['evse']['solar_min_1ph']
                    solar_I_max = cfg['evse']['solar_max_1ph']

                  if (ctrl.evse_solar_target > solar_I_max): ctrl.evse_solar_target = solar_I_max
                  if (ctrl.evse_solar_target < solar_I_start): ctrl.evse_solar_target = 0
                  if ((ctrl.evse_solar_target > 0) and (ctrl.evse_solar_target < solar_I_min)): ctrl.evse_solar_target = solar_I_min

                  if (ctrl.evse_mode == EVSE_MODE_OFF): Send_I_Max = 0
                  if (ctrl.evse_mode == EVSE_MODE_MANUAL): Send_I_Max = evse.Send_I_Max
                  if (ctrl.evse_mode >= EVSE_MODE_SOLAR_MIN): Send_I_Max = max(ctrl.evse_solar_target, evse.Send_I_Max)
                  if ((evse.I_LB_Limit >= 0) and (Send_I_Max > evse.I_LB_Limit)): Send_I_Max = evse.I_LB_Limit
                
                  logger.info("EVSE - Writing - I_Max:%i A - Mode:%i - Solar Target:%i" % (Send_I_Max, ctrl.evse_mode, ctrl.evse_solar_target))
                  builder = BinaryPayloadBuilder(byteorder=Endian.BIG, wordorder=Endian.BIG)
                  builder.add_32bit_float(1.0 * Send_I_Max)
                  payload = builder.build()
                  result  = client.write_registers(1210, payload, skip_encode=True, slave=1)

                  if (((evse.Send_Phases == 1) or (evse.Send_Phases == 3)) and (evse.Send_Phases != evse.Phases_Requested)):
                    logger.info("EVSE - Writing - Phases:%i" % evse.Send_Phases)
                    builder = BinaryPayloadBuilder(byteorder=Endian.BIG, wordorder=Endian.BIG)
                    builder.add_16bit_uint(evse.Send_Phases)
                    payload = builder.build()
                    result  = client.write_registers(1215, payload, skip_encode=True, slave=1)

            evse.Send_Timeout = evse.Send_Timeout-1

          time.sleep(1)
        else:
          logger.error("EVSE - Reconnecting")
          client.close()
          time.sleep(1)
          client.connect()
          evse_connected = client.connected

      except:
        time.sleep(1)

    client.close()
    time.sleep(5)

  time.sleep(5)


# DSMR thread
#  Target : Belgium digital smartmeter with P1 port enabled and serial to socket adapter (ESP8266)
#  - Maintain connection to ESP socket
#  - receive P1 telegram every second
#  - parse telegram and save data
def dsmr_process():
  from datetime import datetime
  from pytz import timezone

  while (True):
    QH_Last = 0
    QH_Step_Last = 0

    try:
      socket_reader = SocketReader(
            host=cfg['grid']['host'],
            port=cfg['grid']['port'],
            telegram_specification=telegram_specifications.BELGIUM_FLUVIUS
            )

      for telegram in socket_reader.read():
        #
        #print(telegram)  # see 'Telegram object' docs below
        with dsmr_lock:
          dsmr.U=[round(float(telegram.INSTANTANEOUS_VOLTAGE_L1.value),1),
                  round(float(telegram.INSTANTANEOUS_VOLTAGE_L2.value),1),
                  round(float(telegram.INSTANTANEOUS_VOLTAGE_L3.value),1)]
          dsmr.I=[round(float(telegram.INSTANTANEOUS_CURRENT_L1.value),2),
                  round(float(telegram.INSTANTANEOUS_CURRENT_L2.value),2),
                  round(float(telegram.INSTANTANEOUS_CURRENT_L3.value),2)]
          dsmr.P_Cons=[round(float(telegram.INSTANTANEOUS_ACTIVE_POWER_L1_POSITIVE.value)*1000,0),
                       round(float(telegram.INSTANTANEOUS_ACTIVE_POWER_L2_POSITIVE.value)*1000,0),
                       round(float(telegram.INSTANTANEOUS_ACTIVE_POWER_L3_POSITIVE.value)*1000,0)]
          dsmr.P_Inj=[round(float(telegram.INSTANTANEOUS_ACTIVE_POWER_L1_NEGATIVE.value)*1000,0),
                       round(float(telegram.INSTANTANEOUS_ACTIVE_POWER_L2_NEGATIVE.value)*1000,0),
                       round(float(telegram.INSTANTANEOUS_ACTIVE_POWER_L3_NEGATIVE.value)*1000,0)]
          dsmr.P_Tot_Cons=round(float(telegram.CURRENT_ELECTRICITY_USAGE.value)*1000,0)
          dsmr.P_Tot_Inj=round(float(telegram.CURRENT_ELECTRICITY_DELIVERY.value)*1000,0)
          dsmr.E_T1_Cons=round(float(telegram.ELECTRICITY_USED_TARIFF_1.value)*1000,0)
          dsmr.E_T2_Cons=round(float(telegram.ELECTRICITY_USED_TARIFF_2.value)*1000,0)
          dsmr.E_Tot_Cons=dsmr.E_T1_Cons + dsmr.E_T2_Cons
          dsmr.E_T1_Inj=round(float(telegram.ELECTRICITY_DELIVERED_TARIFF_1.value)*1000,0)
          dsmr.E_T2_Inj=round(float(telegram.ELECTRICITY_DELIVERED_TARIFF_2.value)*1000,0)
          dsmr.E_Tot_Inj=dsmr.E_T1_Inj + dsmr.E_T2_Inj
          dsmr.Time=telegram.P1_MESSAGE_TIMESTAMP.value.astimezone(timezone('Europe/Brussels'))
          dsmr.Valid = True

          dsmr.P_AC = dsmr.P_Tot_Cons - dsmr.P_Tot_Inj
          PA = dsmr.U[0]*dsmr.I[0]+dsmr.U[1]*dsmr.I[1]+dsmr.U[2]*dsmr.I[2]
          dsmr.P_R = (PA ** 2) - (dsmr.P_AC ** 2)
          if (dsmr.P_R < 0): dsmr.P_R = 0
          dsmr.P_R = round(math.sqrt(dsmr.P_R),0)
          if (PA>0): dsmr.P_PF = abs(round((dsmr.P_AC / PA),2))
          else: dsmr.P_PF=1.0

          QH_Step = 60 * (dsmr.Time.minute % 15) + dsmr.Time.second
          if (QH_Step < QH_Step_Last):
              QH_Last = QH_Current
              dsmr.P_QH_Last = QH_Last
          if (QH_Step==0 ):
              QH_Current = dsmr.P_Tot_Cons / 1000.0
          else:
              QH_Current = telegram.BELGIUM_CURRENT_AVERAGE_DEMAND.value * 1000 * 900 / QH_Step
          QH_Step_Last = QH_Step
          dsmr.P_QH_Current = QH_Current

          dsmr.P_QH_MonthMax = telegram.BELGIUM_MAXIMUM_DEMAND_MONTH.value * 1000

          try:
            for month in range(13):
              data = json.loads(telegram.BELGIUM_MAXIMUM_DEMAND_13_MONTHS[month].to_json())
              #print(data, data['value'])
              # TODO - Parse 13 month QH Max demand
          except:
            pass

    except:
      time.sleep(10)


# Inverter thread
#  Target : Modbus inverter
#  - Maintain connection to inverter
#  - Get data every second
#  - Save data
def CnvStatusTxt(Val):
    Txt = "Communication error"
    Txt = INV_STATUS_TXT[Val]
    return Txt

def invr_process():

  Dbg = False
  Host = cfg['inverter']['host']
  Port = cfg['inverter']['port']
  Addr = cfg['inverter']['id']

  while(True):
    try:
      # Setup TCP socket
      time.sleep(1)
      NetStatus=0
      try:
        client = ModbusTcpClient(host=Host, port=Port, timeout=10) # Create client object
        client.connect() # connect to device, reconnect automatically

      except socket.error as msg:
        NetStatus=msg

      time.sleep(1)

      result  = client.read_holding_registers(30000, 25, slave=1)
      decoder = BinaryPayloadDecoder.fromRegisters(result.registers, Endian.BIG, wordorder=Endian.BIG)
      InvModel = str(decoder.decode_string(size=15))
      InvSN = str(decoder.decode_string(size=10))

      result  = client.read_holding_registers(30071, 4, slave=1)
      decoder = BinaryPayloadDecoder.fromRegisters(result.registers, Endian.BIG, wordorder=Endian.BIG)
      InvStrings = round(decoder.decode_16bit_uint(),2)
      InvMPPT = round(decoder.decode_16bit_uint(),2)
      InvMaxPower = round(decoder.decode_32bit_uint(),2)/1000.0

      while(True):
        # Loop for live data
        Count = 0
        LiveError = 0
        while (LiveError < 10):

          if ((Count % 5) == 0):   # every 5 sec
            #Status = -1; ErrorCode = 0;
            #if (Status > 5): Status = -1
            #StatusTxt = CnvStatusTxt(Status)
            result  = client.read_holding_registers(32000, 11, slave=1)
            decoder = BinaryPayloadDecoder.fromRegisters(result.registers, Endian.BIG, wordorder=Endian.BIG)
            InvSignaling = round(decoder.decode_16bit_uint(),2)
            decoder.skip_bytes(2)
            InvRunStatus1 = round(decoder.decode_16bit_uint(),2)
            InvRunStatus2 = round(decoder.decode_32bit_uint(),2)
            decoder.skip_bytes(6)
            InvAlarm1 = round(decoder.decode_16bit_uint(),2)
            InvAlarm2 = round(decoder.decode_16bit_uint(),2)
            InvAlarm3 = round(decoder.decode_16bit_uint(),2)

          if ((Count % 60) == 0):   # every minute
            result = client.read_holding_registers(32106, 10, slave=1)
            decoder = BinaryPayloadDecoder.fromRegisters(result.registers, Endian.BIG, wordorder=Endian.BIG)
            TotalWh = round(decoder.decode_32bit_uint(),2)*10
            decoder.skip_bytes(12)
            TodayWh = round(decoder.decode_32bit_uint(),2)*10

            # test with meter registers
            result = client.read_holding_registers(37100, 3, slave=1)
            decoder = BinaryPayloadDecoder.fromRegisters(result.registers, Endian.BIG, wordorder=Endian.BIG)
            MeterStatus = round(decoder.decode_16bit_uint(),2)
            Meter_L1_U = round(decoder.decode_32bit_int(),2)/10.0
            #print("Meter", MeterStatus, Meter_L1_U);

          Count += 1
          LiveData = -1

          result  = client.read_holding_registers(32016, 4, slave=1)
          decoder = BinaryPayloadDecoder.fromRegisters(result.registers, Endian.BIG, wordorder=Endian.BIG)
          CC1_U = round(decoder.decode_16bit_int(),2)/10.0
          CC1_I = round(decoder.decode_16bit_int(),2)/100.0
          CC2_U = round(decoder.decode_16bit_int(),2)/10.0
          CC2_I = round(decoder.decode_16bit_int(),2)/100.0
          CC1_P = CC1_U * CC1_I
          CC2_P = CC2_U * CC2_I
          CC3_U = CC3_I = CC3_P = 0.0
          CC_P  = CC1_P + CC2_P + CC3_P

          result  = client.read_holding_registers(32064, 27, slave=1)
          decoder = BinaryPayloadDecoder.fromRegisters(result.registers, Endian.BIG, wordorder=Endian.BIG)
          CC_P  = round(decoder.decode_32bit_int(),2)
          CA1_U = round(decoder.decode_16bit_uint(),2)/10.0
          CA2_U = round(decoder.decode_16bit_uint(),2)/10.0
          CA3_U = round(decoder.decode_16bit_uint(),2)/10.0
          CA4_U = round(decoder.decode_16bit_uint(),2)/10.0
          CA5_U = round(decoder.decode_16bit_uint(),2)/10.0
          CA6_U = round(decoder.decode_16bit_uint(),2)/10.0
          CA1_I = round(decoder.decode_32bit_int(),2)/1000.0
          CA2_I = round(decoder.decode_32bit_int(),2)/1000.0
          CA3_I = round(decoder.decode_32bit_int(),2)/1000.0
          CA1_P = CA1_U * CA1_I
          CA2_P = CA2_U * CA2_I
          CA3_P = CA3_U * CA3_I

          CA_Peak = round(decoder.decode_32bit_int(),2)
          CA_P = round(decoder.decode_32bit_int(),2)
          CA_R = round(decoder.decode_32bit_int(),2)
          CA_PF = round(decoder.decode_16bit_int(),2)/1000.0
          CA_Freq = round(decoder.decode_16bit_uint(),2)/100.0
          Eff = round(decoder.decode_16bit_uint(),2)/100.0
          if (Eff > 100): Eff = 0
          CC1_T = CC2_T = CC3_T = round(decoder.decode_16bit_int(),2)/10.0
          CA1_T = CA2_T = CA3_T = CC1_T 
          decoder.skip_bytes(2)
          Status = round(decoder.decode_16bit_uint(),2)
          Fault = round(decoder.decode_16bit_uint(),2)

          LiveData = 1
          LiveError = 0

          with invr_lock:
            if (LiveData > 0):
              invr.U = [round(CA1_U, 1), round(CA2_U, 1), round(CA3_U, 1)]
              invr.I = [round(CA1_I, 2), round(CA2_I, 2), round(CA3_I, 1)]
              invr.P = [round(CA1_P, 0), round(CA2_P, 0), round(CA3_P, 0)]
              invr.P_AC = round(CA_P, 0)
              invr.P_DC = round(CC_P, 0)
              invr.P_R = round(CA_R, 0)
              if (invr.P_R < 0): invr.P_R = 0
              invr.U_DC = [round(CC1_U, 1), round(CC2_U, 1), round(CC3_U, 1)]
              invr.I_DC = [round(CC1_I, 2), round(CC2_I, 2), round(CC3_I, 1)]
              invr.P_DC = round(CC_P, 0)
              invr.PwrFactor = round(CA_PF, 1);
              invr.Eff = round(Eff, 1);
              invr.Freq = round(CA_Freq, 2);
            if (TodayWh>=0): invr.E_Day = TodayWh
            if (TotalWh>=0): invr.E_Tot = TotalWh
            invr.T_DC = max(CC1_T, CC2_T, CC3_T)
            invr.T_AC = max(CA1_T, CA2_T, CA3_T)
            invr.Status = Status
            invr.Valid = True

          time.sleep(1)

    except:
      client.close()
      time.sleep(10)

  client.close()


# MQTT thread
#  Taget : MQTT server (e.g. Mosquitto)
#  - publish all received data on regular basis (about every 15 seconds)
#  - subscribe to control topics (to control EVSE)
def mqtt_process():
  global mqtt_last_sent
  mqttc = paho.Client(paho.CallbackAPIVersion.VERSION2, cfg['mqtt']['name'])

  MQTTTopic=cfg['mqtt']['topic']+"/"

  def mqtt_on_connect(client, obj, flags, rc, prop):
    logger.info("MQTT - Connected")
    mqttc.subscribe("ems/evse/ctrl/#")

  def mqtt_on_message(client, userdata, message):
    topic = message.topic  #.split("evse/", 1)[1]
    data = message.payload.decode("utf-8")
    if (topic == "ems/evse/ctrl/maxcurrent"):
       val = int(float(data)) 
       if (val >= 0) and (val <= 32):
          with ctrl_lock:
            ctrl.evse_set_I_max = val 
            logger.info("MQTT - Received EVSE MaxCurrent %d", ctrl.evse_set_I_max)
    if (topic == "ems/evse/ctrl/phases"):
       val = int(float(data)) 
       if (val == 1) or (val == 3):
          with ctrl_lock:
            ctrl.evse_set_Phases = val 
            logger.info("MQTT - Received EVSE Phases %d", ctrl.evse_set_Phases)
    if (topic == "ems/evse/ctrl/mode"):
       val = data 
       if (val == "OFF") or (val == "MANUAL") or (val == "SOLAR MIN") or (val == "SOLAR MEAN") or (val == "SOLAR MAX"):
          with ctrl_lock:
            old_mode = ctrl.evse_mode
            if (val == "OFF"): ctrl.evse_mode = EVSE_MODE_OFF
            if (val == "MANUAL"): ctrl.evse_mode = EVSE_MODE_MANUAL
            if (val == "SOLAR MIN"): ctrl.evse_mode = EVSE_MODE_SOLAR_MIN
            if (val == "SOLAR MEAN"): ctrl.evse_mode = EVSE_MODE_SOLAR_MEAN
            if (val == "SOLAR MAX"): ctrl.evse_mode = EVSE_MODE_SOLAR_MAX
            if ((old_mode <= EVSE_MODE_MANUAL) and (ctrl.evse_mode >= EVSE_MODE_SOLAR_MIN)): ctrl.evse_solar_target = evse.Send_I_Max
            ctrl.evse_mode_changed = True
            logger.info("MQTT - Received EVSE charging mode %s", val)

  mqtt_connected = False
  while (True):

    if (mqtt_connected == False):
      try:
        logger.error("MQTT - Connecting")
        mqttc.on_message=mqtt_on_message
        mqttc.on_connect=mqtt_on_connect
        mqttc.username_pw_set(cfg['mqtt']['user'], cfg['mqtt']['password'])
        mqttc.connect(cfg['mqtt']['host'], cfg['mqtt']['port'], 5)
        mqttc.loop_start()
        mqtt_connected = True
        time.sleep(1)
      except Exception as error:
        print("MQTT - An exception occurred:", type(error).__name__, "–", error)
        logger.error("MQTT - Exception - broker connection error")
        mqtt_connected = False
        pass

    if (mqtt_connected and (mqttc.is_connected() == False)):
       logger.error("MQTT - Disconnected")
       mqtt_connected = False
    
    if (mqtt_connected and mqttc.is_connected()):
        with mqtt_lock:
          if (mqtt.Valid):
            force = ((datetime.now().second == 0) and (datetime.now().minute % 15 == 0))
            logger.info("MQTT - publishing")
            try:
              if (mqtt.Meter_U != mqtt_last_sent.Meter_U) or force:
                mqttc.publish(MQTTTopic+'smartmeter/U1', '%0.1f'%mqtt.Meter_U[0])
                mqttc.publish(MQTTTopic+'smartmeter/U2', '%0.1f'%mqtt.Meter_U[1])
                mqttc.publish(MQTTTopic+'smartmeter/U3', '%0.1f'%mqtt.Meter_U[2])
              if (mqtt.Meter_I != mqtt_last_sent.Meter_I) or force:
                mqttc.publish(MQTTTopic+'smartmeter/I1', '%0.2f'%mqtt.Meter_I[0])
                mqttc.publish(MQTTTopic+'smartmeter/I2', '%0.2f'%mqtt.Meter_I[1])
                mqttc.publish(MQTTTopic+'smartmeter/I3', '%0.2f'%mqtt.Meter_I[2])
              if ((mqtt.Meter_P_Tot_Cons != mqtt_last_sent.Meter_P_Tot_Cons) or (mqtt.Meter_P_Tot_Inj != mqtt_last_sent.Meter_P_Tot_Inj)) or force:
                mqttc.publish(MQTTTopic+'smartmeter/P', '%d'%mqtt.Meter_P)
                mqttc.publish(MQTTTopic+'smartmeter/P_consumed', '%d'%mqtt.Meter_P_Tot_Cons)
                mqttc.publish(MQTTTopic+'smartmeter/P_injected', '%d'%mqtt.Meter_P_Tot_Inj)
              if (mqtt.Meter_I != mqtt_last_sent.Meter_I) or force:
                mqttc.publish(MQTTTopic+'smartmeter/E_consumed', '%d'%mqtt.Meter_E_Tot_Cons, retain=True)
              if (mqtt.Meter_E_Tot_Inj != mqtt_last_sent.Meter_E_Tot_Inj) or force:
                mqttc.publish(MQTTTopic+'smartmeter/E_injected', '%d'%mqtt.Meter_E_Tot_Inj, retain=True)
              if (mqtt.Meter_P_QH_Month != mqtt_last_sent.Meter_P_QH_Month) or force:
                mqttc.publish(MQTTTopic+'smartmeter/P_QH_Month', '%d'%mqtt.Meter_P_QH_Month, retain=True)
                mqttc.publish(MQTTTopic+'smartmeter/P_QH_Last', '%d'%mqtt.Meter_P_QH_Last, retain=True)
              elif (mqtt.Meter_P_QH_Last != mqtt_last_sent.Meter_P_QH_Last) or force:
                mqttc.publish(MQTTTopic+'smartmeter/P_QH_Last', '%d'%mqtt.Meter_P_QH_Last, retain=True)

              if (mqtt.house_P != mqtt_last_sent.house_P) or force:
                mqttc.publish(MQTTTopic+'house/P', '%i'%mqtt.house_P)

              if (mqtt.Inv_U != mqtt_last_sent.Inv_U) or force:
                mqttc.publish(MQTTTopic+'inverter/U1', '%0.1f'%mqtt.Inv_U[0])
                mqttc.publish(MQTTTopic+'inverter/U2', '%0.1f'%mqtt.Inv_U[1])
                mqttc.publish(MQTTTopic+'inverter/U3', '%0.1f'%mqtt.Inv_U[2])
              if (mqtt.Inv_I != mqtt_last_sent.Inv_I) or force:
                mqttc.publish(MQTTTopic+'inverter/I1', '%0.2f'%mqtt.Inv_I[0])
                mqttc.publish(MQTTTopic+'inverter/I2', '%0.2f'%mqtt.Inv_I[1])
                mqttc.publish(MQTTTopic+'inverter/I3', '%0.2f'%mqtt.Inv_I[2])
              if (mqtt.Inv_P_Inj != mqtt_last_sent.Inv_P_Inj) or force:
                mqttc.publish(MQTTTopic+'inverter/Power', '%d'%mqtt.Inv_P_Inj)
              if ((mqtt.Inv_E_Day != mqtt_last_sent.Inv_E_Day) or (force and mqtt.Inv_E_Day != 0)):
                mqttc.publish(MQTTTopic+'inverter/E_Today', '%d'%mqtt.Inv_E_Day, retain=True)
              if ((mqtt.Inv_E_Tot != mqtt_last_sent.Inv_E_Tot) or force):
                if (mqtt.Inv_E_Tot!=0):
                  mqttc.publish(MQTTTopic+'inverter/E_injected', '%d'%mqtt.Inv_E_Tot, retain=True)
              if (mqtt.Inv_T_DC != mqtt_last_sent.Inv_T_DC) or force:
                mqttc.publish(MQTTTopic+'inverter/Temp_DC', '%0.1f'%mqtt.Inv_T_DC)
              if (mqtt.Inv_T_AC != mqtt_last_sent.Inv_T_AC) or force:
                mqttc.publish(MQTTTopic+'inverter/Temp_AC', '%0.1f'%mqtt.Inv_T_AC)
              if (mqtt.Inv_Eff != mqtt_last_sent.Inv_Eff) or force:
                mqttc.publish(MQTTTopic+'inverter/Eff', '%0.1f'%mqtt.Inv_Eff)
              if (mqtt.Inv_PF != mqtt_last_sent.Inv_PF) or force:
                mqttc.publish(MQTTTopic+'inverter/PF', '%0.1f'%mqtt.Inv_PF)
              
              if (mqtt.Inv_Status != mqtt_last_sent.Inv_Status) or force:
                mqttc.publish(MQTTTopic+'inverter/Status', CnvStatusTxt(mqtt.Inv_Status), retain=True)

              if (mqtt.evse_U != mqtt_last_sent.evse_U) or force:
                mqttc.publish(MQTTTopic+'evse/U1', '%0.1f'%mqtt.evse_U[0])
                mqttc.publish(MQTTTopic+'evse/U2', '%0.1f'%mqtt.evse_U[1])
                mqttc.publish(MQTTTopic+'evse/U3', '%0.1f'%mqtt.evse_U[2])
              if (mqtt.evse_I != mqtt_last_sent.evse_I) or force:
                mqttc.publish(MQTTTopic+'evse/I1', '%0.2f'%mqtt.evse_I[0])
                mqttc.publish(MQTTTopic+'evse/I2', '%0.2f'%mqtt.evse_I[1])
                mqttc.publish(MQTTTopic+'evse/I3', '%0.2f'%mqtt.evse_I[2])
              if (mqtt.evse_P_Delivered != mqtt_last_sent.evse_P_Delivered) or force:
                mqttc.publish(MQTTTopic+'evse/P', '%i'%mqtt.evse_P_Delivered)
              if (mqtt.evse_E_Delivered != mqtt_last_sent.evse_E_Delivered) or force:
                mqttc.publish(MQTTTopic+'evse/E_Delivered', '%i'%mqtt.evse_E_Delivered, retain=True)
              if (mqtt.evse_State != mqtt_last_sent.evse_State) or force:
                mqttc.publish(MQTTTopic+'evse/State', '%s'%mqtt.evse_State, retain=True)
              if (mqtt.evse_I_Max_Activated != mqtt_last_sent.evse_I_Max_Activated) or force:
                mqttc.publish(MQTTTopic+'evse/I_Max_Activated', '%i'%mqtt.evse_I_Max_Activated, retain=True)
              if (mqtt.evse_I_Max_Applied != mqtt_last_sent.evse_I_Max_Applied) or force:
                mqttc.publish(MQTTTopic+'evse/I_Max_Applied', '%i'%mqtt.evse_I_Max_Applied)
              if (mqtt.evse_I_Max_Safe != mqtt_last_sent.evse_I_Max_Safe) or force:
                mqttc.publish(MQTTTopic+'evse/I_Max_Safe', '%i'%mqtt.evse_I_Max_Safe, retain=True)
              if (mqtt.evse_I_Max_Requested != mqtt_last_sent.evse_I_Max_Requested) or force:
                mqttc.publish(MQTTTopic+'evse/I_Max_Requested', '%i'%mqtt.evse_I_Max_Requested, retain=True)
              if (mqtt.evse_I_Max_Validtime != mqtt_last_sent.evse_I_Max_Validtime) or force:
                mqttc.publish(MQTTTopic+'evse/I_Max_Timeout', '%i'%mqtt.evse_I_Max_Validtime)
              if (mqtt.evse_Phases_Requested != mqtt_last_sent.evse_Phases_Requested) or force:
                mqttc.publish(MQTTTopic+'evse/Phases_Requested', '%i'%mqtt.evse_Phases_Requested, retain=True)

              mqtt.Valid = False
              mqtt_last_sent = copy.deepcopy(mqtt)
            except Exception as error:
              print("MQTT - An exception occurred:", type(error).__name__, "–", error)
              logger.error("MQTT - Exception - Failed to send data to broker")
              mqtt_connected = False
              pass
        
        time.sleep(0.3)

  mqtt_connected = False
  logger.error("MQTT - broker connection error")
  time.sleep(1)

# PVO threat
#  Target : PVOutput portal
#  - publish inverter data to PVO every 5 minutes
def pvo_process():
   
  while(True):
    try:
      pvodate = datetime.now()
      
      if (((pvodate.minute %5) == 0) and (pvodate.second == 0)):

        if ((mqtt.Inv_U_DC[0] !=0) or (mqtt.Inv_U_DC[1] != 0) or (mqtt.Inv_P_Inj != 0)):

          pvo = PVOutput(apikey=cfg['pvo']['apikey'], systemid=cfg['pvo']['systemid'], donation_made=True)

          with mqtt_lock:
            data = {
              "d": pvodate.strftime("%Y%m%d"),
              "t": pvodate.strftime("%H:%M"),
              "v1": mqtt.Inv_E_Day,  # Daily Energy
              "v2": mqtt.Inv_P_Inj,   # power generation
              "v6": (mqtt.Inv_U_DC[0] + mqtt.Inv_U_DC[1]) / 2
            }
            #"v4": 450,  # power consumption
            #  "v5": 23.5,  # temperature
            #  "m1": "Testing",  # custom message
            #}
            pvo_status = pvo.addstatus(data).text
            logger.info("PVO  - Data sent to PVOutput")

    except Exception as error:
      print("PVO  - An exception occurred:", type(error).__name__, "–", error)
      logger.error("PVO  - Failed to send data to PVOutput")
      pass

    time.sleep(1)


# Main
if __name__ == "__main__":

  # Starting all threads
  logger.info("Main - starting threads")

  if (cfg['grid']['enable']):
    dsmr_threat = threading.Thread(target=dsmr_process, args=(), daemon=True)
    dsmr_threat.start()
    logger.info("DSMR - PID:%i" % dsmr_threat.native_id)

  if (cfg['evse']['enable']):
    evse_threat = threading.Thread(target=evse_process, args=(), daemon=True)
    evse_threat.start()
    logger.info("EVSE - PID:%i" % evse_threat.native_id)

  if (cfg['inverter']['enable']):
    invr_threat = threading.Thread(target=invr_process, args=(), daemon=True)
    invr_threat.start()
    logger.info("INVR - PID:%i" % invr_threat.native_id)

  if (cfg['mqtt']['enable']):
    mqtt_threat = threading.Thread(target=mqtt_process, args=(), daemon=True)
    mqtt_threat.start()
    logger.info("MQTT - PID:%i" % mqtt_threat.native_id)

  if (cfg['pvo']['enable']):
    pvo_threat = threading.Thread(target=pvo_process, args=(), daemon=True)
    pvo_threat.start()
    logger.info("PVO  - PID:%i" % pvo_threat.native_id)

  # Init data
  index: int = 0
  dsmr_index: int = 0
  evse_index: int = 0
  invr_index: int = 0
  
  lb_index: int = 0

  # Starting main loop
  logger.info("Main - Main loop")
  while (True):
    index = index + 1

    # get threat data, push to dataframe to keep last 60 seconds of data
    with dsmr_lock:
      if (dsmr.Valid == True):
          new_row = {'U1': dsmr.U[0], 'U2': dsmr.U[1], 'U3': dsmr.U[2],
                     'I1': dsmr.I[0] * (1 if (dsmr.P_Cons[0]>0) else -1), 'I2': dsmr.I[1] * (1 if (dsmr.P_Cons[1]>0) else -1), 'I3': dsmr.I[2] * (1 if (dsmr.P_Cons[2]>0) else -1),
                     'P_Cons': int(dsmr.P_Tot_Cons), 'P_Inj': int(dsmr.P_Tot_Inj), 'P': int(dsmr.P_Tot_Cons - dsmr.P_Tot_Inj),
                     'P_R': int(dsmr.P_R), 'PF': float(dsmr.P_PF),
                     'P_QH_Current': int(dsmr.P_QH_Current*1000), 'P_QH_Last': int(dsmr.P_QH_Last*1000), 'P_QH_Month': int(dsmr.P_QH_MonthMax*1000),
                     'E_Cons': int(dsmr.E_Tot_Cons), 'E_Inj': int(dsmr.E_Tot_Inj)}
          dsmr_df.loc[dsmr_index] = new_row
          dsmr_index = dsmr_index + 1
          if (len(dsmr_df) > 60): 
             dsmr_index = dsmr_df.index[-1]+1
             dsmr_df.drop(index=dsmr_df.index[0], axis=0, inplace=True)
          dsmr_main = copy.deepcopy(dsmr)
          dsmr_hb = 0
          dsmr.Valid = False

    with evse_lock:
      if (evse.Valid == True):
          new_row = {'U1': evse.U[0], 'U2': evse.U[1], 'U3': evse.U[2],
                     'I1': evse.I[0], 'I2': evse.I[1], 'I3': evse.I[2],
                     'P_Delivered': int(evse.P_Delivered), 'E_Delivered': int(evse.E_Delivered),
                     'I_Max': int(evse.I_Max_Activated), 'Phases': int(evse.Phases_Requested)}
          evse_df.loc[evse_index] = new_row
          evse_index = evse_index + 1
          if (len(evse_df) > 60): 
             evse_index = evse_df.index[-1]+1
             evse_df.drop(index=evse_df.index[0], axis=0, inplace=True)
          evse_main = copy.deepcopy(evse)
          evse_hb = 0
          evse.Valid = False

    with invr_lock:
      if (invr.Valid == True):
          new_row = {'U1': invr.U[0], 'U2': invr.U[1], 'U3': invr.U[2],
                     'I1': invr.I[0], 'I2': invr.I[1], 'I3': invr.I[2],
                     'P_Inj': int(invr.P_AC),
                     'E_Inj': int(invr.E_Tot), 'E_Day': int(invr.E_Day),
                     'U1_DC': invr.U_DC[0], 'U2_DC': invr.U_DC[1],
                     'I1_DC': invr.I_DC[0], 'I2_DC': invr.I_DC[1],
                     'T_DC': invr.T_DC, 'T_AC': invr.T_AC,
                     'P_DC': int(invr.P_DC),
                     'P_R': invr.P_R, 'PF': invr.PwrFactor, 'Freq': invr.Freq,
                     'Eff': invr.Eff}
          invr_df.loc[invr_index] = new_row
          invr_index = invr_index + 1
          if (len(invr_df) > 60): 
             invr_index = invr_df.index[-1]+1
             invr_df.drop(index=invr_df.index[0], axis=0, inplace=True)
          invr_main = copy.deepcopy(invr)
          invr_hb = 0
          invr.Valid = False

    # check heartbeat from all threads
    sec = datetime.now().second

    if ((dsmr_hb > 60) and (cfg['grid']['enable'])):
      if ((sec % 15)==0): logger.warning("DSMR - Communication lost")
    dsmr_hb = dsmr_hb + 1

    if ((evse_hb > 60) and (cfg['evse']['enable'])):
      if ((sec % 15)==0):
        logger.warning("EVSE - Communication lost")
        #evse_threat.kill()
        #evse_threat.start()
    evse_hb = evse_hb + 1

    if ((invr_hb > 60) and (cfg['inverter']['enable'])):
      if ((sec % 15)==0): logger.warning("INVR - Communication lost")
    invr_hb = invr_hb + 1

    # Calculate mobile average for 5, 15 and 60 seconds of data
    if ((sec % 15)==0):
      dsmr_mean_15s = dsmr_df.iloc[-15:].mean(numeric_only=True).fillna(0)
      evse_mean_15s = evse_df.iloc[-15:].mean(numeric_only=True).fillna(0)
      invr_mean_15s = invr_df.iloc[-15:].mean(numeric_only=True).fillna(0)

    if ((sec % 5)==0):
      dsmr_mean_5s = dsmr_df.iloc[-5:].mean(numeric_only=True).fillna(0)
      evse_mean_5s = evse_df.iloc[-5:].mean(numeric_only=True).fillna(0)
      invr_mean_5s = invr_df.iloc[-5:].mean(numeric_only=True).fillna(0)
      if (index > 15):
         house_P = dsmr_mean_15s.P - evse_mean_15s.P_Delivered + invr_mean_15s.P_Inj
         if (house_P < 0): house_P = 0

    if ((sec % 60)==0):
      dsmr_mean_60s = dsmr_df.iloc[-60:].mean(numeric_only=True).fillna(0)
      evse_mean_60s = evse_df.iloc[-60:].mean(numeric_only=True).fillna(0)
      invr_mean_60s = invr_df.iloc[-60:].mean(numeric_only=True).fillna(0)
      invr_min_60s = invr_df.iloc[-60:].min(numeric_only=True).fillna(0)
      invr_max_60s = invr_df.iloc[-60:].max(numeric_only=True).fillna(0)


    # Every 5 seconds (after at least 15 records in the dataframes)
    if ((index >= 15) and ((sec % 5)==0)):
      # - print data on the logger
      logger.info("DSMR - P_Cons:%5i P_Inj:%5i E_Cons:%10i E_Inj:%10i U:(%5.1f %5.1f %5.1f) I:(%5.2f %5.2f %5.2f) QH:(%5i %5i %5i) P_R:%5i PF:%4.2f" %
                  (dsmr_mean_5s.P_Cons, dsmr_mean_5s.P_Inj, dsmr_main.E_Tot_Cons, dsmr_main.E_Tot_Inj,
                   dsmr_mean_5s.U1, dsmr_mean_5s.U2, dsmr_mean_5s.U3, dsmr_mean_5s.I1, dsmr_mean_5s.I2, dsmr_mean_5s.I3,
                   dsmr_main.P_QH_Current, dsmr_main.P_QH_Last, dsmr_main.P_QH_MonthMax, dsmr_main.P_R, dsmr_main.P_PF))
      logger.info("EVSE - P_Cons:%5i E_Cons:%10i I_Max:%3i Phases:%1i State:%s" %
                  (evse_mean_5s.P_Delivered, evse_main.E_Delivered, evse_main.I_Max_Applied, evse_main.Phases_Requested, evse_main.State))
      logger.info("INVR - Pwr:%5i E_Day:%5i E_Tot:%10i Temp:(DC:%.0f AC:%.0f) Pwr DC: %5i Pwr R: %5i PF: %4.2f Eff:%4.1f Status:%s" %
                   (invr_mean_5s.P_Inj, invr_main.E_Day, invr_main.E_Tot, invr_mean_5s.T_DC, invr_mean_5s.T_AC,
                    invr_mean_5s.P_DC, invr_mean_5s.P_R, invr_mean_5s.PF, invr_mean_5s.Eff, CnvStatusTxt(invr_main.Status)))
      logger.info("HOME - Pwr:%5i" % house_P)

      # - do loadbalancing algo to limit evse current if needed
      #   TODO:Improove in 1Ph mode
      lb_index = lb_index + 1
      grid_maxphasecurrent = max([dsmr_mean_5s.I1, dsmr_mean_5s.I2, dsmr_mean_5s.I3])
      evse_current = max([evse_mean_5s.I1, evse_mean_5s.I2, evse_mean_5s.I3])
      with ctrl_lock:
        if (grid_maxphasecurrent > grid_maxcurrent):
          grid_loadbalancing = evse_current - (grid_maxphasecurrent - grid_maxcurrent) - 1.5
          if (grid_loadbalancing < 6): grid_loadbalancing = 0
          ctrl.evse_loadbalancing_limit = grid_loadbalancing
          lb_index = 0
          logger.info("LB   - High grid current - Derating")
        else:
          if (grid_loadbalancing < grid_maxcurrent):
            if ((grid_loadbalancing >= 6) and (lb_index >= 3)):
              if (grid_maxphasecurrent < grid_maxcurrent - 2.5):
                grid_loadbalancing = grid_loadbalancing + 1
                if (grid_maxphasecurrent < grid_maxcurrent - 4):
                  grid_loadbalancing = grid_loadbalancing + 1
                if (grid_maxphasecurrent < grid_maxcurrent - 8):
                  grid_loadbalancing = grid_loadbalancing + 3
                ctrl.evse_loadbalancing_limit = grid_loadbalancing
                lb_index = 0
                logger.info("LB   - Restoring")
            if ((grid_loadbalancing < 6) and (lb_index >= 4)):
              if (grid_maxphasecurrent < grid_maxcurrent - 8):
                grid_loadbalancing = 6
                ctrl.evse_loadbalancing_limit = grid_loadbalancing
                lb_index = -3
                logger.info("LB   - Restoring")

      # - Do Solar power algo
      #if (((sec % 15)==0) and (index >= 60)):
      if (((sec % 60)==0) and (index >= 60)):
        mean_solar = invr_mean_60s.P_Inj
        max_solar = invr_max_60s.P_Inj
        min_solar = invr_min_60s.P_Inj
        mean_evse = evse_mean_60s.P_Delivered
        last_evse = evse_mean_15s.P_Delivered
        mean_grid = dsmr_mean_60s.P
        mean_house = mean_grid + mean_solar - mean_evse
        margin_mean = mean_solar - mean_house
        margin_max = (mean_solar - mean_house) + (max_solar - mean_solar)*0.9
        margin_min = mean_solar - mean_house + (min_solar - mean_solar)*0.9
        logger.info("SUN  - Margin Min:%.0f Mean:%.0f Max:%.0f" % (margin_min, margin_mean, margin_max))
        ctrl.evse_solar_margin_min = margin_min
        ctrl.evse_solar_margin_mean = margin_mean
        ctrl.evse_solar_margin_max = margin_max
        ctrl.evse_solar_margin_updated = True

      # - send to MQTT for publishing
      with (mqtt_lock):
        mqtt.Meter_U = [dsmr_mean_5s.U1, dsmr_mean_5s.U2, dsmr_mean_5s.U3] 
        mqtt.Meter_I = [dsmr_mean_5s.I1, dsmr_mean_5s.I2, dsmr_mean_5s.I3] 
        mqtt.Meter_P = dsmr_mean_5s.P
        mqtt.Meter_P_Tot_Cons = dsmr_mean_5s.P_Cons
        mqtt.Meter_P_Tot_Inj = dsmr_mean_5s.P_Inj
        mqtt.Meter_E_Tot_Cons = dsmr_main.E_Tot_Cons
        mqtt.Meter_E_Tot_Inj = dsmr_main.E_Tot_Inj
        mqtt.Meter_P_QH_Last = dsmr_main.P_QH_Last
        mqtt.Meter_P_QH_Month = dsmr_main.P_QH_MonthMax

        mqtt.house_P = house_P

        mqtt.Inv_U = [invr_mean_5s.U1, invr_mean_5s.U2, invr_mean_5s.U3]
        mqtt.Inv_I = [invr_mean_5s.I1, invr_mean_5s.I2, invr_mean_5s.I3]
        mqtt.Inv_P_Inj = invr_mean_5s.P_Inj
        mqtt.Inv_E_Day = invr_main.E_Day
        mqtt.Inv_E_Tot = invr_main.E_Tot
        mqtt.Inv_U_DC = [invr_mean_5s.U1_DC, invr_mean_5s.U2_DC]
        mqtt.Inv_I_DC = [invr_mean_5s.I1_DC, invr_mean_5s.I2_DC]
        mqtt.Inv_P_DC = invr_mean_5s.P_DC
        mqtt.Inv_T_DC = invr_mean_5s.T_DC
        mqtt.Inv_T_AC = invr_mean_5s.T_AC
        mqtt.Inv_PF = invr_mean_5s.PF
        mqtt.Inv_Eff = invr_mean_5s.Eff
        mqtt.Inv_Status = invr_main.Status

        mqtt.evse_U = [evse_mean_5s.U1, evse_mean_5s.U2, evse_mean_5s.U3]
        mqtt.evse_I = [evse_mean_5s.I1, evse_mean_5s.I2, evse_mean_5s.I3]
        mqtt.evse_P_Delivered = evse_mean_5s.P_Delivered
        mqtt.evse_E_Delivered = evse_main.E_Delivered
        mqtt.evse_State = evse_main.State
        mqtt.evse_I_Max_Applied = evse_main.I_Max_Applied
        mqtt.evse_I_Max_Requested = evse_main.I_Max_Requested
        mqtt.evse_I_Max_Activated = evse_main.I_Max_Activated
        mqtt.evse_I_Max_Safe = evse_main.I_Max_Safe
        mqtt.evse_I_Max_Validtime = evse_main.I_Max_Validtime
        mqtt.evse_Phases_Requested = evse_main.Phases_Requested
        
        mqtt.Valid = True

    time.sleep(1)
# x.join()
  logger.info("Main - all done")
