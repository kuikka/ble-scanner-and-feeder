#!/usr/bin/env python3

import serial
import io
import binascii
import struct
import time
from influxdb import InfluxDBClient

INFLUX_SERVER = ('localhost', 8086)

RUUVI_MFG_ID = 1177
MIJIA_SVC_ID = 0xfe95
TEMPERATURE_LABEL = 'temperature'
HUMIDITY_LABEL = 'humidity'

TX_INTERVAL = 30

SENSOR_MAP = [ 
        {
            # Ruuvi #1
            'last_sent': 0.0,
            'accrued_data' : {},
            'address': 'E6:99:94:26:C5:C3',
            'label': '{}',
            'tags': {
                'room': 'office',
                'location': 'window'
                }
        },
        {
            # Ruuvi #2
            'last_sent': 0.0,
            'accrued_data' : {},
            'address': 'C1:2F:56:FB:6A:0A',
            'label': '{}',
            'tags': {
                'room': 'bedroom',
                'location': 'window'
                }
        },
        {
            # Ruuvi #3
            'last_sent': 0.0,
            'accrued_data' : {},
            'address': 'F6:8C:F2:8D:6E:A3',
            'label': '{}',
            'tags': {
                'room': 'living',
                'location': 'tv'
                }
        },
        {
            # Mijia #1
            'last_sent': 0.0,
            'accrued_data' : {},
            'address': '4C:65:A8:D4:03:E3',
            'label': '{}',
            'tags': {
                'room': 'hallway',
                'location': 'kitchen'
                }
        },
    ]

def parse_ruuvi_mfg_data(data):
    """
    Parse Ruuvi advertising formats
    see https://github.com/ruuvi/ruuvi-sensor-protocols/blob/master/broadcast_formats.md 
    and https://github.com/ruuvi/ruuvi-sensor-protocols/blob/master/dataformat_05.md
    for details

    >>> parse_ruuvi_mfg_data(b'\\x05\\x0f\\xfa6\\xc9\\xc6\\xda\\xff\\xe8\\x00\\x18\\x04\\x00\\xb5\\xf6-\\x0f-\\xe6\\x99\\x94&\\xc5\\xc3')
    {'temperature': 20.45, 'humidity': 35.0625, 'atmospheric_pressure': 1009.06, 'battery_voltage': 3.055, 'tx_power': 4}
    """

    if len(data) < 1:
        return

    fmt = data[0]
    if fmt == 5 and len(data) >= 15:
        temperature, humidity, atm_pressure = struct.unpack_from(">hHH", data, offset=1)
        power_info, = struct.unpack_from(">H", data, offset=13)

        battery_voltage_mv = power_info >> 5
        tx_power = power_info & 0x1F
        return {
                # degrees celsius
                TEMPERATURE_LABEL: temperature * 0.005,
                # Percent
                HUMIDITY_LABEL: humidity * 0.0025,
                # hPa
                'atmospheric_pressure': float(atm_pressure + 50000) / 100,
                # Volts
                'battery_voltage': float(battery_voltage_mv + 1600) / 1000,
                # dBm
                'tx_power': -40 + 2 * tx_power
                }

    return {}

def parse_mijia_sensor_data(data):
    """
    Parse Xiaomi Mijia Bluetooth temperature and humidity sensor data
    Based on info from https://github.com/mspider65/Xiaomi-Mijia-Bluetooth-Temperature-and-Humidity-Sensor

    >>> parse_mijia_sensor_data(b'P \\xaa\\x01\\x15\\xe3\\x03\\xd4\\xa8eL\\r\\x10\\x04\\xc9\\x00\\xc5\\x01')
    {'temperature': 20.1, 'humidity': 45.3}
    """

    if len(data) < 12:
        return

    ret = {}

    data_type, = struct.unpack_from("<B", data, offset=11)
    if data_type == 0x0D:
        temperature, humidity = struct.unpack_from("<HH", data, offset=14)
        ret[TEMPERATURE_LABEL] = temperature / 10
        ret[HUMIDITY_LABEL] = humidity /10
    elif data_type == 0x06:
        humidity, = struct.unpack_from("<H", data, offset=14)
        ret[HUMIDITY_LABEL] = humidity /10
    elif data_type == 0x04:
        temperature, = struct.unpack_from("<H", data, offset=14)
        ret[TEMPERATURE_LABEL] = temperature / 10
    elif data_type == 0x0A:
        battery, = struct.unpack_from("<B", data, offset=14)
        ret['battery_percent'] = battery

    return ret

class BleAdvParser:
    def __init__(self):
        self.clear()

    def clear(self):
        self.mfg_data = {}
        self.service_data = {}
        self.ad_elements = [] # { 'ad_type: , 'ad_data': }

    def parse_ad_elements(self):
        for e in self.ad_elements:

            ad_type = e['ad_type']
            ad_data = e['ad_data']

            if ad_type == 255: # MFG data
                mfg_id, = struct.unpack_from('<H', ad_data)
                self.mfg_data[mfg_id] = ad_data[2:]

            elif ad_type == 0x16: # 16-bit service data
                service_uid, = struct.unpack_from('<H', ad_data)
                self.service_data[service_uid] = ad_data[2:]

    def parse(self, data):
        offset = 0
        while offset < len(data):
            length, ad_type = struct.unpack_from("<BB", data, offset=offset)
            if length - 1 > 0:
                ad_length = length -1
                ad_data = data[offset + 2 : offset + 2 + ad_length ]
                self.ad_elements.append( { 'ad_type': ad_type, 'ad_data': ad_data } )

            offset += length + 1

        self.parse_ad_elements()

def send_sensor_data(points):
    print(points)
    client = InfluxDBClient(host=INFLUX_SERVER[0], port=INFLUX_SERVER[1])
    client.switch_database('mydb')
    client.write_points(points)
    client.close()
    client = None

def update_sensor_data(address, data):
    # Find device
    for sensor in SENSOR_MAP:
        if sensor['address'].casefold() == address.casefold():
            now = time.monotonic()
            since_last = now - sensor['last_sent'] 
            sensor['accrued_data'].update(data)

            if since_last >= TX_INTERVAL:
                #print("{}".format(sensor['tags']))
                data = sensor['accrued_data']
                sensor['last_sent'] = now
                json_body = []
                for datakey in data.keys():
                    m = {
                            'measurement': datakey,
                            'tags': sensor['tags'],
                            'fields': {
                                'value': data[datakey]
                            }
                        }
                    json_body.append(m)
                send_sensor_data(json_body)
                sensor['accrued_data'] = {}


def main():
    port = serial.Serial("/dev/ttyACM0", 115200, timeout=0.1)
    sio = io.TextIOWrapper(io.BufferedRWPair(port, port), encoding='ascii', newline=None)

    # Read and ignore first line
    line = sio.readline()
    while True:
        try:
            line = sio.readline()
            line = line.rstrip()
            items = line.split(',')

            address = items[0]
            rssi = float(items[2])
            adv_data = items[5]

            data = binascii.unhexlify(adv_data)

            p = BleAdvParser()
            p.parse(data)

            if RUUVI_MFG_ID in p.mfg_data:
                data = parse_ruuvi_mfg_data(p.mfg_data[RUUVI_MFG_ID])
                data['rssi'] = rssi
                #print(data)
                update_sensor_data(address, data)

            if MIJIA_SVC_ID in p.service_data:
                data = parse_mijia_sensor_data(p.service_data[MIJIA_SVC_ID])
                data['rssi'] = rssi
                #print(data)
                update_sensor_data(address, data)

    #        print(data)
        except UnicodeDecodeError as e:
            pass
        except IndexError as e:
            pass
        except binascii.Error as e:
            pass
        except ValueError as e:
            pass
    
if __name__ == "__main__":
    main()
