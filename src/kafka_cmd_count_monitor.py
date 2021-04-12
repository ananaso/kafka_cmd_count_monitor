#!/usr/bin/env python3

import fnmatch
import json
import sys
from kafka import KafkaConsumer

INTOPIC = 'cosmos-out'
TTC_TYPE = 'COSMOS_SIM_TELEMETRY'
BOOTSTRAP = 'esnode01.has.com:9092'
#BOOTSTRAP = 'esnode02.has.com:9092' # alt bootstrap

consumer = KafkaConsumer(INTOPIC, bootstrap_servers = BOOTSTRAP)

logfile = sys.stdout

cmd_valid_count = None

for msg in consumer:
    try:
        data = json.loads(msg.value)
        message = json.loads(data['message'])
        ttctype = message['zeek']['ttc_type']
        decoded_message = message['decoded']
        print(f'\n---decoded---{decoded_message}\n')

        # we only want to parse messages that apply to our tests
        if ttctype != TTC_TYPE:
            print(f'wrong ttc type: {ttctype}')
            continue

        for key, value in decoded_message.items():
            if fnmatch.fnmatch(key, '*CMD_VALID_COUNT'):
                print(f'valid: {key} = {value}')

    except Exception as err:
        print(f'Failed to decode: {msg.value} {err}', logfile)
