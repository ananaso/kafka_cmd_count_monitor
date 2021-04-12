#!/usr/bin/env python3

import fnmatch
import json
import sys
from kafka import KafkaConsumer

INTOPIC = 'cosmos-out'
TTC_TYPE_TLM = 'COSMOS_SIM_TELEMETRY'
TTC_TYPE_CMD = 'COSMOS_SIM_COMMAND'
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

        # we only want to parse messages that apply to our tests
        if ttctype != TTC_TYPE_TLM:
            continue

        # isolate the only cmd counts we care about
        latest_cmd_valid_count = 0
        for key, value in decoded_message.items():
            if fnmatch.fnmatch(key, '*CMD_VALID_COUNT'):
                print(f'valid: {key} = {value}')
                latest_cmd_valid_count += value

        # i.e. if we're initializing
        if cmd_valid_count is None:
            cmd_valid_count = latest_cmd_valid_count
        elif cmd_valid_count != latest_cmd_valid_count:
            print(f"WARNING: latest command count ({latest_cmd_valid_count}) \
                    doesn't matched stored command count ({cmd_valid_count})", 
                    logfile)

    except Exception as err:
        print(f'Failed to decode: {msg.value} {err}', logfile)
