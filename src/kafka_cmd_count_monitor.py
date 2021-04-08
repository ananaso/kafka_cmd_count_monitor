#!/usr/bin/env python3

from kafka import KafkaConsumer

intopic = 'cosmos_out'
TTC_TYPE = 'COSMOS_SIM_COMMAND'
bootstrap = 'esnode01.has.com:9092'
#bootstrap = 'esnode02.has.com:9092' # alt bootstrap

consumer = KafkaConsumer(intopic, bootstrap_servers = bootstrap)

logfile = sys.stdout

for msg in consumer:
    try:
        data = json.loads(msg.value)
        print(data)
    except Exception as err:
        print(f'Failed to decode: {msg.value} {err}', logfile)
