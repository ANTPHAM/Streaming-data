#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct  6 17:53:19 2019

@author: antoine
"""
import json
from kafka import KafkaConsumer
consumer = KafkaConsumer("bitcoin_index", bootstrap_servers='localhost:9092', group_id="bitcoin_index_dashboard")

for message in consumer:
    data = json.loads(message.value.decode())
    index_time = data["time"]["updated"]
    index_rate = data["bpi"]["EUR"]["rate_float"]
    # We ping the server every 10s to show we are alive
    
    print("{} New index {} BTI".format(
            index_time,
            index_rate                
        ))
    
