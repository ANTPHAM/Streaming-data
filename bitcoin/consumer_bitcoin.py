#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct  6 17:53:19 2019

@author: antoine
"""
import json
from kafka import KafkaConsumer
consumer = KafkaConsumer("bitcoin_price", bootstrap_servers='localhost:9092', group_id="bitcoin_price_dashboard")

for message in consumer:
    data = json.loads(message.value.decode())
    if data["op"] == "utx":
        transaction_timestamp = data["x"]["time"]
        transaction_hash = data['x']['hash'] # this uniquely identifies the transaction
        transaction_total_amount = 0

        for recipient in data["x"]["out"]:
            # Every transaction may in fact have multiple recipients
            # Note that the total amount is in hundredth of microbitcoin; you need to
            # divide by 10**8 to obtain the value in bitcoins.
            transaction_total_amount += recipient["value"] / 100000000.

        print("{} New transaction {}: {} BTC".format(
            transaction_timestamp,
            transaction_hash,
            transaction_total_amount
        ))
    # New block
    elif data["op"] == "block":
        block_hash = data['x']['hash']
        block_timestamp = data["x"]["time"]
        block_found_by = data["x"]["foundBy"]["description"]
        block_reward = 12.5 # blocks mined in 2016 have an associated reward of 12.5 BTC
        print("{} New block {} found by {}".format(block_timestamp, block_hash, block_found_by))

    # This really should never happen
    else:
        print("Unknown op: {}".format(data["op"]))
