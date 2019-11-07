# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import json
from time import time
import websocket # install this with the following command: pip install websocket-client

from kafka import KafkaProducer

def Producer():
    ''' get the data on the API blockchain and send it to different consumers following un given topic
    '''
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    ws = open_websocket_to_blockchain()
    last_ping_time = time()
    
    while True:
        # Receive event
        data = json.loads(ws.recv())
        #print(data)
         
        # We ping the server every 10s to show we are alive
        if time() - last_ping_time >= 10:
            ws.send(json.dumps({"op": "ping"}))
            last_ping_time = time()
    
        # Response to "ping" events
        if data["op"] == "pong":
            pass
        # New unconfirmed transactions
        elif data["op"] == "utx":
            producer.send("bitcoin_price", json.dumps(data).encode(),key=str(data['x']['hash']).encode()) #  if data['op'] == "pong", no contain in data, this one raises an error
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
            producer.send("bitcoin_price", json.dumps(data).encode(),key=str(data['x']['hash']).encode())
            block_hash = data['x']['hash']
            block_timestamp = data["x"]["time"]
            block_found_by = data["x"]["foundBy"]["description"]
            block_reward = 12.5 # blocks mined in 2016 have an associated reward of 12.5 BTC
            print(data)
            print("{} New block {} found by {}".format(block_timestamp, block_hash, block_found_by))
# this is when             
        # This really should never happen
        else:
            print("Unknown op: {}".format(data["op"]))

def open_websocket_to_blockchain():
    # Open a websocket
    ws = websocket.WebSocket()
    ws.connect("wss://ws.blockchain.info/inv")
    # Register to unconfirmed transaction events
    ws.send(json.dumps({"op":"unconfirmed_sub"}))
    # Register to block creation events
    ws.send(json.dumps({"op":"blocks_sub"}))
    return ws   

if __name__ == "__main__":
    Producer()
