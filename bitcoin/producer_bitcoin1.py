# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import json
import time
from kafka import KafkaProducer
import urllib.request

url = "https://api.coindesk.com/v1/bpi/currentprice/EUR.json"


def Producer():
    ''' get the data on the API blockchain and send it to different consumers following un given topic
    '''
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    last_ping_time = time.time()
    
    while True:
        data = open_url_BPI()
        if time.time() - last_ping_time >= 60:
            last_ping_time = time.time()
            # Receive event
            data = json.loads(data)
            producer.send("bitcoin_index", json.dumps(data).encode(),key=str(data['time']['updated']).encode())
            index_time = data["time"]["updated"]
            index_rate = data["bpi"]["EUR"]["rate_float"]
            # We ping the server every 10s to show we are alive
            time.sleep(2)
            print("{} New index {} BTI".format(
                    index_time,
                    index_rate                
                ))
        else:
            pass
        
def open_url_BPI():
    with urllib.request.urlopen(url) as response:
        data = response.read()   
    return data
    
if __name__ == "__main__":
    Producer()
