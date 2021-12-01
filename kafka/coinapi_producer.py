import websocket
import json
from kafka import KafkaProducer

#API_key of CoinAPI
api_key = 'AC857EEF-D841-4FF7-BE8E-61121010D85A' #your api key

# Create the websocket listener settings
class CoinAPIv1_subscribe(object):
    def __init__(self, apikey):
        self.type = "hello"
        self.apikey = apikey
        self.heartbeat = True
        self.subscribe_data_type = ["trade"]
        self.subscribe_filter_asset_id = ["ADA"]

# On message, get result as dictionary and check if data['type'] is set and has value trade
# If so, then send to Kafka
def on_message(ws, message):
    data = json.loads(message)
    if data['type'] == 'trade':
        producer.send('trade', bytes(message, encoding='utf-8'))

# Error handling
def on_error(ws, message):
    print(message)

# Closing handling
def on_close(ws, close_status_code, close_msg):
    print("### closed ###")

# On connect, send hello message with settings
def on_open(ws):
    sub = CoinAPIv1_subscribe(api_key)
    ws.send(json.dumps(sub.__dict__))

# Start the Kafka producer and the websocket listener
if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers='34.67.197.41:9092')  # VM's external IP
    ws = websocket.WebSocketApp("ws://ws.coinapi.io/v1/",
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()
