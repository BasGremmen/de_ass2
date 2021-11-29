import websocket
import json
from kafka import KafkaProducer
api_key = 'AC857EEF-D841-4FF7-BE8E-61121010D85A' #your api key


class CoinAPIv1_subscribe(object):
    def __init__(self, apikey):
        self.type = "hello"
        self.apikey = apikey
        self.heartbeat = True
        self.subscribe_data_type = ["trade"]
        self.subscribe_filter_asset_id = ["ADA"]


def on_message(ws, message):
    data = json.loads(message)
    if data['type'] == 'trade':
        producer.send('trade', bytes(message, encoding='utf-8'))


def on_error(ws, message):
    print(message)


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")


def on_open(ws):
    sub = CoinAPIv1_subscribe(api_key)
    ws.send(json.dumps(sub.__dict__))


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers='35.226.103.131:9092')  # VM's external IP
    ws = websocket.WebSocketApp("ws://ws.coinapi.io/v1/",
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()
