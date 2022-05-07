# 指定されたチャンネルのメッセージをひたすらログ出力する

import websocket
import time
import sys
import json
from loguru import logger

channel = sys.argv[1]

logger.add(
    f'{channel}.log', 
    format='T->[{time:YYYY-MM-DD HH:mm:ss.SSS}] F->[{function}] M->[{message}]',
    rotation='00:00')

def on_message(ws, message):
    logger.info(message)

def on_open(ws):
    ws.send(json.dumps({
        'method': 'subscribe',
        'params': {
            'channel': channel
        }
    }))

def main():
    while True:
        try:
            ws = websocket.WebSocketApp(
                'wss://ws.lightstream.bitflyer.com/json-rpc',
                on_open=on_open,
                on_message=on_message,
                on_error=lambda x, e: logger.exception(e),
                on_close=None,
                on_ping=None,
                on_pong=None,
                on_cont_message=None,
                on_data=None)
            
            ws.run_forever()
        except Exception as e:
            logger.exception(e)
            time.sleep(1)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.exception(f'unexpected error {e}')
