from binance.websocket.spot.websocket_client import SpotWebsocketClient
from binance.spot import Spot as Client
import sqlalchemy as db
import pandas as pd
import logging
import asyncio
import json
import time
import csv
import os

engine = db.create_engine('sqlite:///spread.db')
connection = engine.connect()

symbol = 'APEUSDT'
base_url = 'https://api.binance.com'
stream_url = 'wss://stream.binance.com:9443/ws'

client = Client(base_url=base_url)
ws_client = SpotWebsocketClient(stream_url=stream_url)

order_book = {
    "lastUpdateId": 0,
    "bids": [],
    "asks": []
}

def get_snapshot():
    """
    Retrieve order book
    """
    return client.depth(symbol, limit=1000)

def manage_order_book(side, update):
    """
    Updates local order book's bid or ask lists based on the received update ([price, quantity])
    """
    price, quantity = update
    # price exists: remove or update local order
    for i in range(0, len(order_book[side])):
        if price == order_book[side][i][0]:
            # quantity is 0: remove
            if float(quantity) == 0:
                order_book[side].pop(i)
                return
            else:
                # quantity is not 0: update the order with new quantity
                order_book[side][i] = update
                return

    # price not found: add new order
    if float(quantity) != 0:
        order_book[side].insert(-1, update) 
        if side == 'asks':
            order_book[side] = sorted(order_book[side], key=lambda x: float(x[0])) # asks prices in ascendant order
        else:
            order_book[side] = sorted(order_book[side], key=lambda x:float(x[0]) ,reverse=True)  # bids prices in descendant order

    if len(order_book[side]) > 1000:
            order_book[side].pop(len(order_book[side])-1)

def process_updates(message):
    start = time.time()

    for update in message['b']:
        manage_order_book('bids', update)
    for update in message['a']:
        manage_order_book('asks', update)
    now = time.time()

def message_handler(message):
    global order_book
    if "depthUpdate" in json.dumps(message):
        last_update_id = order_book['lastUpdateId']
        if message['u'] <= last_update_id:
            return  
        if message['U'] <= last_update_id + 1 <= message['u']:
            order_book['lastUpdateId'] = message['u']
            process_updates(message)
        else:
            logging.info('Out of sync, re-syncing...')
            order_book = get_snapshot()

def spread(orderbook):
    best_bid = float(orderbook["bids"][0][0])
    best_ask = float(orderbook["asks"][0][0])
    spread = ((best_ask-best_bid)/best_ask)*100
    spread = '{:.10f}'.format(spread)
    return spread

def create_df(orderbook, spread_val):
    best_bid = float(orderbook["bids"][0][0])
    best_ask = float(orderbook["asks"][0][0])
    msg = {'timestamp':time.time(), 'best_bid':best_bid, 'best_ask':best_ask, 'spread':spread_val}
    df = pd.DataFrame([msg])
    df.astype('float')
    return df

def write_csv_row():
    with open(f'{symbol}_spread.csv', 'a', encoding='UTF8') as f:
        writer = csv.writer(f)    
        best_bid = float(order_book["bids"][0][0])
        best_ask = float(order_book["asks"][0][0])
        spread_val = spread(order_book)
        row = [time.time(), best_bid, best_ask, spread_val]
        
        writer.writerow(row)

def write_csv_header():
    header = ['timestamp','best_bid', 'best_ask', 'spread']
    with open(f'{symbol}_spread.csv', 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow(header)

async def listen_ws():
    ws_client.start()
    ws_client.diff_book_depth(
        symbol=symbol.lower(),
        id=1,
        speed=1000,
        callback=message_handler,
    )

async def get_best_price():

    while True:
        if order_book.get('lastUpdateId') > 0:
            #SAVE TO SQL DB
            df = create_df(order_book, spread(order_book))
            df.to_sql(f'{symbol.lower()}', connection, if_exists='append', index=False)
            
            #SAVE TO CSV        
            write_csv_row()
            
        await asyncio.sleep(1)


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(listen_ws(), get_best_price()))
    loop.close()

main()

