import websocket
import asyncio
import json
import uuid
import multiprocessing as mp
import pyupbit as up
import time

class WebSocketManager(mp.Process):
    def __init__(self, type: str, codes: list, qsize: int=1000):
        self.__q = mp.Queue(qsize)
        self.alive = False

        self.type = type
        self.codes = codes

        super().__init__()

    async def __connect_socket(self):
        uri = "wss://api.upbit.com/websocket/v1"
        if True or 'https_proxy' in os.environ:
            import websocket
            while True:
                try:
                    ws = websocket.WebSocket()     
                    ws.connect(uri)
                    data = [{"ticket": str(uuid.uuid4())[:6]}, {"type": self.type, "codes": self.codes}]
                    ws.send(json.dumps(data))
                    while self.alive:
                        recv_data = ws.recv()
                        data = json.loads(recv_data.decode('utf8'))
                        self.__q.put([data['timestamp'], data['code'],data['trade_price'],data['trade_volume'],data['ask_bid'][0]])
                except:
                    traceback.print_exc()
                    time.sleep(1)
        else:
            import websockets as ws
            async with ws.connect(uri, ping_interval=60) as websocket:
                data = [{"ticket": str(uuid.uuid4())[:6]}, {"type": self.type, "codes": self.codes}]
                await websocket.send(json.dumps(data))
            while self.alive:
                recv_data = websocket.recv()
                data = json.loads(recv_data.decode('utf8'))
                self.__q.put([data['timestamp'], data['code'],data['trade_price'],data['trade_volume'],data['ask_bid'][0]])

    def run(self):
        self.__aloop = asyncio.get_event_loop()
        self.__aloop.run_until_complete(self.__connect_socket())

    def get(self):
        if self.alive == False:
            self.alive = True
            self.start()
        return self.__q.get(block=True, timeout=1000)

    def terminate(self):
        self.alive = False
        super().terminate()

from datetime import datetime as dt, timedelta
import sys, os
import traceback
from struct import pack
import numpy as np
from zipfile import ZipFile
import zipfile
import glob
import threading
import platform

def zip_and_upload(date):
    zip_file = 'upbit-%s.zip' % date
    with ZipFile(zip_file, 'w', compression=zipfile.ZIP_BZIP2) as myzip:
        files = glob.glob('upbit-%s.*.bin' % date)
        for file in files:
            myzip.write(file)
            # os.remove(file)
    if not 'Linux' in platform.platform():
        os.system('rclone copy %s gdrive:' % zip_file)

def git_add(file):
    os.system("../g %s" % file)

PREFIX = '.'

if __name__ == "__main__":
    save = True
    if '-x' in sys.argv:
        save = False    
    tickers = up.get_tickers(fiat='KRW')
    old_hours = 0
    old_file = ''
    hours = 1
    f = None
    data = None
    while True:
        wm = WebSocketManager("ticker", tickers)
        while True:
            if hours > old_hours or f is None:
                now = dt.now()
                tag = now.strftime('%Y-%m-%d.%H')
                file = '%s/upbit-%s.bin' % (PREFIX, target)
                if len(old_file) > 10:
                    threading.Thread(target=git_add, args=(old_file)).start()
                    old_file = file
                # if not os.path.exists(file):
                #     if '.00' in tag and save:
                #         date = (now-timedelta(days=1)).strftime('%Y-%m-%d')
                #         threading.Thread(target=zip_and_upload, args=(date,)).start()
                #     print(file)
                f = open(file, 'ab')
                old_hours = hours
                tickers = up.get_tickers(fiat='KRW')
                wm = WebSocketManager("ticker", tickers)
            try:
                data = wm.get()
                f.write(pack('q4sffb', data[0], bytes(data[1][4:], encoding='euc-kr'), data[2], data[3], ord(data[4])))
                hours = int(data[0]/1000/60/60)
            except:
                f = None