import socket
import struct
import demjson
from time import time
from uuid import uuid4
import threading
from ECCSign import *
from block_chain import Transaction
from config_handler import load_config

BUF_SIZE = 1024 * 256
HOST = load_config('P2P_HOST')
PORT = load_config('P2P_LISTENING_PORT')
NOW = None
db = None

generate_id = lambda: uuid4().hex
USER_ID = generate_id()
CON = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
CON.connect((HOST, PORT))
WAIT_EVENT = threading.Event()

def sendLine(data):
    print(type(data),data)
    header = struct.pack("!1I", data.__len__())
    return header + data

hello = str(demjson.encode({'type': 'add_user', 'miner_id': f'{USER_ID}'}))
CON.send(sendLine(hello.encode()))


class Msg(object):
    def __init__(self, type, data, sign=''):
        self.type = type
        self.data = data
        self.time = int(time())
        self._from = '0'
        self.sign = sign


def data_handle(data,wait):
    data = demjson.decode(data)
    if data['type'] == 'asset':
        add = data['address']
        print(f'{ add } asset is :', data['asset'])
        wait.clear()


def listener(wait):
    global CON
    dataBuffer = bytes()
    headerSize = 4
    while True:
        data = CON.recv(BUF_SIZE)
        if not data:
            print('Connection Losed!')
            break
        dataBuffer += data
        while True:
            if len(dataBuffer) < headerSize:
                break
            bodySize = struct.unpack('!1I', dataBuffer[:headerSize])[0]
            if len(dataBuffer) < headerSize + bodySize:
                break
            body = dataBuffer[headerSize:headerSize + bodySize]
            data_handle(body,wait)
            dataBuffer = dataBuffer[headerSize + bodySize:]


Listener = threading.Thread(target=listener, args=(WAIT_EVENT,), name='listener')
Listener.setDaemon(True)
Listener.start()
while True:
    op = input('>')
    print(op)
    if op == 'keypair':
        k_v = get_key_pair()
        print("Public_key = ", k_v[0], ' Private_Key = ', k_v[1])
    if op.startswith('trx'):
        str = op.split(' ')
        _from = str[1]
        to = str[2]
        asset = str[3]
        private_key = str[4]
        transaction = Transaction(generate_id(), _from, to, asset, int(time())).__dict__
        sign = sign_trx(demjson.encode(transaction), private_key)
        msg = Msg('transaction', transaction, sign).__dict__
        CON.send(sendLine(demjson.encode(msg).encode()))
        # trx = str(transaction)
        # print(trx)
    if op.startswith('asset'):
        str = op.split(' ')
        address = str[1]
        msg = Msg('asset', address).__dict__
        print(msg)
        CON.send(sendLine(demjson.encode(msg).encode()))
        WAIT_EVENT.set()
        while WAIT_EVENT.is_set():
            pass
