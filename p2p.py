import random
import pickle
import struct
import demjson
import leveldb
import threading
from time import time
from uuid import uuid4
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.protocols.basic import LineReceiver
from twisted.internet.protocol import Factory
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet.endpoints import TCP4ClientEndpoint, connectProtocol

from config_handler import load_config

LISTENING_PORT = load_config('P2P_LISTENING_PORT')
CONNECTION_PORT = load_config('P2P_CONNECTION_PORT')
HOST = load_config('P2P_HOST')
db = None
ndb = None
tlog = None

generate_nodeid = lambda: str(uuid4())
LOCK = threading.Lock()

HEAD = None


def creat_leveldb():
    while True:
        try:
            return leveldb.LevelDB(load_config('CHAIN_DB'))
        except:
            pass


def creat_book():
    while True:
        try:
            return leveldb.LevelDB(load_config('BOOK_DB'))
        except:
            pass


def creat_tlog():
    while True:
        try:
            return leveldb.LevelDB(load_config('TRANSACTION_LOG'))
        except:
            pass


def do_send(conn, data):
    header = struct.pack("!1I", data.__len__())
    conn.transport.write(header + data)


class book(object):
    def __init__(self, type, add, asset):
        self.type = type
        self.address = add
        self.asset = asset


class Msg(object):
    def __init__(self, type):
        self.type = type
        self.data = []
        self.stat = -1
        self.compelet = False


class Block(object):
    def __init__(self, id, proof, pre_hash, timestamp):
        self.id = id
        self.current_transaction = []
        self.proof = proof
        self.pre_hash = pre_hash
        self.timestamp = timestamp


class MyProtocol(LineReceiver):
    def __init__(self, factory):
        self.factory = factory
        self.nodeid = self.factory.nodeid
        self.remote_nodeid = None
        self.last_ping = 0
        self.lc_ping = LoopingCall(self.send_ping)
        self.miner_id = None
        self._data_buffer = bytes()

    def connectionMade(self):
        LineReceiver.connectionMade(self)
        remote_ip = self.transport.getPeer()
        host_ip = self.transport.getHost()
        self.remote_ip = remote_ip.host + ":" + str(remote_ip.port)
        self.host_ip = host_ip.host + ":" + str(LISTENING_PORT)
        print("Connection from", self.transport.getPeer())

    def connectionLost(self, reason):
        LineReceiver.connectionLost(self, reason)
        try:
            self.lc_ping.stop()
        except:
            pass
        print('pair: ', self.factory.pair)
        print('conn_list: ', self.factory.connection_list)
        print('peers: ', self.factory.peers)
        print('host_list:', self.factory.host_list)
        print('miner_list:', self.factory.miner_nodes)
        if self.miner_id in self.factory.miner_nodes:
            self.factory.miner_nodes.pop(self.miner_id)
            print('miner :', self.miner_id, ' disconnected')
        if self.remote_nodeid in self.factory.peers:
            host = self.factory.pair[self.remote_nodeid]
            self.factory.pair.pop(self.remote_nodeid)
            self.factory.peers.pop(self.remote_nodeid)
            self.factory.host_list.remove(host)
            self.factory.connection_list.remove(host)
            print(self.remote_nodeid, "disconnected")
        else:
            print(self.nodeid, 'disconnected')

    def sendLine(self, data):
        do_send(self, data)

    def data_handle(self, data):
        data = demjson.decode(data)
        if data['type'] == 'connected':
            self.handle_hello(data)
        if data['type'] == 'ping':
            self.handle_ping()
        if data['type'] == 'response':
            self.handle_response()
        if data['type'] == 'addr':
            self.handle_addr(data)
        if data['type'] == 'boardcast':
            self.handle_boardcast(data)
        if data['type'] == 'add_miner':
            self.handle_add_miner(data)
        if data['type'] == 'new_block':
            self.handle_newblock(data)
        if data['type'] == 'sync_chain_request_byMiner':
            self.handle_sync_chain_request_byMiner()
        if data['type'] == 'sync_chain_request_byNode':
            self.handle_sync_chain_request_byNode()
        if data['type'] == 'sync_chain':
            self.handle_sync_chain(data)
        if data['type'] == 'transaction':
            self.handle_transaction(data)
        if data['type'] == 'asset':
            self.handle_asset(data)

    def dataReceived(self, data):
        self._data_buffer += data
        headerSize = 4
        while True:
            if len(self._data_buffer) < headerSize:
                return
            bodySize = struct.unpack('!1I', self._data_buffer[:headerSize])[0]
            if len(self._data_buffer) < headerSize + bodySize:
                return
            body = self._data_buffer[headerSize:headerSize + bodySize]
            # print(body)
            self.data_handle(body)
            self._data_buffer = self._data_buffer[headerSize + bodySize:]

    def handle_asset(self, data):
        global ndb
        LOCK.acquire()
        ndb = creat_book()
        asset = ndb.Get(data['data'].encode()).decode()
        msg = book('asset', data['data'], asset).__dict__
        self.sendLine(demjson.encode(msg).encode())
        ndb = None
        LOCK.release()

    def handle_transaction(self, data):
        global tlog
        LOCK.acquire()
        try:
            tlog = creat_tlog()
            _trx = tlog.Get(data['data']['trx_id'].encode())
        except:
            trx = data['data']
            self.handle_boardcast(data, type='transaction')
            if trx['_from'] != '0':
                for miner in self.factory.miner_nodes:
                    _miner = self.factory.miner_nodes[miner]
                    do_send(_miner, str(data).encode())
        finally:
            tlog = None
            LOCK.release()

    def handle_sync_chain(self, data):
        global db, HEAD
        LOCK.acquire()
        chain = data['data']
        db = creat_leveldb()
        i = -1
        for index, jindex in db.RangeIter():
            i += 1
        if i == -1:
            HEAD = Block(-1, 0, 0, 0)
        else:
            HEAD = pickle.loads(db.Get(str(i).encode()))
        print('data[stat] = ', data['stat'], ' ', HEAD.id)
        if int(data['stat']) <= HEAD.id:
            self.factory.failed_num += 1
            if self.factory.failed_num == len(self.factory.peers):
                for node in self.factory.miner_nodes:
                    _node = self.factory.miner_nodes[node]
                    msg = Msg('sync_finish').__dict__
                    do_send(_node, str(demjson.encode(msg)).encode())
                    break
            db = None
            LOCK.release()
            return
        for block in chain:
            _block = Block(block['id'], block['proof'], block['pre_hash'], block['timestamp'])
            _block.current_transaction = block['current_transaction']
            db.Put(str(_block.id).encode(), pickle.dumps(_block))
        db = None
        if data['compelet'] == True:
            for node in self.factory.miner_nodes:
                _node = self.factory.miner_nodes[node]
                msg = Msg('sync_finish').__dict__
                do_send(_node, str(demjson.encode(msg)).encode())
                break
        LOCK.release()

    def handle_sync_chain_request_byMiner(self):
        self.factory.failed_num = 0
        print('recived sync request')
        if not self.factory.peers:
            print('NO Peers')
            msg = Msg('sync_finish').__dict__
            self.sendLine(str(demjson.encode(msg)).encode())
            return
        for node in self.factory.peers:
            _node = self.factory.peers[node]
            msg = demjson.encode(Msg('sync_chain_request_byNode').__dict__).encode()
            do_send(_node, msg)

    def handle_sync_chain_request_byNode(self):
        global db, HEAD
        print('recived_sync_chain_request')
        LOCK.acquire()
        db = creat_leveldb()
        i = -1
        for index, jindex in db.RangeIter():
            i += 1
        if i == -1:
            HEAD = Block(-1, 0, 0, 0)
        else:
            HEAD = pickle.loads(db.Get(str(i).encode()))
        num = 0
        chain = Msg('sync_chain')
        for block in range(0, HEAD.id + 1):
            _block = pickle.loads(db.Get(str(block).encode())).__dict__
            chain.data.append(_block)
            num += 1
            if num == 100:
                chain.stat = HEAD.id
                chain = chain.__dict__
                self.sendLine(str(demjson.encode(chain)).encode())
                num = 0
                chain = Msg('sync_chain')

        chain.stat = HEAD.id
        chain.compelet = True
        print('db created')
        chain = chain.__dict__
        db = None
        self.sendLine(str(demjson.encode(chain)).encode())
        LOCK.release()

    def handle_newblock(self, data):
        # print('new_block_data: ', data)
        global db
        LOCK.acquire()
        try:
            db = creat_tlog()
            db.Get(data['data'][-1]['id'].encode())
            print('Already Exist!')
        except:
            self.handle_boardcast(data, type='new_block')
            for miner in self.factory.miner_nodes:
                _miner = self.factory.miner_nodes[miner]
                do_send(_miner, str(data).encode())
        finally:
            db = None
            LOCK.release()

    def handle_add_miner(self, data):
        self.miner_id = data['miner_id']
        self.factory.miner_nodes[self.miner_id] = self
        print('new miner added!')

    def handle_boardcast(self, rawdata, type='boardcast'):
        global db
        data = rawdata
        if isinstance(rawdata, str):
            data = eval(rawdata)
        for host in self.factory.peers:
            ip = self.factory.pair[host]
            tar_host = self.factory.peers[host].transport.getHost()
            from_ip = f'{tar_host.host}:{tar_host.port}'
            det_time = int(time()) - int(data['time'])
            if str(data['_from']) != str(ip):
                if type == 'boardcast':
                    if det_time < 3:
                        send_data = demjson.encode(
                            {
                                'type': data['type'],
                                'data': data['data'],
                                '_from': from_ip,
                                'time': data['time'],
                                'sign': data['sign']
                            }
                        )
                        do_send(self.factory.peers[host], send_data.encode())
                else:
                    send_data = demjson.encode(
                        {
                            'type': data['type'],
                            'data': data['data'],
                            '_from': from_ip,
                            'time': data['time'],
                            'sign': data['sign']
                        }
                    )
                    do_send(self.factory.peers[host], send_data.encode())

    def handle_addr(self, data):
        list = data['data']
        if len(self.factory.connection_list) > 4:
            return
        for host in list:
            print('handle list :', host)
            if host not in self.factory.connection_list:
                ip = str(host).split(':')[0]
                port = str(host).split(':')[1]
                point = TCP4ClientEndpoint(reactor, ip, int(port))
                d = connectProtocol(point, MyProtocol(factory))
                d.addCallback(gotProtocol)

    def handle_hello(self, data):
        self.remote_nodeid = data['data']
        if self.remote_nodeid == self.nodeid:
            print("Connected to myself.")
            self.transport.loseConnection()
        else:
            host_ip = str(data['remote_ip'])
            self.lc_ping.start(60)
            self.send_addr()
            if host_ip not in self.factory.connection_list:
                self.factory.peers[self.remote_nodeid] = self
                self.factory.pair[self.remote_nodeid] = host_ip
                ip = host_ip.split(':')[0]
                port = host_ip.split(':')[1]
                self.factory.connection_list.add(host_ip)
                self.factory.host_list.add(host_ip)
                point = TCP4ClientEndpoint(reactor, ip, int(port))
                d = connectProtocol(point, MyProtocol(factory))
                d.addCallback(gotProtocol)

    def send_addr(self):
        hostlist = list(set(self.factory.host_list))
        data = demjson.encode({'type': 'addr', 'data': random.sample(hostlist, min(len(hostlist), 2))})
        self.sendLine(str(data).encode())

    def send_ping(self):
        msg = demjson.encode({'type': 'ping', 'data': f'ping from : {self.transport.getHost()}'})
        self.sendLine(str(msg).encode())

    def send_response(self):
        msg = demjson.encode({'type': 'response', 'data': f'response from : {self.transport.getHost()}'})
        self.sendLine(str(msg).encode())

    def handle_ping(self):
        print('received ping from:', self.transport.getPeer())
        self.send_response()

    def handle_response(self):
        print(f'recived response from {self.transport.getPeer()}')
        self.last_ping = time()

    def send_hello(self):
        hello = demjson.encode({'type': 'connected', 'data': self.nodeid,
                                'remote_ip': f'{str(self.transport.getHost().host)}:{LISTENING_PORT}'})
        print('send hello')
        print('list ', self.factory.connection_list)
        self.sendLine(str(hello).encode())

    def getData(self):
        pass

    def boardcastData(self):
        pass


class MyFactory(Factory):
    def startFactory(self):
        self.peers = {}
        self.host_list = set()
        self.connection_list = set()
        self.nodeid = generate_nodeid()
        self.pair = {}
        self.miner_nodes = {}
        self.failed_num = 0

    def buildProtocol(self, addr):
        return MyProtocol(self)


def gotProtocol(p):
    p.send_hello()


factory = MyFactory()
endpoint = TCP4ServerEndpoint(reactor, LISTENING_PORT)
endpoint.listen(factory)

point = TCP4ClientEndpoint(reactor, HOST, CONNECTION_PORT)
d = connectProtocol(point, MyProtocol(factory))
d.addCallback(gotProtocol)

print("Connected...")
reactor.run()
