import socket
import leveldb
import threading
import struct
import pickle
from uuid import uuid4

from ECCSign import *
from block_chain import *
from config_handler import load_config


BUF_SIZE = 1024 * 256
HOST = load_config('P2P_HOST')
PORT = load_config('P2P_LISTENING_PORT')
LOCK = threading.Lock()
NOW = None
db = None
tdb = None
ndb = None

FIND_NEW_BLOCK = threading.Event()
BOARDCAST_EVENT = threading.Event()
SYNC_FINISH_EVENT = threading.Event()
MINER_ADDRESS = None

generate_minerid = lambda: str(uuid4())
generate_trxid = lambda: uuid4().hex
MINER_ID = generate_minerid()
MINER_ADDRESS = "1G1NpejXSEtfpeR3E9qqmvs2hTX4tLMbSH"
CON = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
CON.connect((HOST, PORT))


def sendLine(data):
    header = struct.pack("!1I", data.__len__())
    return header + data


hello = str(demjson.encode({'type': 'add_miner', 'miner_id': f'{MINER_ID}'}))
CON.send(sendLine(hello.encode()))

sync_chain = str(demjson.encode({'type': 'sync_chain_request_byMiner', 'miner_id': f'{MINER_ID}'})).encode()

CON.send(sendLine(sync_chain))


def creat_leveldb():
    while True:
        try:
            return leveldb.LevelDB(load_config('CHAIN_DB'))
        except:
            pass


def creat_trxdb():
    while True:
        try:
            return leveldb.LevelDB(load_config('TRANSACTION_POOL_DB'))
        except:
            pass


def creat_book():
    while True:
        try:
            return leveldb.LevelDB(load_config('BOOK_DB'))
        except:
            pass


def destory_chain():
    db = creat_leveldb()
    for index in db.RangeIter(include_value=False):
        db.Delete(index)
    db = None
    print('Destory successful')


def init_chain(skip=False):
    global NOW, db
    db = creat_leveldb()
    if skip:
        print('Already sync newest Chain')
        db = None
        return
    try:
        NOW = pickle.loads(db.Get(b'0'))
        db = None
        print('Already Init Chain')
        return
    except:
        pass
    Creation_block = Block(0, 0, 0, 0,[])
    NOW = Creation_block
    index = str(Creation_block.id).encode()
    block = pickle.dumps(Creation_block)
    db.Put(index, block)
    db = None
    print('Initialization successful!')


def check_trx(block):
    num = 0
    for trx in block.current_transaction:
        if trx['_from'] == '0':
            num += 1
            if num > 1:
                return False
            if trx['asset'] != 5000000000:
                return False
    return True


def check_valid(chain):
    Last_block = Block(
        chain[0]['id'],
        chain[0]['proof'],
        chain[0]['pre_hash'],
        chain[0]['timestamp'],
        chain[0]['current_transaction']
    )
    if check_trx(Last_block) is False:
        return False
    index = 1
    while index < len(chain):
        block = Block(
            chain[index]['id'],
            chain[index]['proof'],
            chain[index]['pre_hash'],
            chain[index]['timestamp'],
            chain[index]['current_transaction']
        )
        if check_trx(block) is False:
            return False
        if block.pre_hash != hash(Last_block):
            print("Block.pre_hash != Hash(Last_block)")
            print(Last_block.__dict__)
            print(block.__dict__)
            return False
        if not valid_proof(Last_block.proof, block.proof):
            print("Not valid_proof(Last_block.proof, block.proof)")
            print(Last_block.__dict__)
            print(block.__dict__)
            return False
        Last_block = block
        index += 1
    return True


def dict_chain(chain):
    return chain.__dict__


def dict_block(block):
    return block.__dict__


def resolve_conflicts(db, chain):
    global NOW
    # print(chain)
    if (int(chain[-1]['id']) <= int(NOW.id)):
        print("The NEW CHAIN IS SHORT THEN THIS CHAIN ", chain[-1]['id'], '  ', NOW.id)
        return False
    if (int(chain[0]['id']) > int(NOW.id)):
        print('The NEW CHAIN IS TOO LONG ', chain[-1]['id'], '  ', NOW.id)
        # TODO sync chain
        return False

    if not check_valid(chain):
        return False

    if (int(chain[0]['id']) == 0):
        return True

    pre_id = int(pickle.loads(db.Get(str(chain[0]['id']).encode())).id) - 1
    last_block = pickle.loads(db.Get(str(pre_id).encode()))
    if hash(last_block) != chain[0]['pre_hash']:
        print('IN THERE')
        #print(last_block.__dict__)
        #print(chain[0]['pre_hash'])
        with open('error_log1.json','w') as f:
            f.write(demjson.encode(last_block.__dict__))
        with open('error_log2.json','w') as f:
            f.write(chain[0]['pre_hash'])
        print("HASH NOT EQUAL")
        exit(-1)
        return False
    if not valid_proof(last_block.proof, chain[0]['proof']):
        print("PROOF INVALID")
        exit(-1)
        return False
    return True


def get_block(id, db):
    try:
        return pickle.loads(db.Get(str(id).encode()))
    except:
        return None


def check_all(db):
    global NOW
    last_block = get_block(0, db)
    if last_block == None:
        return False
    index = 1
    block = get_block(index, db)
    NOW = last_block
    if block != None:
        NOW = block
        print('NOWb = ', NOW.__dict__)
    while block:
        if block.pre_hash != hash(last_block):
            print(block.__dict__)
            print(last_block.__dict__)
            print(hash(last_block))
            print('block !hash')
            return False
        if not valid_proof(last_block.proof, block.proof):
            print('invalid_proof')
            return False
        last_block = block
        index += 1
        block = get_block(index, db)
        if block != None:
            NOW = block
    return True


def mining(newblock_event, boardcast_event, sync_finish_event):
    print('Sync_chain...')
    while not sync_finish_event.is_set():
        pass
    global NOW, db, tdb, ndb
    proof = random.randint(0, 2 ** 256)
    while True:
        ans = False
        print('Mining Block id = ', NOW.id)
        while True:
            if newblock_event.is_set():
                newblock_event.clear()
                print('There is new chain!')
                break
            if (valid_proof(NOW.proof, proof) is False):
                proof = random.randint(0, 2 ** 256)
            else:
                if newblock_event.is_set():
                    newblock_event.clear()
                    print('There is new chain!')
                    break
                ans = True
                break
        if ans is True:
            LOCK.acquire()
            if not valid_proof(NOW.proof,proof):
                LOCK.release()
                continue
            db = creat_leveldb()
            tdb = creat_trxdb()
            ndb = creat_book()
            #print('NOW hash = ', hash(NOW), ' ', NOW.__dict__)
            new_block = Block(NOW.id + 1, proof, hash(NOW), int(time()), [])
            reward = Transaction(generate_trxid(), '0', MINER_ADDRESS, 5000000000, new_block.timestamp).__dict__
            new_block.current_transaction.append(reward)
            for id, trx in tdb.RangeIter():
                transaction = pickle.loads(trx).__dict__
                new_block.current_transaction.append(transaction)
                tdb.Delete(id)
            NOW = new_block
            db.Put(str(new_block.id).encode(), pickle.dumps(new_block))
            if NOW.id - 6 >= 0:
                trx_block = db.Get(str(NOW.id - 6).encode())
                trx_block = pickle.loads(trx_block)
                trx_list = trx_block.current_transaction
                for trx in trx_list:
                    try:
                        f_amount = int(ndb.Get(trx['_from'].encode()).decode())
                    except:
                        ndb.Put(trx['_from'].encode(), b'0')
                        f_amount = 0

                    try:
                        t_amount = int(ndb.Get(trx['to'].encode()).decode())
                    except:
                        ndb.Put(trx['to'].encode(), b'0')
                        t_amount = 0

                    if trx['_from'] == '0':
                        t_amount += int(trx['asset'])
                        ndb.Put(trx['to'].encode(), str(t_amount).encode())

                    if int(trx['asset']) > 0 and f_amount - int(trx['asset']) >= 0:
                        f_amount -= int(trx['asset'])
                        t_amount += int(trx['asset'])
                        ndb.Put(trx['_from'].encode(), str(f_amount).encode())
                        ndb.Put(trx['to'].encode(), str(t_amount).encode())
            db = None
            tdb = None
            ndb = None
            boardcast_event.set()
            LOCK.release()


def data_handle(data, newblock_event, sync_finish_event):
    global NOW, db, CON, tdb, ndb
    data = demjson.decode(data)
    if data['type'] == 'sync_finish':
        print('Sync chain finished,Begin to check valid')
        LOCK.acquire()
        db = creat_leveldb()
        reslut = check_all(db)
        db = None
        if reslut:
            print('Vaild chain! Begin minning')
            init_chain(skip=True)
        else:
            print('Not valid chain,Begin to init chain ....')
            init_chain()
        sync_finish_event.set()
        LOCK.release()
    if not sync_finish_event.is_set():
        return
    if data['type'] == 'new_block':
        LOCK.acquire()
        chain = data['data']
        db = creat_leveldb()
        tdb = creat_trxdb()
        ndb = creat_book()
        solved = resolve_conflicts(db, chain)
        if solved:
            newblock_event.set()
            for block in chain:
                new_block = Block(
                    block['id'],
                    block['proof'],
                    block['pre_hash'],
                    block['timestamp'],
                    block['current_transaction']
                )
                try:
                    old_block = db.Get(str(block['id']).encode())
                    old_block = pickle.loads(old_block)
                    for trx in old_block.current_transaction:
                        if trx['_from'] == '0':
                            continue
                        _trx = Transaction(trx['trx_id'], trx['_from'], trx['to'], trx['asset'], trx['timestamp'])
                        tdb.Put(trx['trx_id'].encode(), pickle.dumps(_trx))
                    for trx in block['current_transaction']:
                        try:
                            tdb.Get(trx['trx_id'].encode())
                            tdb.Delete(trx['trx_id'].encode())
                        except KeyError:
                           pass
                except KeyError:
                    pass

                NOW = new_block
                db.Put(str(NOW.id).encode(), pickle.dumps(NOW))
                # TODO update comfirmed transcation
            if NOW.id - 6 >= 0:
                trx_block = db.Get(str(NOW.id - 6).encode())
                trx_block = pickle.loads(trx_block)
                trx_list = trx_block.current_transaction
                for trx in trx_list:
                    try:
                        f_amount = int(ndb.Get(trx['_from'].encode()).decode())
                    except:
                        ndb.Put(trx['_from'].encode(), b'0')
                        f_amount = 0

                    try:
                        t_amount = int(ndb.Get(trx['to'].encode()).decode())
                    except:
                        ndb.Put(trx['to'].encode(), b'0')
                        t_amount = 0

                    if trx['_from'] == '0':
                        t_amount += int(trx['asset'])
                        ndb.Put(trx['to'].encode(), str(t_amount).encode())

                    if f_amount - int(trx['asset']) >= 0:
                        f_amount -= int(trx['asset'])
                        t_amount += int(trx['asset'])
                        ndb.Put(trx['_from'].encode(), str(f_amount).encode())
                        ndb.Put(trx['to'].encode(), str(t_amount).encode())
            print(f'Other Node Solve The Promble ,Merged Chain :{NOW.id}')

        db = None
        tdb = None
        ndb = None
        LOCK.release()
    if data['type'] == 'transaction':
        LOCK.acquire()
        print('Recived an transaction! :', data)
        _trx = data['data']
        trx = Transaction(_trx['trx_id'], _trx['_from'], _trx['to'], _trx['asset'], _trx['timestamp'])
        if not (trx.asset > 0 and trx._from - trx.asset >= 0):
            print('Not valid !')
            return
        print('sign data:',data)
        signature = data['sign']
        if verify_sign(demjson.encode(trx.__dict__), signature, trx._from):
            print('OK,valid signature')
            tdb = creat_trxdb()
            tdb.Put(trx.trx_id.encode(), pickle.dumps(trx))
            tdb = None
            print('write Done!')
        LOCK.release()


def listener(newblock_event, sync_finish_event):
    global NOW, db, CON, tdb, ndb
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
            data_handle(body,newblock_event,sync_finish_event)
            dataBuffer = dataBuffer[headerSize + bodySize:]


def speaker(boardcast_event):
    global NOW, db, CON
    while True:
        if boardcast_event.is_set():
            LOCK.acquire()
            begin = max(0, NOW.id - 5)
            end = NOW.id
            chain = Msg('new_block')
            db = creat_leveldb()
            for id in range(begin, end + 1):
                flag = True
                while flag:
                    try:
                        block = db.Get(str(id).encode())
                        Block = pickle.loads(block)
                        chain.data.append(dict_block(Block))
                        flag = False
                    except:
                        pass

            db = None
            chain_json = (str(demjson.encode(dict_chain(chain)))).encode()
            data = sendLine(chain_json)
            CON.send(data)
            boardcast_event.clear()
            LOCK.release()


MINNING_THREAD = threading.Thread(target=mining, args=(FIND_NEW_BLOCK, BOARDCAST_EVENT, SYNC_FINISH_EVENT),
                                  name='worker')
LISTENER_THREAD = threading.Thread(target=listener, args=(FIND_NEW_BLOCK, SYNC_FINISH_EVENT), name='listener')
SPEAKER_THREAD = threading.Thread(target=speaker, args=(BOARDCAST_EVENT,), name='speaker')

# def conversation(host,port)

# CON.send("ok\r\n".encode('utf8'))
# if len(data.decode()) > 0:
# print(data)
# destory_chain()


MINNING_THREAD.setDaemon(True)
LISTENER_THREAD.setDaemon(True)
SPEAKER_THREAD.setDaemon(True)
LISTENER_THREAD.start()
SPEAKER_THREAD.start()

MINNING_THREAD.start()

try:
    while True:
        pass
except:
    print('Stop minning')
