import socket
import leveldb
import demjson
import threading
from uuid import uuid4
from block_chain import *

BUF_SIZE = 1024 * 256
HOST = '127.0.0.1'
PORT = 4999
TAIL = '\r\n'
LOCK = threading.Lock()
NOW = None
db = None
FIND_NEW_BLOCK = threading.Event()
BOARDCAST_EVENT = threading.Event()
SYNC_FINISH_EVENT = threading.Event()

generate_minerid = lambda: str(uuid4())
MINER_ID = generate_minerid()
CON = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
CON.connect((HOST, PORT))
hello = str(demjson.encode({'type': 'add_miner', 'miner_id': f'{MINER_ID}'})) + TAIL
CON.send(hello.encode())
sync_chain = str(demjson.encode({'type': 'sync_chain_request_byMiner', 'miner_id': f'{MINER_ID}'})) + TAIL
CON.send(sync_chain.encode())


def creat_leveldb():
    while True:
        try:
            return leveldb.LevelDB('./chain.db')
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
    Creation_block = Block(0, 0, 0, 0)
    NOW = Creation_block
    index = str(Creation_block.id).encode()
    block = pickle.dumps(Creation_block)
    db.Put(index, block)
    db = None
    print('Initialization successful!')


def check_valid(chain):
    Last_block = Block(chain[0]['id'], chain[0]['proof'], chain[0]['pre_hash'],chain[0]['timestamp'])
    Last_block.current_transaction = chain[0]['current_transaction']
    index = 1
    while index < len(chain):
        block = Block(chain[index]['id'], chain[index]['proof'], chain[index]['pre_hash'],chain[index]['timestamp'])
        block.current_transaction = chain[index]['current_transaction']
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
        print("HASH NOT EQUAL")
        return False
    if not valid_proof(last_block.proof, chain[0]['proof']):
        print("PROOF INVALID")
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
    while block:
        if block.pre_hash != hash(last_block):
            return False
        if not valid_proof(last_block.proof, block.proof):
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
    global NOW, db
    proof = random.randint(0, 2 ** 256)
    while True:
        ans = False
        print('Mining Block id = ', NOW.id)
        while True:
            try:
                if newblock_event.is_set():
                    newblock_event.clear()
                    print('There is new chain!')
                    break
            except:
                pass

            if (valid_proof(NOW.proof, proof) is False):
                proof = random.randint(0, 2 ** 64)
            else:
                ans = True
                break
        if ans is True:
            LOCK.acquire()
            db = creat_leveldb()
            new_block = Block(NOW.id + 1, proof, hash(NOW),int(time()))
            NOW = new_block
            print('New Block Created! ', new_block.id, ' ', new_block.proof, ' ', new_block.pre_hash)
            db.Put(str(new_block.id).encode(), pickle.dumps(new_block))
            db = None
            LOCK.release()
            boardcast_event.set()


def listener(newblock_event, sync_finish_event):
    global NOW, db, CON
    while True:
        data = CON.recv(BUF_SIZE)
        if not data:
            print('Connection Losed!')
            break

        data = demjson.decode(data)
        if (data['type'] == 'sync_finish'):
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

        if not sync_finish_event:
            continue

        if (data['type'] == 'new_block'):
            chain = sorted(data['data'], key=lambda x: x['id'])
            LOCK.acquire()
            db = creat_leveldb()
            solved = resolve_conflicts(db, chain)
            if solved:
                for block in chain:
                    new_block = Block(block['id'], block['proof'], block['pre_hash'],block['timestamp'])
                    NOW = new_block
                    db.Put(str(NOW.id).encode(), pickle.dumps(NOW))
                    # TODO update comfirmed transcation
                print(f'Other Node Solve The Promble ,Merged Chain :{NOW.id}')
                newblock_event.set()
            db = None
            LOCK.release()


def speaker(boardcast_event):
    global NOW, db, CON
    while True:
        if boardcast_event.is_set():
            begin = max(0, NOW.id - 5)
            end = NOW.id
            chain = Msg('new_block')
            LOCK.acquire()
            db = creat_leveldb()
            for id in range(begin, end + 1):
                block = db.Get(str(id).encode())
                Block = pickle.loads(block)
                chain.data.append(dict_block(Block))
            db = None
            LOCK.release()
            chain_json = (str(demjson.encode(dict_chain(chain))) + TAIL).encode()
            CON.send(chain_json)
            boardcast_event.clear()


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
