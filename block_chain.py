import random
import hashlib
import demjson
from time import time

FIRST_POOF = 0


class Block(object):
    def __init__(self, id, proof, pre_hash, timestamp,current_transaction):
        self.id = id
        self.current_transaction = current_transaction
        self.proof = proof
        self.pre_hash = pre_hash
        self.timestamp = timestamp


class Transaction(object):
    def __init__(self, id, _from, to, asset, timestamp):
        self.trx_id = id
        self._from = _from
        self.to = to
        self.asset = asset
        self.timestamp = timestamp


class Msg(object):
    def __init__(self, type):
        self.type = type
        self.data = []
        self.time = int(time())
        self._from = '0'
        self.sign = ''


def hash(block):
    sorted_transaction = sorted(block.current_transaction, key=lambda x: x['trx_id'])
    block_dict = {
        'id': block.id,
        'proof': block.proof,
        'pre_hash':block.pre_hash,
        'timestamp':block.timestamp,
        'current_transaction':sorted_transaction
    }
    block_string = demjson.encode(block_dict)
    hash_string = hashlib.sha256(block_string.encode()).hexdigest()
    return hash_string
    #hash_string


def valid_proof(last_proof, proof):
    guess = f'{last_proof}{proof}'.encode()
    guess_hash = hashlib.sha256(guess).hexdigest()
    return guess_hash[:5] == '00000'


def proof_of_work(last_proof):
    print(time())
    proof = 0
    while valid_proof(last_proof, proof) is False:
        proof = random.randint(0, 2 ** 256)
    print(time())
    return proof

# print(proof_of_work(0))
