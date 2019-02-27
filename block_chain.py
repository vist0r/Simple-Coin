import random
import hashlib
import pickle
from time import time

FIRST_POOF = 0


class Block(object):
    def __init__(self, id, proof, pre_hash,timestamp):
        self.id = id
        self.current_transaction = []
        self.proof = proof
        self.pre_hash = pre_hash
        self.timestamp = timestamp


class Msg(object):
    def __init__(self, type):
        self.type = type
        self.data = []
        self.time = int(time())
        self._from = '0'



def hash(block):
    block_string = pickle.dumps(block)
    return hashlib.sha256(block_string).hexdigest()


def valid_proof(last_proof, proof):
    guess = f'{last_proof}{proof}'.encode()
    guess_hash = hashlib.sha256(guess).hexdigest()
    return guess_hash[:5] == '00000'


def proof_of_work(last_proof):
    print(time())
    proof = 0
    while valid_proof(last_proof, proof) is False:
        proof = random.randint(0, 2 ** 64)
    print(time())
    return proof

# print(proof_of_work(0))
