import bitcoin
def get_key_pair():
    v = False
    while not v:
        private_key = bitcoin.random_key()
        private_key = bitcoin.decode_privkey(private_key)
        v = 0 < private_key < bitcoin.N
    wif_private_key = bitcoin.encode_privkey(private_key,'wif')
    public_key = bitcoin.privkey_to_pubkey(wif_private_key)
    address = bitcoin.pubkey_to_address(public_key)
    return address,wif_private_key

def sign_trx(msg,private_key):
    return bitcoin.ecdsa_sign(msg,private_key)

def verify_sign(msg,sig,address):
    public_key = bitcoin.ecdsa_recover(msg,sig)
    return  address == bitcoin.pubkey_to_address(public_key)

