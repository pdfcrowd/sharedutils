import base64
import json
from cryptography.fernet import Fernet

FERNET_KEY_LEN = 32

def create_fernet(key):
    key_len = len(key)
    if key_len < FERNET_KEY_LEN:
        key += '0' * (FERNET_KEY_LEN - key_len)
    key = base64.urlsafe_b64encode(key[:32].encode())
    return Fernet(key)

def dumps(data, key):
    f = create_fernet(key)
    data = json.dumps(data, separators=(',', ':')).encode('latin-1')
    return f.encrypt(data)

def loads(data, key):
    f = create_fernet(key)
    data = f.decrypt(data)
    return json.loads(data.decode('latin-1'))
