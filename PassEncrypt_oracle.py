# AES encryption for password
# http://stackoverflow.com/questions/12524994/encrypt-decrypt-using-pycrypto-aes-256

# FYI - If you are using Python 3+, you will need to download the package 'pycryptodome'
# to be able to run this script! 
# If using Python 2.7 - download the package 'pycrypto' -- E. Willmot, 10/3/2018

import base64
import getpass
from Crypto.Cipher import AES
import os
import re

from os.path import expanduser
home = expanduser("~")

class AESCipher:

    def __init__(self, key):
        self.key = key

    def encrypt(self, raw):
        """Encrypt the raw text"""
        while len(raw) % 16 != 0:
            raw = raw + str('|')
        raw = raw.encode("utf-8")
        iv = os.urandom(AES.block_size)
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return base64.b64encode(iv + cipher.encrypt(raw))

    def decrypt(self, enc, key):
        enc = base64.b64decode(enc)
        iv = enc[:16]
        cipher = AES.new(key, AES.MODE_CBC, iv)
        raw = cipher.decrypt(enc[16:]).decode()
        decrypted = re.sub('\\|', '', raw)
        return decrypted


def make_new_pass():
    """Creates a new key and cipher text based on the user supplied password.
    Call this function to create a new key+ciphertext pair"""
    pass_text = getpass.getpass('Enter your password:')
    key = os.urandom(32)
    cipher = AESCipher(key)
    cipher_text = cipher.encrypt(pass_text)

    # Write the key and ciper text to H drive
    with open(home + '\\python_key_oracle.key', 'wb') as k:
        k.write(key)
    with open('H:\\python_key_oracle.creds', 'wb') as k:
        k.write(cipher_text)


def get_pass():
    """ Read the key and cipher text and return the decoded password"""
    with open(home + '\\python_key_oracle.key', 'rb') as k:
        key = k.read()
    with open('H:\\python_key_oracle.creds', 'rb') as k:
        cipher_text = k.read()

    cipher = AESCipher(key)
    password = cipher.decrypt(cipher_text, key)
    return password

