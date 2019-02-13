import os
import singer
import struct
import base64
import binascii
from collections import namedtuple

from Crypto.Cipher import AES
from Crypto import Random

logger = singer.get_logger()

PKCS5_UNPAD = lambda v: v[0:-v[-1]]
def PKCS5_PAD(value, block_size):
        return b"".join(
            [value, (block_size - len(value) % block_size) * chr(
                block_size - len(value) % block_size).encode(u'utf-8')])

EncryptionMetadata = namedtuple(
    "EncryptionMetadata", [
        "key",
        "iv",
    ]
)


class Crypto:
    """
    Cryptography library for target-snowflake to do Client Side Encryption and to
    achieve End-to-End encryption.

    End-to-end encryption is a form of communication where only the end users can
    read the data, but nobody else. For the Snowflake data warehouse service it means
    that only the customer and runtime components of the Snowflake service can read
    the data. No third parties, including Amazon AWS and any ISPs, can see data in
    the clear. This makes end-to-end encryption the most secure way to communicate
    with the Snowflake data warehouse service.

    :type master_key: string
    :param master_key: Master Key
    :param mode: Encryption mode, use AES.MODE_CDB by default
    :type chunk_size: int
    :param chunk_size: Size of chunks to encrypt/decrypt in one go
    """
    def __init__(self, master_key, chunk_size = AES.block_size * 4 * 1024):
        self._master_key = base64.standard_b64decode(master_key)
        self._chunk_size = chunk_size
        self._iv = None
        self._cipher = None

    @property
    def master_key(self):
        return self._master_key

    @property
    def chunk_size(self):
        return self._chunk_size

    @property
    def iv(self):
        return self._iv

    @property
    def cipher(self):
        return self._cipher

    @master_key.setter
    def master_key(self, value):
        self._master_key = value

    @chunk_size.setter
    def chunk_size(self, value):
        self._chunk_size = value


    @staticmethod
    def finalise_wrk_file(wrk_filename, out_filename, in_filename):
        if out_filename:
            os.rename(wrk_filename, out_filename)
        else:
            os.rename(wrk_filename, in_filename)


    @staticmethod
    def get_work_filename(filename):
        return "{}.wrk".format(filename)


    @staticmethod
    def get_secure_random(byte_length):
        return Random.new().read(byte_length)


    def reset_cipher(self, key, mode, iv=None):
        # Generate a new cipher with initialisation vector for CBC mode
        if mode == AES.MODE_CBC:
            if iv:
                self._iv = iv
            else:
                self._iv = self.get_secure_random(AES.block_size)

            self._cipher = AES.new(key, mode, self._iv)
        # Don't use iv for other mode
        else:
            self._cipher = AES.new(key, mode)


    def do_in_chunks(self, func, in_filename, out_filename):
        # Generate a work file that will be used as the intermediate output
        wrk_filename = self.get_work_filename(in_filename)

        # Encrypt or decrypt (func) in chunks
        with open(in_filename, 'rb') as infile:
            with open(wrk_filename, 'wb') as w:
                last_chunk_length = 0
                while True:
                    chunk = infile.read(self._chunk_size)
                    last_chunk_length = len(chunk)
                    if last_chunk_length == 0 or last_chunk_length < self._chunk_size:
                        break
                    w.write(func(chunk))

                # AES CBC Padding: https://security.stackexchange.com/questions/29993
                # 
                # Padding is a way to encrypt messages of a size that the block cipher
                # would not be able to decrypt otherwise; it is a convention between
                # whoever encrypts and whoever decrypts. If your input messages always
                # have a length which can be processed with your encryption mode
                # (e.g. your messages always have a length multiple of 16) then you do
                # not have to add padding -- as long as during decryption, you do not
                # try to look for a padding when there is none. If some of your messages
                # require padding, then you will have to add some sort of padding
                # systematically, otherwise decryption will be ambiguous.
                # Write extra padding when encrypting
                if func == self._cipher.encrypt:
                    chunk = PKCS5_PAD(chunk, AES.block_size)
                    w.write(func(chunk))

                # Remove extra padding when decrypting
                elif func == self._cipher.decrypt:
                    decrypted_last_chunk = func(chunk)
                    last_byte = decrypted_last_chunk[-1]
                    if last_byte <= AES.block_size and len(set(decrypted_last_chunk[-last_byte:])) == 1:
                        w.write(decrypted_last_chunk[:-last_byte])
                    else:
                        w.write(decrypted_last_chunk)

        # Move the newly created wrk file to the expected location
        self.finalise_wrk_file(wrk_filename, out_filename, in_filename)


    def encrypt_file(self, in_filename, out_filename=None):
        logger.info("Client Side Encryption - encrypting file {}...".format(in_filename))
        key_size = len(self._master_key)

        # Generate key for data encryption
        file_key = self.get_secure_random(key_size)

        # Encrypt data: reset cipher with no iv. IV will be generated
        self.reset_cipher(key=file_key, mode=AES.MODE_CBC)
        self.do_in_chunks(self._cipher.encrypt, in_filename, out_filename)

        # Encrypt key with QRMK
        self.reset_cipher(key=self._master_key, mode=AES.MODE_ECB)
        enc_kek = self._cipher.encrypt(PKCS5_PAD(file_key, AES.block_size))

        # Return the encryption key and initialization vector, that will be needed to decrypt
        return EncryptionMetadata(
            key=base64.b64encode(enc_kek).decode('utf-8'),
            iv=base64.b64encode(self._iv).decode('utf-8'),
        )


    def decrypt_file(self, in_filename, metadata, out_filename=None):
        logger.info("Client Side Encryption - decrypting file {}...".format(in_filename))

        # Extract the key and iv from metadata that requires to decrypt
        key = base64.standard_b64decode(metadata.key)
        iv = base64.standard_b64decode(metadata.iv)

        # Decrypt key that requires to decrypt data
        self.reset_cipher(key=self._master_key, mode=AES.MODE_ECB)
        file_key = PKCS5_UNPAD(self._cipher.decrypt(key))

        # Decrypt file
        self.reset_cipher(key=file_key, mode=AES.MODE_CBC, iv=iv)
        self.do_in_chunks(self._cipher.decrypt, in_filename, out_filename)
