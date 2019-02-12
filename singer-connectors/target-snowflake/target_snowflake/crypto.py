import os
import singer
import struct
import binascii

from Crypto.Cipher import AES
from Crypto import Random

logger = singer.get_logger()


class Crypto(object):
    """

    """
    def __init__(self, master_key, mode = AES.MODE_CBC, chunk_size = 16 * 1024):
        self._master_key = master_key.encode("utf8")
        self._mode = mode
        self._chunk_size = chunk_size
        self._iv = None
        self._cipher = None

    @property
    def master_key(self):
        return self._master_key

    @property
    def mode(self):
        return self._mode

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
        self._master_key = value.encode("utf8")

    @mode.setter
    def mode(self, value):
        self._mode = value

    @chunk_size.setter
    def chunk_size(self, value):
        self._chunk_size = value

    @iv.setter
    def iv(self, value):
        self.reset_cipher(value)


    @staticmethod
    def finalise_wrk_file(wrk_filename, out_filename, in_filename):
        if out_filename:
            os.rename(wrk_filename, out_filename)
        else:
            os.rename(wrk_filename, in_filename)


    @staticmethod
    def get_work_filename(filename):
        return "{}.wrk".format(filename)


    def reset_cipher(self, iv = None):
        if iv:
            self._iv = iv
        else:
            self._iv = Random.new().read(AES.block_size)

        # Generate a new cipher with initialisation vector
        self._cipher = AES.new(self._master_key, self._mode, self._iv)


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
                    length_to_pad = 16 - (last_chunk_length % 16)
                    chunk += struct.pack('B', length_to_pad) * length_to_pad
                    w.write(func(chunk))

                # Remove extra padding when decrypting
                elif func == self._cipher.decrypt:
                    decrypted_last_chunk = func(chunk)
                    last_byte = decrypted_last_chunk[-1]
                    if last_byte <= 16 and len(set(decrypted_last_chunk[-last_byte:])) == 1:
                        w.write(decrypted_last_chunk[:-last_byte])
                    else:
                        w.write(decrypted_last_chunk)

        # Move the newly created wrk file to the expected location
        self.finalise_wrk_file(wrk_filename, out_filename, in_filename)


    def encrypt_file(self, in_filename, out_filename=None):
        logger.info("Client Side Encryption - encrypting file {}...".format(in_filename))

        # Reset cipher with no IV, encryption method will generate a new one
        self.reset_cipher()
        self.do_in_chunks(self._cipher.encrypt, in_filename, out_filename)

        # Return the initialization vector, that will be needed to decrypt
        return self._iv


    def decrypt_file(self, in_filename, iv, out_filename=None):
        logger.info("Client Side Encryption - decrypting file {}...".format(in_filename))

        # Reset cipher with IV that used during the encryption
        self.reset_cipher(iv)
        self.do_in_chunks(self._cipher.decrypt, in_filename, out_filename)
