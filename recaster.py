import os
import sys
import base64
import hashlib

from typing import List
from collections.abc import Callable

def _xor_inplace(data: bytearray, hash: bytes) -> None:
    """XOR data in place with a cyclically-repeating key, vectorized via translate()."""
    ln = len(hash)
    tables = [bytes(b ^ k for b in range(256)) for k in hash]
    for p in range(ln):
        data[p::ln] = data[p::ln].translate(tables[p])

class Recaster():
    def __init__(self, hashlvl: int, passphrase: str | None = None):
        if passphrase:
            # the hashObj is derived from the provided passphrase
            self.passphrase = passphrase.encode('utf-8')

            self.hasher: Callable[[bytes], bytes] = None
            match hashlvl:
                case 1:
                    self.hasher = hashlib.sha1
                case 256:
                    self.hasher = hashlib.sha256
                case 384:
                    self.hasher = hashlib.sha384
                case 512:
                    self.hasher = hashlib.sha512
                case _:
                    raise Exception(f"Hash level {hashlvl} is not supported!")

            self.hashObj = self.hasher(self.passphrase)

    def encrypt(self, chunkPath: str) -> bool:
        """
        Encrypt the data using Fernet in a backup chunk "in place" on disk.

        Encrypted data does not support comparisons or snapshots.
        """
        try:
            from cryptography.fernet import Fernet
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
        except ModuleNotFoundError:
            raise Exception("Encryption support requires the 'cryptography' package!")

        salt = self.hashObj.digest()

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=2_000_000,
        )

        with open(chunkPath, 'rb') as f:
            data = f.read()

        key = base64.urlsafe_b64encode(kdf.derive(self.passphrase))
        f = Fernet(key)
        token = f.encrypt(data)

        path, filename = os.path.split(chunkPath)
        basename, ext = os.path.splitext(filename)
        ext = '.enc'
        encPath = os.path.join(path, f"{basename}{ext}")

        try:
            with open(encPath, 'wb') as f:
                f.write(token)
        except Exception as e:
            sys.stderr.write(f"Failed to write encrypted file {encPath}:\n  '{e}'!\n")
            sys.exit(1)

        os.remove(chunkPath)

        return True

    def decrypt(self, encPath: str) -> bytes:
        """
        Decrypt the data encrypted using Fernet.
        Encrypted data does not support comparisons or snapshots.
        """
        try:
            from cryptography.fernet import Fernet
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
        except ModuleNotFoundError:
            raise Exception("Encryption support requires the 'cryptography' package!")

        salt = self.hashObj.digest()

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=2_000_000,
        )

        with open(encPath, 'rb') as f:
            token = f.read()

        key = base64.urlsafe_b64encode(kdf.derive(self.passphrase))
        fernet = Fernet(key)
        return fernet.decrypt(token)

    def obfuscate(self, chunkPath: str) -> bool:
        """
        Why obfuscate instead of using traditional encryption?  Encryption methods (e.g., Fernet)
        may add overhead to a file, resulting in a size that will almost always be greater or smaller
        than the original file; even if it ended up matching the size, it is highly unlikely to have
        a 1:1 byte mapping with the partition from whence it came.  While obfuscation may not be as
        technically secure, it does have the distinct side effect of producing "encrypted" files that
        are the same byte-for-byte size as the original.

        Why would this be useful?  Oh, well, let's imagine that perhaps you want to harden your
        partitions against imaging by government representatives at a border crossing.  With traditional
        encryption, you could not simply overlay the "encrypted" backup directly onto your partitions
        losslessly because of the size discrepancy.  With obfuscation, there's no overhead, so it's a
        1:1 mapping to the bytes in the partition.

        Steps to obfuscate a partition:
        1. Backup your partition using the "obfuscate" option.
        2. Immediately perform a "restore" of that snapshot (WITHOUT using "clarify") to the same partition.
          2a. If you can be reasonably assured that nothing will happen to the data you just restored,
              you can delete your backup files--the exact same data is now contained in the partition.

        Steps to recover an obfuscated partition:
        1. Backup your partition (WITHOUT using "obfuscation"; i.e., a standard snapshot backup).
        2. Immediately perform a "restore" of that snapshot using the "clarify" option, using the same
           passphrase (and hash level) you used to originally back it up in obfuscation mode.

        Once you've resonstituted the obfuscated data to your partition using the "clarify" option,
        your partition is once again in a functional, humanly usable state.  I have personally tested
        this process, even gonig so far as to obfuscate my "EFI" partition, with accurate recovery.

        In any case, MAKE SURE YOU HAVE BACKUPS OF YOUR DATA.  If you misstep, it's entirely
        possible you can render things irretrievable.

        Protege te.
        """
        with open(chunkPath, 'rb') as f:
            data: bytearray = bytearray(f.read())

        # our hash value starts out derived from the provided passphrase.  however,
        # each subsequent chunk provided adds its unique hash to this cumulative value,
        # creating a rolling-chain key where each stand-alone chunk cannot (hope
        # to) be recovered without information calculated from the preceeding chunks in
        # the same order they were processed.
        nextHash = self.hasher(data).digest()

        hash = self.hashObj.digest()
        _xor_inplace(data, hash)

        path, filename = os.path.split(chunkPath)
        basename, ext = os.path.splitext(filename)
        ext = '.obf'
        obfPath = os.path.join(path, f"{basename}{ext}")

        try:
            with open(obfPath, 'wb') as f:
                f.write(data)
        except Exception as e:
            sys.stderr.write(f"Failed to write obfuscated file {obfPath}:\n  '{e}'!\n")
            sys.exit(1)

        os.remove(chunkPath)

        self.hashObj.update(nextHash)

        return True

    def clarify(self, chunkPath: str) -> bytes:
        with open(chunkPath, 'rb') as f:
            data: bytearray = bytearray(f.read())

        hash = self.hashObj.digest()
        _xor_inplace(data, hash)

        # calculate the next hash AFTER we reconstitute a chunk
        nextHash = self.hasher(data).digest()
        self.hashObj.update(nextHash)

        return bytes(data)
