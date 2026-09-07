"""Read and write a value sealed the way pkg/tritium's Go client seals it,
using nothing but redis-py and the `cryptography` package — proof that the
format is a wire contract, not a Go-only trick.

pkg/tritium/crypto.go: KeyFromPassphrase derives a 32-byte key with
PBKDF2-HMAC-SHA256 at 600,000 iterations; the sealed value is

    "TE1" || 12-byte nonce || AES-256-GCM(plaintext, aad = key name)

with the 16-byte GCM tag appended to the ciphertext as usual. The key name is
authenticated but not encrypted, so a value can't be replayed under another
key without the seal failing to open.

    pip install redis cryptography
    go run ./cmd/tritium              # from the repo root, in another shell
    python3 examples/python/sealed.py
"""

import hashlib
import os

import redis
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

MAGIC = b"TE1"
NONCE_SIZE = 12


def key_from_passphrase(passphrase: str, salt: str) -> bytes:
    """Same derivation as tritium.KeyFromPassphrase; passphrase and salt must
    match on every client sharing the data."""
    return hashlib.pbkdf2_hmac("sha256", passphrase.encode(), salt.encode(), 600_000, dklen=32)


def seal(aesgcm: AESGCM, key_name: str, plaintext: bytes) -> bytes:
    nonce = os.urandom(NONCE_SIZE)
    return MAGIC + nonce + aesgcm.encrypt(nonce, plaintext, key_name.encode())


def unseal(aesgcm: AESGCM, key_name: str, data: bytes) -> bytes:
    if data[:3] != MAGIC:
        raise ValueError("not a tritium-sealed value")
    nonce, ciphertext = data[3 : 3 + NONCE_SIZE], data[3 + NONCE_SIZE :]
    return aesgcm.decrypt(nonce, ciphertext, key_name.encode())


if __name__ == "__main__":
    key = key_from_passphrase("correct horse battery staple", "my-app")
    aesgcm = AESGCM(key)

    r = redis.Redis(host="localhost", port=8080, password="change-me")
    r.set("hello", seal(aesgcm, "hello", b"world"), ex=3600)

    raw = r.get("hello")
    print(raw)  # b"TE1..." — ciphertext; the node never saw the plaintext
    print(unseal(aesgcm, "hello", raw))  # b"world"
