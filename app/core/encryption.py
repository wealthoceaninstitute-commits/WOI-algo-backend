"""
AES-256-GCM encryption for sensitive credential fields.
Each value gets its own random nonce — safe to store in DB.
"""
import os
import base64
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from app.core.config import get_settings


def _get_key() -> bytes:
    key_hex = get_settings().ENCRYPTION_KEY
    key_bytes = bytes.fromhex(key_hex)
    if len(key_bytes) != 32:
        raise ValueError("ENCRYPTION_KEY must be 64 hex chars (32 bytes)")
    return key_bytes


def encrypt(plaintext: str) -> str:
    """Encrypt a string and return base64-encoded nonce+ciphertext."""
    key = _get_key()
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)  # 96-bit nonce
    ct = aesgcm.encrypt(nonce, plaintext.encode(), None)
    return base64.b64encode(nonce + ct).decode()


def decrypt(token: str) -> str:
    """Decrypt a base64-encoded nonce+ciphertext string."""
    key = _get_key()
    aesgcm = AESGCM(key)
    data = base64.b64decode(token.encode())
    nonce, ct = data[:12], data[12:]
    return aesgcm.decrypt(nonce, ct, None).decode()
