"""Versioned encrypted credentials; deployment holds the key outside PostgreSQL."""
import base64
import binascii
import json
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class SecretProvider:
    def __init__(self):
        try:
            key = base64.b64decode(os.environ['SECRET_MASTER_KEY'], validate=True)
        except (KeyError, ValueError, binascii.Error):
            raise RuntimeError('必须部署注入有效的 SECRET_MASTER_KEY（Base64 32字节），不可临时生成') from None
        if len(key) != 32:
            raise RuntimeError('SECRET_MASTER_KEY 必须为 Base64 编码的32字节主密钥')
        self.cipher = AESGCM(key)

    def seal(self, reference, plaintext):
        nonce = os.urandom(12)
        return nonce, self.cipher.encrypt(nonce, plaintext.encode('utf-8'), self.context(reference))

    def open(self, reference):
        return self.cipher.decrypt(reference.nonce, reference.ciphertext, self.context(reference)).decode('utf-8')

    @staticmethod
    def context(reference):
        return json.dumps([reference.tenant_id, reference.project_id, reference.id, reference.version],
                          separators=(',', ':')).encode('utf-8')
