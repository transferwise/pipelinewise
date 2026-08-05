"""Credential conversion helpers owned by the data-diff project."""

from cryptography.hazmat.primitives import serialization


def pem_to_der(pem_file: str, password: str = None) -> bytes:
    """Convert a PEM private key to unencrypted PKCS8 DER bytes."""
    with open(pem_file, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=password,
        )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
