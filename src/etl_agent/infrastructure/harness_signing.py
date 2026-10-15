"""Ed25519 signing infrastructure for capability tokens and detached signatures.

This module provides cryptographic primitives using PyNaCl (libsodium) for:
- Ed25519 keypair generation and management
- Detached signature creation and verification
- Compact capability token issuance and verification

All operations follow the principle of least privilege: signers hold private keys
only at authorization boundaries, while verifiers work with public keys only.
"""

import base64
import binascii
import json
import time
from typing import Any

try:
    from nacl.signing import SigningKey, VerifyKey
except ImportError as exc:
    raise ImportError(
        "PyNaCl is required for Ed25519 signing. Install it with: uv add pynacl"
    ) from exc

from etl_agent.domain.errors import DomainError
from etl_agent.domain.harness import DetachedSignature


class Ed25519Signer:
    """Holds a single Ed25519 private key; only signs at authorization or policy boundaries."""

    def __init__(self, private_key_bytes: bytes, key_id: str = "default") -> None:
        """Initialize with raw 32-byte private key bytes.

        Args:
            private_key_bytes: Raw 32-byte Ed25519 private key material.
            key_id: Stable identifier for this key (e.g., "auth-service-v1").

        Raises:
            ValueError: If private_key_bytes is not exactly 32 bytes.
        """
        if len(private_key_bytes) != 32:
            raise ValueError(f"Private key must be 32 bytes, got {len(private_key_bytes)}")
        if not key_id.strip():
            raise ValueError("key_id cannot be empty")
        self._signing_key = SigningKey(private_key_bytes)
        self.key_id = key_id.strip()

    @classmethod
    def generate(cls) -> "Ed25519Signer":
        """Generate a new Ed25519 keypair for local development or testing.

        Production keys must come from Vault/KMS.

        Returns:
            New Ed25519Signer instance with freshly generated keypair.
        """
        signing_key = SigningKey.generate()
        return cls(signing_key.encode())

    def sign(self, message: bytes) -> bytes:
        """Sign a message with the private key.

        Args:
            message: The message bytes to sign.

        Returns:
            64-byte Ed25519 signature.
        """
        signed_message = self._signing_key.sign(message)
        # Extract just the signature (last 64 bytes), not the signed message
        return signed_message.signature

    def get_public_key(self) -> bytes:
        """Return the public key as raw 32 bytes.

        Returns:
            32-byte Ed25519 public key material.
        """
        return bytes(self._signing_key.verify_key)

    def sign_detached(self, payload: bytes) -> DetachedSignature:
        """Create a detached Ed25519 signature for the given payload.

        Args:
            payload: The message bytes to sign.

        Returns:
            DetachedSignature containing algorithm, key_id, and base64url-encoded signature.
        """
        signature_bytes = self.sign(payload)
        return DetachedSignature(
            algorithm="Ed25519",
            key_id=self.key_id,
            value=_base64url_encode(signature_bytes),
        )


class Ed25519PublicKeyRing:
    """Read-only ring of public keys for signature verification.

    Verifiers cannot issue new tokens or forge audit checkpoints.
    """

    def __init__(self) -> None:
        """Initialize an empty public key ring."""
        self._keys: dict[str, VerifyKey] = {}

    def add_key(self, key_id: str, public_key_bytes: bytes) -> None:
        """Add a public key to the ring.

        Args:
            key_id: Stable identifier for the key (e.g., "auth-service-v1").
            public_key_bytes: Raw 32-byte Ed25519 public key material.

        Raises:
            ValueError: If key_id is empty or public_key_bytes is not 32 bytes.
        """
        if not key_id.strip():
            raise ValueError("key_id cannot be empty")
        if len(public_key_bytes) != 32:
            raise ValueError(f"Public key must be 32 bytes, got {len(public_key_bytes)}")
        self._keys[key_id.strip()] = VerifyKey(public_key_bytes)

    def verify(self, key_id: str, message: bytes, signature: bytes) -> bool:
        """Verify a signature against a message using the specified public key.

        Args:
            key_id: The key identifier to look up in the ring.
            message: The original message that was signed.
            signature: The 64-byte Ed25519 signature to verify.

        Returns:
            True if the signature is valid.

        Raises:
            DomainError: If key_id is not found or signature verification fails.
        """
        verify_key = self._keys.get(key_id)
        if verify_key is None:
            raise DomainError(f"Unknown key ID: {key_id}")

        try:
            verify_key.verify(message, signature)
            return True
        except Exception as exc:
            raise DomainError("Signature verification failed") from exc


class CapabilityTokenIssuer:
    """Issues short-lived, one-shot, minimal-privilege capability tokens."""

    def __init__(self, signer: Ed25519Signer, key_id: str) -> None:
        """Bind to a signer and its stable key ID.

        Args:
            signer: The Ed25519 signer holding the private key.
            key_id: Stable key identifier included in token headers.
        """
        self._signer = signer
        self._key_id = key_id

    def issue(self, claims: dict[str, Any], expires_in_seconds: int = 300) -> str:
        """Issue a compact JWT-like capability token.

        Token format: header.payload.signature (base64url-encoded, no padding)

        Header: {"alg": "EdDSA", "kid": key_id}
        Payload: {...claims, "exp": timestamp, "iat": timestamp}
        Signature: Ed25519(header.payload)

        Args:
            claims: Arbitrary claim data to include in the token payload.
            expires_in_seconds: Token lifetime in seconds (default 300s / 5 min).

        Returns:
            Compact token string in format: header.payload.signature

        Raises:
            ValueError: If expires_in_seconds is negative or exceeds 900 seconds (15 min).
        """
        if expires_in_seconds <= 0:
            raise ValueError("expires_in_seconds must be positive")
        if expires_in_seconds > 900:
            raise ValueError("Token lifetime cannot exceed 15 minutes (900 seconds)")

        now = int(time.time())

        # Build header
        header = {"alg": "EdDSA", "kid": self._key_id}
        header_json = json.dumps(header, separators=(",", ":")).encode("utf-8")
        header_encoded = _base64url_encode(header_json)

        # Build payload with temporal claims
        payload = {**claims, "iat": now, "exp": now + expires_in_seconds}
        payload_json = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        payload_encoded = _base64url_encode(payload_json)

        # Sign the header.payload portion
        signing_input = f"{header_encoded}.{payload_encoded}".encode("ascii")
        signature = self._signer.sign(signing_input)
        signature_encoded = _base64url_encode(signature)

        return f"{header_encoded}.{payload_encoded}.{signature_encoded}"


class CapabilityTokenVerifier:
    """Verifies capability tokens using only public keys.

    Checks signature, algorithm, key ID, and expiration with configurable clock skew.
    """

    def __init__(self, key_ring: Ed25519PublicKeyRing, clock_skew_seconds: int = 30) -> None:
        """Initialize with a public key ring and acceptable clock skew.

        Args:
            key_ring: Ring of trusted public keys for verification.
            clock_skew_seconds: Acceptable clock drift in seconds (0-60, default 30).

        Raises:
            ValueError: If clock_skew_seconds is outside 0-60 range.
        """
        if clock_skew_seconds < 0 or clock_skew_seconds > 60:
            raise ValueError("clock_skew_seconds must be between 0 and 60 seconds")
        self._key_ring = key_ring
        self._clock_skew = clock_skew_seconds

    def verify(self, token: str) -> dict[str, Any]:
        """Verify and parse a capability token.

        Validates:
        - Token format (three dot-separated parts)
        - Header algorithm and type
        - Key ID presence
        - Signature validity
        - Token expiration (with clock skew tolerance)

        Args:
            token: Compact token string in format header.payload.signature

        Returns:
            Decoded claims dictionary if token is valid.

        Raises:
            DomainError: If token is invalid, expired, or signature verification fails.
        """
        parts = token.split(".")
        if len(parts) != 3:
            raise DomainError("Capability token format is invalid")

        try:
            header_json = _base64url_decode(parts[0])
            payload_json = _base64url_decode(parts[1])
            signature = _base64url_decode(parts[2])

            header = json.loads(header_json)
            payload = json.loads(payload_json)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise DomainError("Capability token encoding is invalid") from exc

        # Validate header
        if not isinstance(header, dict):
            raise DomainError("Capability token header is invalid")
        if header.get("alg") != "EdDSA":
            raise DomainError("Capability token algorithm is not supported")

        key_id = header.get("kid")
        if not isinstance(key_id, str) or not key_id:
            raise DomainError("Capability token is missing key ID")

        # Verify signature
        signing_input = f"{parts[0]}.{parts[1]}".encode("ascii")
        self._key_ring.verify(key_id=key_id, message=signing_input, signature=signature)

        # Validate payload structure
        if not isinstance(payload, dict):
            raise DomainError("Capability token payload is invalid")

        # Check temporal claims
        now = time.time()
        iat = payload.get("iat")
        exp = payload.get("exp")

        if not isinstance(iat, (int, float)) or not isinstance(exp, (int, float)):
            raise DomainError("Capability token is missing temporal claims")

        # Check if token was issued in the future (beyond clock skew)
        if iat > now + self._clock_skew:
            raise DomainError("Capability token was issued in the future")

        # Check if token has expired (with clock skew tolerance)
        if exp <= now - self._clock_skew:
            raise DomainError("Capability token has expired")

        return payload


def generate_test_keypair() -> tuple[bytes, bytes]:
    """Generate an Ed25519 keypair for testing purposes.

    Returns:
        Tuple of (private_key_bytes, public_key_bytes), each 32 bytes.

    Note:
        Production keys must come from Vault/KMS. This function is for
        local development and testing only.
    """
    signer = Ed25519Signer.generate()
    return signer._signing_key.encode(), signer.get_public_key()


def _base64url_encode(value: bytes) -> str:
    """Encode bytes to URL-safe Base64 without padding.

    Args:
        value: Bytes to encode.

    Returns:
        Base64URL-encoded string without padding characters.
    """
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _base64url_decode(value: str) -> bytes:
    """Decode URL-safe Base64 string (with or without padding) to bytes.

    Args:
        value: Base64URL-encoded string.

    Returns:
        Decoded bytes.

    Raises:
        ValueError: If the input contains invalid characters.
    """
    if not value or any(
        character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        for character in value
    ):
        raise ValueError("Invalid Base64URL content")

    # Add padding if necessary
    padding = "=" * (-len(value) % 4)
    try:
        return base64.b64decode(value + padding, altchars=b"-_", validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("Invalid Base64URL content") from exc
