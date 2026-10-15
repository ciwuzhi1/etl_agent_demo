"""Side Effect Harness - minimal skeleton for Chapter 11."""

import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.domain.harness import (
    CapabilityClaims,
)
from etl_agent.domain.side_effects import (
    SideEffectOperation,
)
from etl_agent.infrastructure.harness_signing import (
    Ed25519PublicKeyRing,
)


class SideEffectHarnessService:
    """Unified non-Pipeline side effect Prepare, approval, Commit, Outbox & Worker validation."""

    def __init__(
        self,
        *,
        decision_point: Any,
        signer: Any,
        token_issuer: Any,
        token_verifier: Any,
        environment: str,
        policy_bundle_version: str,
        capability_ttl_seconds: int = 300,
        access: Any | None = None,
    ) -> None:
        """Inject signing, policy and environment; service does not persist credential bodies or replayable Tokens."""
        pass

    async def prepare(
        self,
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        actor_id: uuid.UUID,
        operation: SideEffectOperation,
        aggregate_type: str,
        aggregate_id: uuid.UUID,
        idempotency_key: str,
        frozen_facts: dict[str, Any],
        command_payload: dict[str, Any],
        resource_scopes: list[Any] | None = None,
        trace_id: str | None = None,
    ) -> Any:
        """Freeze business fact summary and command, call PDP then create immutable side effect Preparation."""
        return "stub-preparation-id"

    async def decide(
        self,
        session: AsyncSession,
        *,
        preparation_id: uuid.UUID,
        actor_id: uuid.UUID,
        decision: Any,
        reason: str,
        role: Any | None = None,
        trace_id: str | None = None,
    ) -> Any:
        """Occupy one duty approval slot; P0 two slots must be completed by different natural persons."""
        pass

    async def commit(
        self,
        session: AsyncSession,
        *,
        preparation_id: uuid.UUID,
        actor_id: uuid.UUID,
        trace_id: str | None = None,
        inline_callback: Callable[[], Awaitable[uuid.UUID]] | None = None,
    ) -> Any:
        """Issue and consume one-time capability, create Outbox or execute managed Secret staging in same transaction."""
        return "stub-execution-id"


async def claim_committed_side_effect(
    session: AsyncSession,
    *,
    operation: SideEffectOperation,
    aggregate_id: uuid.UUID,
    command_payload: dict[str, Any],
    key_ring: Ed25519PublicKeyRing,
    now: datetime | None = None,
    execution_lease_seconds: int = 300,
) -> dict:
    """Worker verifies signature and leases side effect before external call, preventing concurrent replay of valid messages."""
    return {}


async def complete_side_effect(
    session: AsyncSession,
    *,
    operation: SideEffectOperation,
    aggregate_id: uuid.UUID,
    result: dict[str, Any],
    trace_id: str | None = None,
) -> None:
    """Mark idempotent external result summary as succeeded and append evidence event without secrets."""
    pass


async def fail_side_effect(
    session: AsyncSession,
    *,
    operation: SideEffectOperation,
    aggregate_id: uuid.UUID,
    error: str,
    trace_id: str | None = None,
) -> None:
    """Record final failed redacted summary, preserving Preparation and authorization for audit and manual retry."""
    pass
