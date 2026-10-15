"""第 12 章单元测试：副作用治理的冻结、审批分离、一次性能力与 Worker 租约。

覆盖：
- prepare 冻结事实（facts_sha256）并创建 WAITING_APPROVAL/execution PENDING 准备单；
- PDP DENY 阻断副作用；
- decide 的职责分离（申请人不自批、同一自然人不占两槽）；
- commit 只在审批齐备后签发一次性能力令牌并创建 Outbox 命令；
- claim/complete/fail 的租约与幂等语义；
- API 端点端到端（prepare → decide → commit）。
"""

import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.harness_policy import EmbeddedPolicyDecisionPoint
from etl_agent.application.side_effect_harness import (
    SideEffectHarnessService,
    _freeze_facts_hash,
    claim_committed_side_effect,
    complete_side_effect,
    fail_side_effect,
)
from etl_agent.domain.enums import ProjectRole
from etl_agent.domain.errors import PermissionDenied, VersionConflict
from etl_agent.domain.harness import (
    HarnessApprovalValue,
    HarnessRole,
    PolicyDecision,
    PolicyEvaluationRequest,
    PolicyOutcome,
    PolicyRuleMatch,
    RiskLevel,
)
from etl_agent.domain.side_effects import (
    SideEffectExecutionStatus,
    SideEffectOperation,
    SideEffectPreparationStatus,
)
from etl_agent.infrastructure.database import get_session
from etl_agent.infrastructure.models import (
    HarnessCommandOutbox,
    HarnessSideEffectCapabilityGrant,
    HarnessSideEffectPreparation,
    Project,
    ProjectMembership,
    ProjectRoleGrant,
    User,
)
from etl_agent.main import app


class _DenyPDP:
    """恒定返回 DENY 的测试 PDP，验证副作用门禁不可被审批翻案。"""

    async def evaluate(self, request: PolicyEvaluationRequest) -> PolicyDecision:
        """任何意图都判 P0 拒绝。"""
        return PolicyDecision(
            id="pol-test-deny",
            project_id=request.intent.project_id,
            actor_id=request.intent.actor_id,
            outcome=PolicyOutcome.DENY,
            risk_level=RiskLevel.P0,
            policy_bundle_version=request.context.policy_bundle_version,
            request_fingerprint="0" * 64,
            rule_matches=[
                PolicyRuleMatch(
                    rule_id="TEST-DENY-ALL",
                    rule_version="1",
                    outcome=PolicyOutcome.DENY,
                    risk_level=RiskLevel.P0,
                    message="测试策略拒绝所有副作用",
                )
            ],
        )


@dataclass
class _Fixtures:
    """一个项目加四类成员：工程师（申请人）、双职责人、单职责运维、无审批职责者。"""

    project: Project
    engineer: User
    multi: User
    operator: User
    outsider: User


async def _seed_project(session: AsyncSession) -> _Fixtures:
    """建项目、成员关系与可叠加 Harness 职责并落库。"""
    suffix = uuid.uuid4()
    project = Project(name=f"side-effects-{suffix}")
    engineer = User(subject=f"engineer-{suffix}", display_name="副作用工程师")
    multi = User(subject=f"multi-{suffix}", display_name="双职责审批人")
    operator = User(subject=f"operator-{suffix}", display_name="运维审批人")
    outsider = User(subject=f"outsider-{suffix}", display_name="无职责成员")
    session.add_all([project, engineer, multi, operator, outsider])
    await session.flush()
    session.add_all(
        [
            ProjectMembership(
                project_id=project.id,
                user_id=engineer.id,
                role=ProjectRole.DATA_ENGINEER,
            ),
            ProjectMembership(
                project_id=project.id,
                user_id=multi.id,
                role=ProjectRole.APPROVER,
            ),
            ProjectMembership(
                project_id=project.id,
                user_id=operator.id,
                role=ProjectRole.APPROVER,
            ),
            ProjectMembership(
                project_id=project.id,
                user_id=outsider.id,
                role=ProjectRole.VIEWER,
            ),
        ]
    )
    grants = [
        (engineer, HarnessRole.DATA_ENGINEER),
        (engineer, HarnessRole.OPERATOR),
        (multi, HarnessRole.DATA_ENGINEER),
        (multi, HarnessRole.DATA_APPROVER),
        (multi, HarnessRole.SECURITY_APPROVER),
        (operator, HarnessRole.OPERATOR),
        (operator, HarnessRole.DATA_APPROVER),
        (outsider, HarnessRole.AUDITOR),
    ]
    session.add_all(
        [
            ProjectRoleGrant(project_id=project.id, user_id=user.id, role=role)
            for user, role in grants
        ]
    )
    await session.commit()
    return _Fixtures(
        project=project,
        engineer=engineer,
        multi=multi,
        operator=operator,
        outsider=outsider,
    )


def _service() -> SideEffectHarnessService:
    """默认装配确定性内嵌 PDP 与进程内签名密钥的服务实例。"""
    return SideEffectHarnessService(pdp=EmbeddedPolicyDecisionPoint())


async def _prepare_cancellation(
    session: AsyncSession,
    fixtures: _Fixtures,
    *,
    service: SideEffectHarnessService | None = None,
    aggregate_id: uuid.UUID | None = None,
) -> HarnessSideEffectPreparation:
    """对 CANCEL_EXECUTION 发起一次标准准备，供多个测试复用。"""
    return await (service or _service()).prepare(
        session,
        project_id=fixtures.project.id,
        operation=SideEffectOperation.CANCEL_EXECUTION,
        aggregate_id=aggregate_id or uuid.uuid4(),
        context={"job_id": "job-42", "status": "RUNNING"},
        requested_by=fixtures.engineer.id,
    )


@pytest.mark.asyncio
async def test_prepare_freezes_facts_and_creates_pending_preparation(
    session: AsyncSession,
) -> None:
    """prepare 冻结 facts_sha256，审批状态 WAITING_APPROVAL、执行状态 PENDING。"""
    fixtures = await _seed_project(session)
    aggregate_id = uuid.uuid4()
    preparation = await _prepare_cancellation(session, fixtures, aggregate_id=aggregate_id)
    assert preparation.status is SideEffectPreparationStatus.WAITING_APPROVAL
    assert preparation.execution_status is SideEffectExecutionStatus.PENDING
    assert preparation.facts_sha256 == _freeze_facts_hash(
        SideEffectOperation.CANCEL_EXECUTION,
        aggregate_id,
        {"job_id": "job-42", "status": "RUNNING"},
    )
    assert preparation.required_roles == ["OPERATOR"]
    assert preparation.required_approvals == 1
    assert preparation.idempotency_key == f"cancel_execution:{aggregate_id}"
    # 冻结事实只落业务库不回显接口；载荷里没有令牌或凭据。
    assert preparation.frozen_facts == {"job_id": "job-42", "status": "RUNNING"}


@pytest.mark.asyncio
async def test_prepare_is_idempotent_and_rejects_fact_rebinding(
    session: AsyncSession,
) -> None:
    """同一幂等键重复 Prepare 返回同一行；换事实重用键必须冲突失败。"""
    fixtures = await _seed_project(session)
    aggregate_id = uuid.uuid4()
    first = await _prepare_cancellation(session, fixtures, aggregate_id=aggregate_id)
    second = await _prepare_cancellation(session, fixtures, aggregate_id=aggregate_id)
    assert first.id == second.id

    with pytest.raises(VersionConflict, match="冻结事实"):
        await _service().prepare(
            session,
            project_id=fixtures.project.id,
            operation=SideEffectOperation.CANCEL_EXECUTION,
            aggregate_id=uuid.uuid4(),
            context={"job_id": "job-99"},
            requested_by=fixtures.engineer.id,
            idempotency_key=first.idempotency_key,
        )


@pytest.mark.asyncio
async def test_prepare_denied_by_policy_blocks_preparation(
    session: AsyncSession,
) -> None:
    """PDP DENY 时准备单落 DENIED 终态并抛 403，审批与 Commit 都被阻断。"""
    fixtures = await _seed_project(session)
    service = SideEffectHarnessService(pdp=_DenyPDP())
    with pytest.raises(PermissionDenied, match="策略拒绝"):
        await _prepare_cancellation(session, fixtures, service=service)

    rows = list(
        (
            await session.scalars(
                select(HarnessSideEffectPreparation).where(
                    HarnessSideEffectPreparation.project_id == fixtures.project.id
                )
            )
        ).all()
    )
    assert len(rows) == 1
    denied = rows[0]
    assert denied.status is SideEffectPreparationStatus.DENIED
    assert denied.risk_level == "P0"
    assert denied.required_roles == []
    with pytest.raises(VersionConflict):
        await service.decide(
            session,
            preparation_id=denied.id,
            actor_id=fixtures.operator.id,
            role=HarnessRole.OPERATOR,
            decision=HarnessApprovalValue.APPROVE,
            reason="测试翻案",
        )
    with pytest.raises(VersionConflict):
        await service.commit(
            session,
            preparation_id=denied.id,
            committed_by=fixtures.engineer.id,
        )


@pytest.mark.asyncio
async def test_prepare_requires_project_membership(session: AsyncSession) -> None:
    """项目外用户不能为该项目准备副作用。"""
    fixtures = await _seed_project(session)
    stranger = User(subject=f"stranger-{uuid.uuid4()}", display_name="陌生人")
    session.add(stranger)
    await session.commit()
    with pytest.raises(PermissionDenied):
        await _service().prepare(
            session,
            project_id=fixtures.project.id,
            operation=SideEffectOperation.CANCEL_EXECUTION,
            aggregate_id=uuid.uuid4(),
            context={"job_id": "job-1"},
            requested_by=stranger.id,
        )


@pytest.mark.asyncio
async def test_decide_rejects_self_approval(session: AsyncSession) -> None:
    """申请人不能审批自己的副作用 Preparation。"""
    fixtures = await _seed_project(session)
    preparation = await _prepare_cancellation(session, fixtures)
    with pytest.raises(PermissionDenied, match="申请人"):
        await _service().decide(
            session,
            preparation_id=preparation.id,
            actor_id=fixtures.engineer.id,
            role=HarnessRole.OPERATOR,
            decision=HarnessApprovalValue.APPROVE,
            reason="自批",
        )


@pytest.mark.asyncio
async def test_decide_rejects_actor_without_granted_role(session: AsyncSession) -> None:
    """只持有 AUDITOR 职责的成员不能占用 OPERATOR 审批槽。"""
    fixtures = await _seed_project(session)
    preparation = await _prepare_cancellation(session, fixtures)
    with pytest.raises(PermissionDenied, match="职责"):
        await _service().decide(
            session,
            preparation_id=preparation.id,
            actor_id=fixtures.outsider.id,
            role=HarnessRole.OPERATOR,
            decision=HarnessApprovalValue.APPROVE,
            reason="伪称职责",
        )


@pytest.mark.asyncio
async def test_decide_enforces_separation_of_duties(session: AsyncSession) -> None:
    """同一自然人即使身兼多职责，也只能占用一个审批槽；相同决定重试幂等。"""
    fixtures = await _seed_project(session)
    service = _service()
    preparation = await service.prepare(
        session,
        project_id=fixtures.project.id,
        operation=SideEffectOperation.ROLLBACK_DORIS,
        aggregate_id=uuid.uuid4(),
        context={"database_name": "ods", "target_table": "orders"},
        requested_by=fixtures.engineer.id,
    )
    assert preparation.required_roles == ["DATA_ENGINEER", "DATA_APPROVER"]

    await service.decide(
        session,
        preparation_id=preparation.id,
        actor_id=fixtures.multi.id,
        role=HarnessRole.DATA_ENGINEER,
        decision=HarnessApprovalValue.APPROVE,
        reason="数据侧确认",
    )
    # 完全相同的重试幂等返回，不新增第二行，也不改状态。
    again = await service.decide(
        session,
        preparation_id=preparation.id,
        actor_id=fixtures.multi.id,
        role=HarnessRole.DATA_ENGINEER,
        decision=HarnessApprovalValue.APPROVE,
        reason="数据侧确认",
    )
    assert again.status is SideEffectPreparationStatus.WAITING_APPROVAL

    with pytest.raises(PermissionDenied, match="同一自然人"):
        await service.decide(
            session,
            preparation_id=preparation.id,
            actor_id=fixtures.multi.id,
            role=HarnessRole.DATA_APPROVER,
            decision=HarnessApprovalValue.APPROVE,
            reason="想一人双批",
        )

    final = await service.decide(
        session,
        preparation_id=preparation.id,
        actor_id=fixtures.operator.id,
        role=HarnessRole.DATA_APPROVER,
        decision=HarnessApprovalValue.APPROVE,
        reason="业务独立审批",
    )
    assert final.status is SideEffectPreparationStatus.READY
    approvals = await service.approvals(
        session,
        preparation_id=final.id,
        actor_id=fixtures.operator.id,
    )
    assert {item.role for item in approvals} == {
        HarnessRole.DATA_ENGINEER,
        HarnessRole.DATA_APPROVER,
    }


@pytest.mark.asyncio
async def test_decide_reject_marks_preparation_rejected(session: AsyncSession) -> None:
    """任何职责 REJECT 都把 Preparation 推向不可 Commit 的 REJECTED 终态。"""
    fixtures = await _seed_project(session)
    service = _service()
    preparation = await _prepare_cancellation(session, fixtures)
    rejected = await service.decide(
        session,
        preparation_id=preparation.id,
        actor_id=fixtures.operator.id,
        role=HarnessRole.OPERATOR,
        decision=HarnessApprovalValue.REJECT,
        reason="作业已自行收敛",
    )
    assert rejected.status is SideEffectPreparationStatus.REJECTED
    with pytest.raises(VersionConflict, match="审批"):
        await service.commit(
            session,
            preparation_id=rejected.id,
            committed_by=fixtures.engineer.id,
        )


@pytest.mark.asyncio
async def test_commit_requires_full_approvals(session: AsyncSession) -> None:
    """审批不齐时 Commit 失败关闭。"""
    fixtures = await _seed_project(session)
    preparation = await _prepare_cancellation(session, fixtures)
    with pytest.raises(VersionConflict, match="审批"):
        await _service().commit(
            session,
            preparation_id=preparation.id,
            committed_by=fixtures.engineer.id,
        )


async def _approved_preparation(
    session: AsyncSession,
    fixtures: _Fixtures,
    *,
    service: SideEffectHarnessService,
    aggregate_id: uuid.UUID | None = None,
) -> HarnessSideEffectPreparation:
    """准备并让 OPERATOR 完成唯一必需审批，返回 READY 的 Preparation。"""
    preparation = await _prepare_cancellation(
        session,
        fixtures,
        service=service,
        aggregate_id=aggregate_id,
    )
    return await service.decide(
        session,
        preparation_id=preparation.id,
        actor_id=fixtures.operator.id,
        role=HarnessRole.OPERATOR,
        decision=HarnessApprovalValue.APPROVE,
        reason="确认取消",
    )


@pytest.mark.asyncio
async def test_commit_issues_one_time_capability_and_outbox(
    session: AsyncSession,
) -> None:
    """Commit 签发一次性能力令牌、创建 Outbox 命令，并对重复 Commit 幂等收敛。"""
    fixtures = await _seed_project(session)
    service = _service()
    preparation = await _approved_preparation(session, fixtures, service=service)
    assert preparation.status is SideEffectPreparationStatus.READY

    result = await service.commit(
        session,
        preparation_id=preparation.id,
        committed_by=fixtures.engineer.id,
    )
    assert result.preparation_id == str(preparation.id)
    grant = await session.get(HarnessSideEffectCapabilityGrant, uuid.UUID(result.token_id))
    assert grant is not None
    assert grant.claims_json["single_use"] is True
    assert grant.claims_json["facts_sha256"] == preparation.facts_sha256
    assert len(grant.token_sha256) == 64
    assert grant.consumed_at is None  # 令牌在 Worker 首次领取时才消费
    outbox = await session.get(HarnessCommandOutbox, uuid.UUID(result.execution_id))
    assert outbox is not None
    assert outbox.status.value == "PENDING"
    assert outbox.command_payload["side_effect_preparation_id"] == str(preparation.id)

    replay = await service.commit(
        session,
        preparation_id=preparation.id,
        committed_by=fixtures.engineer.id,
    )
    assert replay.execution_id == result.execution_id
    assert replay.token_id == result.token_id

    await session.refresh(preparation)
    assert preparation.status is SideEffectPreparationStatus.COMMITTED
    assert preparation.execution_status is SideEffectExecutionStatus.PENDING
    assert preparation.committed_at is not None


@pytest.mark.asyncio
async def test_worker_claim_complete_and_fail_cycle(session: AsyncSession) -> None:
    """租约阻止并发领取；complete 落 SUCCEEDED，fail 落脱敏 FAILED 并可重领。"""
    fixtures = await _seed_project(session)
    service = _service()
    preparation = await _approved_preparation(session, fixtures, service=service)
    result = await service.commit(
        session,
        preparation_id=preparation.id,
        committed_by=fixtures.engineer.id,
    )

    command = await claim_committed_side_effect(session, "worker-1")
    assert command is not None
    assert command["preparation_id"] == str(preparation.id)
    assert command["command_id"] == result.execution_id
    busy = await claim_committed_side_effect(session, "worker-2")
    assert busy is None  # 有效租约内不能被第二个 Worker 领取

    await complete_side_effect(session, result.execution_id, {"cancelled": True})
    await session.refresh(preparation)
    assert preparation.execution_status is SideEffectExecutionStatus.SUCCEEDED
    assert preparation.result_sha256 is not None
    assert await claim_committed_side_effect(session, "worker-1") is None

    # 失败路径：另一条命令 fail 后可被重新领取，错误明文被脱敏。
    second = await _approved_preparation(
        session,
        fixtures,
        service=service,
        aggregate_id=uuid.uuid4(),
    )
    second_result = await service.commit(
        session,
        preparation_id=second.id,
        committed_by=fixtures.engineer.id,
    )
    claimed = await claim_committed_side_effect(session, "worker-3")
    assert claimed is not None
    assert claimed["preparation_id"] == str(second.id)
    await fail_side_effect(session, second_result.execution_id, "连接失败 password=hunter2")
    await session.refresh(second)
    assert second.execution_status is SideEffectExecutionStatus.FAILED
    assert second.execution_error is not None
    assert "hunter2" not in second.execution_error
    reclaim = await claim_committed_side_effect(session, "worker-4")
    assert reclaim is not None
    assert reclaim["preparation_id"] == str(second.id)

    with pytest.raises(ValueError, match="租约"):
        await claim_committed_side_effect(session, "worker-5", lease_seconds=5)


@pytest.mark.asyncio
async def test_side_effect_api_prepare_decide_and_commit(
    session: AsyncSession,
) -> None:
    """端到端：HTTP Prepare → 审批 → Commit，响应只回状态与摘要。"""
    fixtures = await _seed_project(session)

    async def override_session() -> AsyncIterator[AsyncSession]:
        """让 ASGI 请求复用当前 SQLite 会话。"""
        yield session

    app.dependency_overrides[get_session] = override_session
    try:
        base = "/api/v1"
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            prepared = await client.post(
                f"{base}/projects/{fixtures.project.id}/side-effect-preparations",
                headers={"X-Actor-ID": str(fixtures.engineer.id)},
                json={
                    "operation": "CANCEL_EXECUTION",
                    "aggregate_id": str(uuid.uuid4()),
                    "context": {"job_id": "job-http", "force": False},
                },
            )
            assert prepared.status_code == 201
            body = prepared.json()
            assert body["status"] == "WAITING_APPROVAL"
            assert body["execution_status"] == "PENDING"
            assert len(body["facts_sha256"]) == 64
            assert "frozen_facts" not in body

            decided = await client.post(
                f"{base}/side-effect-preparations/{body['id']}/decisions",
                headers={"X-Actor-ID": str(fixtures.operator.id)},
                json={"role": "OPERATOR", "decision": "APPROVE", "reason": "同意取消"},
            )
            assert decided.status_code == 200
            assert decided.json()["status"] == "READY"

            committed = await client.post(
                f"{base}/side-effect-preparations/{body['id']}/commit",
                headers={"X-Actor-ID": str(fixtures.engineer.id)},
            )
            assert committed.status_code == 202
            assert len(committed.json()["token_id"]) == 36

            listing = await client.get(
                f"{base}/projects/{fixtures.project.id}/side-effect-preparations",
                headers={"X-Actor-ID": str(fixtures.engineer.id)},
            )
            assert listing.status_code == 200
            assert [item["id"] for item in listing.json()] == [body["id"]]
    finally:
        app.dependency_overrides.pop(get_session, None)
