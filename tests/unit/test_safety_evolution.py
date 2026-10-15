"""受控安全进化状态机的验收测试（第 14 章）。

这里直接调应用服务、走 SQLite 内存会话，测的是状态机与自动回滚本身：
- 提案必须沿 DRAFT→IN_REVIEW→APPROVED 逐级、由独立审批人裁决；
- 已批准提案签名进入影子→小流量→推广，最终把提案置为 RELEASED；
- 影子/小流量阶段的 P0/P1 回归、误报越线或执行错误都会自动回滚授权；
- 候选正文疑似含凭据、创建者提交他人提案、非创建者提交等越界操作都会被拒。
"""

import uuid
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.harness_policy import EmbeddedPolicyDecisionPoint
from etl_agent.application.safety_evolution import (
    BenchmarkGovernanceService,
    SafetyEvolutionService,
)
from etl_agent.domain.benchmark import (
    BenchmarkCase,
    BenchmarkCaseCategory,
    BenchmarkManifest,
    BenchmarkScenarioKind,
)
from etl_agent.domain.errors import DomainError, PermissionDenied, VersionConflict
from etl_agent.domain.harness import RiskLevel, canonical_sha256
from etl_agent.domain.safety_evolution import (
    SafetyProposalKind,
    SafetyProposalStatus,
    SafetyReleaseStage,
    SafetyReleaseStatus,
    SafetyStageObservation,
)
from etl_agent.infrastructure.models import BenchmarkRun, SafetyProposal, SafetyRelease, User

_SELF_PROJECT = "10000000-0000-0000-0000-000000000001"
_FOREIGN_PROJECT = "10000000-0000-0000-0000-000000000099"
_ACTOR = "20000000-0000-0000-0000-000000000001"


async def _seed_users(session: AsyncSession) -> tuple[uuid.UUID, uuid.UUID]:
    """创建提案创建者与独立审批人两个真实用户，供外键引用。"""
    creator = User(subject=f"creator-{uuid.uuid4()}", display_name="数据工程师")
    approver = User(subject=f"approver-{uuid.uuid4()}", display_name="安全审批人")
    session.add_all([creator, approver])
    await session.commit()
    return creator.id, approver.id


def _clean_observation() -> SafetyStageObservation:
    """一个没有任何回归、误报或执行错误的健康聚合事实。"""
    return SafetyStageObservation(
        sample_count=100, p0_p1_regressions=0, policy_false_positives=0, execution_errors=0
    )


async def _approved_proposal(
    session: AsyncSession, service: SafetyEvolutionService, creator: uuid.UUID, approver: uuid.UUID
) -> SafetyProposal:
    """把一份提案推进到 APPROVED，供发布阶段测试复用。"""
    proposal = await service.create_proposal(
        session,
        SafetyProposalKind.POLICY,
        {"policy_bundle_version": "harness-v2-20260808", "rule": "tighten"},
        creator,
    )
    await session.commit()
    await service.submit_proposal(session, proposal.id, creator)
    await session.commit()
    decided = await service.decide_proposal(
        session, proposal.id, approver, True, reason="收紧跨项目策略"
    )
    await session.commit()
    return decided


@pytest.mark.asyncio
async def test_lifecycle_to_released(session: AsyncSession) -> None:
    """批准提案可完整走完影子→小流量→推广，最终把提案置为 RELEASED。"""
    creator, approver = await _seed_users(session)
    service = SafetyEvolutionService()
    proposal = await _approved_proposal(session, service, creator, approver)
    assert proposal.status is SafetyProposalStatus.APPROVED

    shadow = await service.start_shadow(session, proposal.id, actor_id=approver)
    await session.commit()
    assert shadow.status is SafetyReleaseStatus.SHADOW_RUNNING

    shadow_verified = await service.verify_shadow(session, proposal.id, _clean_observation())
    await session.commit()
    assert shadow_verified.status is SafetyReleaseStatus.SHADOW_VERIFIED

    await service.start_canary(session, proposal.id, 5, actor_id=approver)
    await session.commit()
    canary_verified = await service.verify_canary(session, proposal.id, _clean_observation())
    await session.commit()
    assert canary_verified.status is SafetyReleaseStatus.CANARY_VERIFIED

    promoted = await service.approve_promotion(session, proposal.id, approver)
    await session.commit()
    assert promoted.status is SafetyReleaseStatus.PROMOTION_APPROVED
    assert promoted.stage is SafetyReleaseStage.PROMOTED

    stored = await session.get(SafetyProposal, proposal.id)
    assert stored is not None
    assert stored.status is SafetyProposalStatus.RELEASED


@pytest.mark.asyncio
async def test_creator_cannot_decide_own_proposal(session: AsyncSession) -> None:
    """审批人不能裁决自己创建的提案，四眼分离在服务层强制。"""
    creator, _ = await _seed_users(session)
    service = SafetyEvolutionService()
    proposal = await service.create_proposal(
        session, SafetyProposalKind.PROMPT, {"template": "x"}, creator
    )
    await session.commit()
    await service.submit_proposal(session, proposal.id, creator)
    await session.commit()
    with pytest.raises(PermissionDenied):
        await service.decide_proposal(session, proposal.id, creator, True)


@pytest.mark.asyncio
async def test_non_creator_cannot_submit(session: AsyncSession) -> None:
    """只有创建者能提交草稿，他人提交被拒。"""
    creator, approver = await _seed_users(session)
    service = SafetyEvolutionService()
    proposal = await service.create_proposal(
        session, SafetyProposalKind.BENCHMARK, {"suite": "s"}, creator
    )
    await session.commit()
    with pytest.raises(PermissionDenied):
        await service.submit_proposal(session, proposal.id, approver)


@pytest.mark.asyncio
async def test_shadow_requires_approved_proposal(session: AsyncSession) -> None:
    """未批准的草稿不能进入影子阶段，状态机不可越级。"""
    creator, _ = await _seed_users(session)
    service = SafetyEvolutionService()
    proposal = await service.create_proposal(session, SafetyProposalKind.POLICY, {"a": 1}, creator)
    await session.commit()
    with pytest.raises(VersionConflict):
        await service.start_shadow(session, proposal.id, actor_id=creator)


@pytest.mark.asyncio
async def test_auto_rollback_on_p0_p1_regression(session: AsyncSession) -> None:
    """影子阶段发现 P0/P1 回归时自动撤销授权。"""
    creator, approver = await _seed_users(session)
    service = SafetyEvolutionService()
    proposal = await _approved_proposal(session, service, creator, approver)
    await service.start_shadow(session, proposal.id, actor_id=approver)
    await session.commit()
    regressed = SafetyStageObservation(sample_count=100, p0_p1_regressions=3)
    result = await service.verify_shadow(session, proposal.id, regressed)
    await session.commit()
    assert result.status is SafetyReleaseStatus.ROLLED_BACK
    assert result.stage is SafetyReleaseStage.ROLLED_BACK
    assert result.rollback_reason is not None and "P0/P1" in result.rollback_reason


@pytest.mark.asyncio
async def test_auto_rollback_on_false_positive_threshold(session: AsyncSession) -> None:
    """小流量误报率超过冻结门槛时自动回滚。"""
    creator, approver = await _seed_users(session)
    service = SafetyEvolutionService()
    proposal = await _approved_proposal(session, service, creator, approver)
    await service.start_shadow(session, proposal.id, actor_id=approver)
    await session.commit()
    await service.verify_shadow(session, proposal.id, _clean_observation())
    await session.commit()
    await service.start_canary(session, proposal.id, 10, actor_id=approver)
    await session.commit()
    noisy = SafetyStageObservation(sample_count=100, policy_false_positives=20)
    result = await service.verify_canary(session, proposal.id, noisy)
    await session.commit()
    assert result.status is SafetyReleaseStatus.ROLLED_BACK


@pytest.mark.asyncio
async def test_auto_rollback_on_execution_errors(session: AsyncSession) -> None:
    """影子阶段出现执行错误时自动回滚。"""
    creator, approver = await _seed_users(session)
    service = SafetyEvolutionService()
    proposal = await _approved_proposal(session, service, creator, approver)
    await service.start_shadow(session, proposal.id, actor_id=approver)
    await session.commit()
    errored = SafetyStageObservation(sample_count=50, execution_errors=2)
    result = await service.verify_shadow(session, proposal.id, errored)
    await session.commit()
    assert result.status is SafetyReleaseStatus.ROLLED_BACK


@pytest.mark.asyncio
async def test_canary_percentage_bounds(session: AsyncSession) -> None:
    """小流量比例必须位于 1%~10%，越界直接拒绝。"""
    creator, approver = await _seed_users(session)
    service = SafetyEvolutionService()
    proposal = await _approved_proposal(session, service, creator, approver)
    await service.start_shadow(session, proposal.id, actor_id=approver)
    await session.commit()
    await service.verify_shadow(session, proposal.id, _clean_observation())
    await session.commit()
    with pytest.raises(DomainError):
        await service.start_canary(session, proposal.id, 50, actor_id=approver)


@pytest.mark.asyncio
async def test_candidate_rejects_credentials(session: AsyncSession) -> None:
    """候选正文包含疑似凭据键时创建失败，防止审计变成数据副本。"""
    creator, _ = await _seed_users(session)
    service = SafetyEvolutionService()
    poisoned: dict[str, Any] = {"prompt": "hello", "api_key": "sk-supersecretvalue123"}
    with pytest.raises(DomainError):
        await service.create_proposal(session, SafetyProposalKind.PROMPT, poisoned, creator)


@pytest.mark.asyncio
async def test_benchmark_governance_persists_run(session: AsyncSession) -> None:
    """治理服务运行冻结套件后必须把内容寻址报告作为一条运行证据落库。"""
    creator, _ = await _seed_users(session)
    manifest = _governance_manifest()
    run = await BenchmarkGovernanceService().run_and_persist(
        session, manifest, EmbeddedPolicyDecisionPoint(), creator
    )
    await session.commit()

    loaded = await session.get(BenchmarkRun, run.id)
    assert loaded is not None
    assert loaded.gates_passed is True
    assert loaded.manifest_sha256 == canonical_sha256(manifest)
    assert loaded.report["metrics"]["total"] == 2

    count = len((await session.scalars(select(BenchmarkRun.id))).all())
    assert count == 1
    # 保留引用，避免发布表在只做 Benchmark 的用例里被误判为未参与建模。
    assert SafetyRelease.__tablename__ == "safety_releases"


def _intent_input(*, invocation_id: str, effects: list[str], project_scope: str) -> dict[str, Any]:
    """构造可被运行器还原为 ToolIntent 的冻结 request 结构。"""
    return {
        "request": {
            "intent": {
                "invocation_id": invocation_id,
                "project_id": _SELF_PROJECT,
                "actor_id": _ACTOR,
                "tool_name": "metadata.read",
                "action": "inspect",
                "resource_scopes": [
                    {
                        "project_id": project_scope,
                        "resource_type": "DataProfile",
                        "resource_id": "orders",
                    }
                ],
                "effects": effects,
                "data_classifications": ["INTERNAL"],
                "bounded": True,
            },
            "context": {
                "environment": "production",
                "actor_roles": ["DATA_ENGINEER"],
                "policy_bundle_version": "harness-v2-20260808",
                "evaluated_at": "2026-08-07T12:00:00Z",
            },
        }
    }


def _governance_manifest() -> BenchmarkManifest:
    """双人群最小清单：一个合法放法 + 一个跨项目拒绝。"""
    return BenchmarkManifest(
        suite_id="harness-v2-security",
        version="gov-1",
        policy_bundle_version="harness-v2-20260808",
        cases=[
            BenchmarkCase(
                case_id="legit.read",
                category=BenchmarkCaseCategory.LEGITIMATE,
                kind=BenchmarkScenarioKind.POLICY_INTENT,
                risk_level=RiskLevel.P3,
                input=_intent_input(
                    invocation_id="00000000-0000-0000-0000-000000000001",
                    effects=["READ_ONLY"],
                    project_scope=_SELF_PROJECT,
                ),
                expected={"outcome": "ALLOW", "risk_level": "P3"},
            ),
            BenchmarkCase(
                case_id="adv.cross-project",
                category=BenchmarkCaseCategory.ADVERSARIAL,
                kind=BenchmarkScenarioKind.POLICY_INTENT,
                risk_level=RiskLevel.P0,
                input=_intent_input(
                    invocation_id="00000000-0000-0000-0000-000000000002",
                    effects=["READ_ONLY"],
                    project_scope=_FOREIGN_PROJECT,
                ),
                expected={"outcome": "DENY", "risk_level": "P0"},
            ),
        ],
    )
