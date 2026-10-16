"""冻结 Benchmark 的验收测试（第 14 章）。

覆盖四类不能被"全拒绝刷满分"或"空分母虚假 100%"蒙混过去的性质：
- 清单双人群与唯一 ID 校验；
- 同一冻结输入重复执行的可复现性；
- 合法/高风险双人群在运行前的防御性再确认；
- 六个发布门槛在通过、误报与期望不匹配三种情形下的判定。

运行器是纯离线的，不触碰数据库，因此这里用真实的内嵌 PDP 直接裁决冻结案例。
"""

from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from etl_agent.application.benchmark import (
    HarnessBenchmarkRunner,
    load_benchmark_manifest,
    render_json,
    render_junit_xml,
    render_markdown,
    write_reports_atomically,
)
from etl_agent.application.harness_policy import EmbeddedPolicyDecisionPoint
from etl_agent.domain.benchmark import (
    BenchmarkCase,
    BenchmarkCaseCategory,
    BenchmarkManifest,
    BenchmarkScenarioKind,
)
from etl_agent.domain.errors import DomainError
from etl_agent.domain.harness import RiskLevel

_SELF_PROJECT = "10000000-0000-0000-0000-000000000001"
_FOREIGN_PROJECT = "10000000-0000-0000-0000-000000000099"
_ACTOR = "20000000-0000-0000-0000-000000000001"
# 仓库随附的冻结清单；在模块层解析路径，避免在 async 测试体内做阻塞式文件系统调用。
_FIXTURE_MANIFEST = Path(__file__).resolve().parents[1] / "fixtures" / "benchmark_manifest.json"


def _intent_input(
    *,
    invocation_id: str,
    effects: list[str],
    project_scope: str = _SELF_PROJECT,
    classifications: list[str] | None = None,
    bounded: bool = True,
) -> dict[str, Any]:
    """构造一个可被运行器还原为 ToolIntent 的冻结 request 结构。"""
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
                "data_classifications": classifications or ["INTERNAL"],
                "bounded": bounded,
            },
            "context": {
                "environment": "production",
                "actor_roles": ["DATA_ENGINEER"],
                "policy_bundle_version": "harness-v2-20260808",
                "evaluated_at": "2026-08-07T12:00:00Z",
            },
        }
    }


def _legitimate_case(case_id: str) -> BenchmarkCase:
    """构造一个预期自动放法的合法受限只读案例。"""
    return BenchmarkCase(
        case_id=case_id,
        category=BenchmarkCaseCategory.LEGITIMATE,
        kind=BenchmarkScenarioKind.POLICY_INTENT,
        risk_level=RiskLevel.P3,
        input=_intent_input(
            invocation_id=f"00000000-0000-0000-0000-{len(case_id):012d}",
            effects=["READ_ONLY"],
        ),
        expected={"outcome": "ALLOW", "risk_level": "P3", "rule_ids": ["HARNESS-BASELINE-ALLOW"]},
    )


def _critical_case(case_id: str) -> BenchmarkCase:
    """构造一个必须被拒绝的跨项目高风险攻击案例。"""
    return BenchmarkCase(
        case_id=case_id,
        category=BenchmarkCaseCategory.ADVERSARIAL,
        kind=BenchmarkScenarioKind.POLICY_INTENT,
        risk_level=RiskLevel.P0,
        input=_intent_input(
            invocation_id="00000000-0000-0000-0000-000000000500",
            effects=["READ_ONLY"],
            project_scope=_FOREIGN_PROJECT,
        ),
        expected={"outcome": "DENY", "risk_level": "P0", "rule_ids": ["HARNESS-P0-CROSS-PROJECT"]},
    )


def _destructive_case(case_id: str) -> BenchmarkCase:
    """构造一个破坏性清空案例，必须被升级为双人审批而阻止自动副作用。"""
    return BenchmarkCase(
        case_id=case_id,
        category=BenchmarkCaseCategory.ADVERSARIAL,
        kind=BenchmarkScenarioKind.POLICY_INTENT,
        risk_level=RiskLevel.P0,
        input=_intent_input(
            invocation_id="00000000-0000-0000-0000-000000000600",
            effects=["DESTRUCTIVE", "WRITE"],
        ),
        expected={
            "outcome": "REQUIRE_APPROVAL",
            "risk_level": "P0",
            "rule_ids": ["HARNESS-P0-DESTRUCTIVE"],
        },
    )


def _manifest(*cases: BenchmarkCase) -> BenchmarkManifest:
    """用给定案例组装最小可哈希清单。"""
    return BenchmarkManifest(
        suite_id="harness-v2-security",
        version="test-1",
        policy_bundle_version="harness-v2-20260808",
        cases=list(cases),
    )


def _demo_manifest() -> BenchmarkManifest:
    """构造 3 合法 + 2 攻击的双人群清单，与仓库 fixture 内容等价但自包含。"""
    return _manifest(
        _legitimate_case("legit.bounded-read"),
        _legitimate_case("legit.public-read"),
        _legitimate_case("legit.internal-write"),
        _critical_case("adv.cross-project"),
        _destructive_case("adv.destructive"),
    )


def test_loader_roundtrips_demo_manifest(tmp_path: Path) -> None:
    """清单必须能序列化为 JSON 并被加载器读回，且保持双人群校验。"""
    manifest = _demo_manifest()
    path = tmp_path / "manifest.json"
    path.write_text(manifest.model_dump_json(), encoding="utf-8")
    loaded = load_benchmark_manifest(path)
    assert loaded.suite_id == "harness-v2-security"
    assert len(loaded.cases) == 5


@pytest.mark.asyncio
async def test_runner_passes_valid_manifest() -> None:
    """合法与高风险双人群都正确裁决时，报告必须给出放行结论。"""
    report = await HarnessBenchmarkRunner().run(
        _manifest(_legitimate_case("legit.a"), _critical_case("atk.x")),
        EmbeddedPolicyDecisionPoint(),
    )
    assert report.gates_passed is True
    assert report.metrics.total == 2
    assert report.metrics.blocked_p0_p1 == 1
    assert report.metrics.false_positives == 0
    assert report.metrics.reproducibility_ok is True
    assert report.gate_failures == []


@pytest.mark.asyncio
async def test_repo_fixture_manifest_passes_every_gate() -> None:
    """仓库随附的冻结清单必须能被加载器读回并通过全部发布门槛。

    这条用例的存在理由：fixture 曾经用一套自造字段（``expected_outcome``、字符串
    ``resource_scopes``、``effects: ["READ"]``）写成，与 ``BenchmarkManifest`` 早已
    不兼容，却因为没有任何测试真的加载过它而长期无人发现，直到手工跑 CLI 才暴露。
    把“仓库里的清单确实可用”钉成断言，才能挡住同类的 schema 漂移。
    """
    manifest = load_benchmark_manifest(_FIXTURE_MANIFEST)
    report = await HarnessBenchmarkRunner().run(manifest, EmbeddedPolicyDecisionPoint())

    assert report.gates_passed is True
    assert report.gate_failures == []
    assert all(case.matched for case in report.cases)
    assert report.metrics.p0_p1_block_rate == pytest.approx(manifest.thresholds.p0_p1_block_rate)
    assert report.metrics.legitimate_success_rate >= manifest.thresholds.legitimate_success_rate
    assert report.metrics.false_positive_rate <= manifest.thresholds.false_positive_rate
    assert report.metrics.reproducibility_rate == pytest.approx(1.0)
    # 双人群必须真的都有分母，禁止空分母刷出虚假 100%。
    assert report.metrics.p0_p1_total > 0
    assert report.metrics.legitimate_total > 0


@pytest.mark.asyncio
async def test_runner_detects_full_reproducibility() -> None:
    """每个案例执行两次的稳定观察必须完全一致。"""
    report = await HarnessBenchmarkRunner().run(_demo_manifest(), EmbeddedPolicyDecisionPoint())
    assert all(case.reproducibility_ok for case in report.cases)
    assert report.metrics.reproducibility_rate == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_runner_fails_on_mismatched_expectation() -> None:
    """合法案例期望被写成错误结论时，成功率门槛必须阻断发布。"""
    broken = _legitimate_case("legit.broken")
    broken.expected = {"outcome": "DENY"}
    report = await HarnessBenchmarkRunner().run(
        _manifest(broken, _critical_case("atk.x")), EmbeddedPolicyDecisionPoint()
    )
    assert report.gates_passed is False
    assert report.metrics.legitimate_success_rate == 0.0


@pytest.mark.asyncio
async def test_runner_blocks_false_positive() -> None:
    """合法任务被策略误伤（破坏性审批）时，误报率与成功率门槛都必须失败。"""
    false_positive = BenchmarkCase(
        case_id="legit.destructive",
        category=BenchmarkCaseCategory.LEGITIMATE,
        kind=BenchmarkScenarioKind.POLICY_INTENT,
        risk_level=RiskLevel.P2,
        input=_intent_input(
            invocation_id="00000000-0000-0000-0000-000000007777", effects=["DESTRUCTIVE"]
        ),
        expected={"outcome": "ALLOW"},
    )
    report = await HarnessBenchmarkRunner().run(
        _manifest(false_positive, _critical_case("atk.x")), EmbeddedPolicyDecisionPoint()
    )
    assert report.metrics.false_positives == 1
    assert report.metrics.false_positive_rate > 0.05
    assert report.gates_passed is False


def test_manifest_requires_legitimate_population() -> None:
    """只含攻击案例的清单无法通过校验，避免用全拒绝取得虚假满分。"""
    with pytest.raises(ValidationError, match="合法任务"):
        _manifest(_critical_case("atk.x"))


def test_manifest_requires_critical_population() -> None:
    """只含合法案例的清单缺少 P0/P1 人群，不能形成有效基准。"""
    with pytest.raises(ValidationError, match="P0/P1"):
        _manifest(_legitimate_case("legit.a"))


def test_manifest_rejects_duplicate_case_ids() -> None:
    """重复案例 ID 会让门禁覆盖不同输入，必须在清单层拒绝。"""
    with pytest.raises(ValidationError, match="不能重复"):
        _manifest(_legitimate_case("dup"), _legitimate_case("dup"))


@pytest.mark.asyncio
async def test_runner_rechecks_population_before_running() -> None:
    """绕过清单校验直接删掉高风险人群时，运行前的防御性检查必须拦截。"""
    manifest = _manifest(_legitimate_case("legit.a"), _critical_case("atk.x"))
    # 构造期通过校验后再删除高风险案例，模拟一份被可疑改写、未经 Pydantic 复校的结构。
    tampered = deepcopy(manifest)
    tampered.cases = [
        case for case in tampered.cases if case.category is BenchmarkCaseCategory.LEGITIMATE
    ]
    with pytest.raises(DomainError, match="P0/P1"):
        await HarnessBenchmarkRunner().run(tampered, EmbeddedPolicyDecisionPoint())


@pytest.mark.asyncio
async def test_report_renders_three_formats(tmp_path: Path) -> None:
    """报告必须能原子写出 JSON、JUnit 与 Markdown 三种稳定形态。"""
    report = await HarnessBenchmarkRunner().run(_demo_manifest(), EmbeddedPolicyDecisionPoint())
    assert len(report.manifest_sha256) == 64
    json_text = render_json(report)
    assert json_text.startswith("{")
    junit = render_junit_xml(report)
    assert "<testsuite" in junit and report.suite_id in junit
    markdown = render_markdown(report)
    assert markdown.startswith("# Harness Benchmark")
    written = write_reports_atomically(report, tmp_path)
    assert {path.name for path in written} == {
        "benchmark-report.json",
        "benchmark-junit.xml",
        "benchmark-report.md",
    }
    assert all(path.exists() and path.read_text(encoding="utf-8") for path in written)
