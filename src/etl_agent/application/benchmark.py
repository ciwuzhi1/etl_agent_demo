"""离线确定性 Benchmark 执行器与报告渲染（第 14 章）。

运行器只调用白名单策略与静态检查，不访问生产数据库、网络、密钥或大模型；
每个案例被执行两次以证明同一冻结输入的裁决可复现，再按清单里冻结的门槛给出
发布放行结论。报告可以原子写成 JSON、JUnit 与 Markdown，供 CI 直接消费。
"""

import json
import os
import tempfile
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from etl_agent.application.harness_policy import PolicyDecisionPoint
from etl_agent.application.policy import (
    require_no_hardcoded_secrets,
    require_supported_transform_contract,
)
from etl_agent.domain.benchmark import (
    BenchmarkCase,
    BenchmarkCaseCategory,
    BenchmarkCaseResult,
    BenchmarkManifest,
    BenchmarkMetrics,
    BenchmarkReport,
    BenchmarkScenarioKind,
    BenchmarkThresholds,
)
from etl_agent.domain.errors import DomainError
from etl_agent.domain.harness import (
    DataClassification,
    HarnessRole,
    PolicyEvaluationContext,
    PolicyEvaluationRequest,
    PolicyOutcome,
    ResourceScope,
    RiskLevel,
    ToolEffect,
    ToolIntent,
    canonical_sha256,
)


def load_benchmark_manifest(path: Path) -> BenchmarkManifest:
    """从 UTF-8 JSON 加载版本化场景清单，拒绝未知或不完整结构。"""
    return BenchmarkManifest.model_validate_json(path.read_text(encoding="utf-8"))


class HarnessBenchmarkRunner:
    """离线执行确定性安全案例，不访问生产数据库、网络、密钥或大模型。"""

    async def run(self, manifest: BenchmarkManifest, pdp: PolicyDecisionPoint) -> BenchmarkReport:
        """执行每个案例两次验证重放一致性，再按冻结门槛生成发布结论。"""
        _require_dual_populations(manifest)
        started_at = datetime.now(UTC)
        results = [await self._run_case(case, pdp) for case in manifest.cases]
        finished_at = datetime.now(UTC)
        metrics = _metrics(results)
        failures = _gate_failures(metrics=metrics, thresholds=manifest.thresholds)
        return BenchmarkReport(
            suite_id=manifest.suite_id,
            version=manifest.version,
            policy_bundle_version=manifest.policy_bundle_version,
            manifest_sha256=canonical_sha256(manifest),
            thresholds=manifest.thresholds,
            started_at=started_at,
            finished_at=finished_at,
            gates_passed=not failures,
            gate_failures=failures,
            metrics=metrics,
            cases=results,
        )

    async def _run_case(self, case: BenchmarkCase, pdp: PolicyDecisionPoint) -> BenchmarkCaseResult:
        """执行并重放一个案例，只比较稳定观察字段，不比较随机决策 ID。"""
        first = await self._execute(case, pdp)
        second = await self._execute(case, pdp)
        errors = _expected_mismatches(case.expected, first)
        reproducible = canonical_sha256(first) == canonical_sha256(second)
        blocked = _is_blocked(first)
        false_positive = case.category is BenchmarkCaseCategory.LEGITIMATE and blocked
        matched = not errors and not false_positive
        if not reproducible:
            errors.append("相同冻结输入的裁决不可复现")
        return BenchmarkCaseResult(
            case_id=case.case_id,
            category=case.category,
            expected=case.expected,
            actual=first,
            matched=matched,
            reproducibility_hash=canonical_sha256(first),
            reproducibility_ok=reproducible,
            blocked=blocked,
            false_positive=false_positive,
            errors=errors,
        )

    async def _execute(self, case: BenchmarkCase, pdp: PolicyDecisionPoint) -> dict[str, Any]:
        """按白名单类型路由到确定性执行器，禁止场景文件指定任意 Python 调用。"""
        if case.kind is BenchmarkScenarioKind.POLICY_INTENT:
            return await _execute_policy(case, pdp)
        if case.kind is BenchmarkScenarioKind.STATIC_HOCON:
            return _execute_hocon(case)
        raise DomainError(f"当前离线 Benchmark 不支持该场景类型：{case.kind}")


async def _execute_policy(case: BenchmarkCase, pdp: PolicyDecisionPoint) -> dict[str, Any]:
    """运行冻结 ToolIntent，并仅返回裁决、规则和规范证据摘要。"""
    request = _build_policy_request(case.input)
    decision = await pdp.evaluate(request)
    return {
        "outcome": decision.outcome.value,
        "risk_level": decision.risk_level.value,
        "rule_ids": sorted(match.rule_id for match in decision.rule_matches),
        "required_roles": sorted(role.value for role in decision.required_roles),
        "required_approvals": decision.required_approvals,
        "evidence_sha256": decision.request_fingerprint,
    }


def _execute_hocon(case: BenchmarkCase) -> dict[str, Any]:
    """对白名单静态门禁执行无副作用 HOCON 攻击或合法案例。"""
    check = str(case.input.get("check") or "")
    hocon = str(case.input.get("hocon") or "")
    rule_id = f"STATIC-{check.upper().replace('_', '-')}"
    try:
        if check == "no_hardcoded_secrets":
            require_no_hardcoded_secrets(hocon)
        elif check == "supported_transform":
            require_supported_transform_contract(hocon)
        else:
            raise DomainError(f"Benchmark 不允许调用静态检查：{check}")
    except DomainError as exc:
        return {
            "outcome": PolicyOutcome.DENY.value,
            "risk_level": case.risk_level.value,
            "rule_ids": [rule_id],
            "error": str(exc),
            "evidence_sha256": canonical_sha256(case.input),
        }
    return {
        "outcome": PolicyOutcome.ALLOW.value,
        "risk_level": RiskLevel.P3.value,
        "rule_ids": [rule_id],
        "evidence_sha256": canonical_sha256(case.input),
    }


def _build_policy_request(input: dict[str, Any]) -> PolicyEvaluationRequest:
    """把冻结的 request 结构还原为 PDP 输入；只接受白名单字段，不执行任意调用。"""
    raw = input.get("request")
    if not isinstance(raw, dict):
        raise DomainError("POLICY_INTENT 场景缺少 request")
    intent_raw = raw.get("intent")
    context_raw = raw.get("context")
    if not isinstance(intent_raw, dict) or not isinstance(context_raw, dict):
        raise DomainError("POLICY_INTENT 场景缺少 intent 或 context")
    scopes = [
        ResourceScope(
            project_id=UUID(str(scope["project_id"])),
            resource_type=scope.get("resource_type"),
            resource_id=scope.get("resource_id"),
        )
        for scope in intent_raw.get("resource_scopes", [])
        if isinstance(scope, dict)
    ]
    intent = ToolIntent(
        tool_name=str(intent_raw["tool_name"]),
        action=str(intent_raw["action"]),
        project_id=UUID(str(intent_raw["project_id"])),
        actor_id=UUID(str(intent_raw["actor_id"])),
        invocation_id=UUID(str(intent_raw.get("invocation_id") or uuid4())),
        resource_scopes=scopes,
        effects=[ToolEffect(effect) for effect in intent_raw.get("effects", [])],
        data_classifications=[
            DataClassification(value) for value in intent_raw.get("data_classifications", [])
        ],
        external_destination=intent_raw.get("external_destination"),
        bounded=bool(intent_raw.get("bounded", True)),
        artifact_sha256=intent_raw.get("artifact_sha256"),
        metadata=dict(intent_raw.get("metadata", {}) or {}),
    )
    evaluated_at = _parse_timestamp(context_raw.get("evaluated_at"))
    context = PolicyEvaluationContext(
        environment=str(context_raw["environment"]),
        actor_roles=[HarnessRole(role) for role in context_raw.get("actor_roles", [])],
        policy_bundle_version=str(context_raw["policy_bundle_version"]),
        evaluated_at=evaluated_at,
    )
    return PolicyEvaluationRequest(intent=intent, context=context)


def _parse_timestamp(value: Any) -> datetime:
    """把冻结时间戳解析为带时区 datetime，缺失时回退到固定纪元以保证可复现。"""
    if isinstance(value, str):
        text = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    return datetime(2026, 8, 7, 12, 0, tzinfo=UTC)


def _expected_mismatches(expected: dict[str, Any], observed: dict[str, Any]) -> list[str]:
    """只比较场景显式冻结的观察字段，允许报告增加非授权性诊断字段。"""
    errors: list[str] = []
    for key, value in expected.items():
        actual = observed.get(key)
        if isinstance(value, list):
            actual = sorted(actual) if isinstance(actual, list) else actual
            value = sorted(value)
        if actual != value:
            errors.append(f"{key} 期望 {value!r}，实际 {actual!r}")
    return errors


def _is_blocked(observation: dict[str, Any]) -> bool:
    """把拒绝与审批要求都视为阻止自动副作用。"""
    return observation.get("outcome") in {
        PolicyOutcome.DENY.value,
        PolicyOutcome.REQUIRE_APPROVAL.value,
    }


def _require_dual_populations(manifest: BenchmarkManifest) -> None:
    """在运行前再次确认合法与 P0/P1 双人群，防御绕过清单校验直接构造的 manifest。"""
    if not any(case.category is BenchmarkCaseCategory.LEGITIMATE for case in manifest.cases):
        raise DomainError("Benchmark 至少需要一个合法任务案例")
    if not any(
        case.category is not BenchmarkCaseCategory.LEGITIMATE
        and case.risk_level in {RiskLevel.P0, RiskLevel.P1}
        for case in manifest.cases
    ):
        raise DomainError("Benchmark 至少需要一个 P0/P1 风险的非合法案例")


def _metrics(results: list[BenchmarkCaseResult]) -> BenchmarkMetrics:
    """计算安全阻断、合法成功、误报与复现四类评分。"""
    total = len(results)
    legitimate = [item for item in results if item.category is BenchmarkCaseCategory.LEGITIMATE]
    critical = [
        item
        for item in results
        if item.category is not BenchmarkCaseCategory.LEGITIMATE
        and _risk_of(item) in {RiskLevel.P0, RiskLevel.P1}
    ]
    blocked_p0_p1 = sum(item.blocked for item in critical)
    false_positives = sum(item.false_positive for item in legitimate)
    legitimate_passed = sum(item.matched for item in legitimate)
    reproducibility_count = sum(item.reproducibility_ok for item in results)
    p0_p1_total = len(critical)
    legitimate_total = len(legitimate)
    return BenchmarkMetrics(
        total=total,
        blocked_p0_p1=blocked_p0_p1,
        p0_p1_total=p0_p1_total,
        legitimate_total=legitimate_total,
        false_positives=false_positives,
        reproducibility_ok=reproducibility_count == total,
        reproducibility_count=reproducibility_count,
        p0_p1_block_rate=_rate(blocked_p0_p1, p0_p1_total, empty_value=1.0),
        legitimate_success_rate=_rate(legitimate_passed, legitimate_total, empty_value=1.0),
        false_positive_rate=_rate(false_positives, legitimate_total, empty_value=0.0),
        reproducibility_rate=_rate(reproducibility_count, total, empty_value=1.0),
    )


def _risk_of(result: BenchmarkCaseResult) -> RiskLevel:
    """从案例实际观察里取风险等级，缺省时按 P0 保守处理。"""
    raw = str(result.actual.get("risk_level") or "")
    try:
        return RiskLevel(raw)
    except ValueError:
        return RiskLevel.P0


def _gate_failures(
    metrics: BenchmarkMetrics,
    thresholds: BenchmarkThresholds,
) -> list[str]:
    """按冻结门槛返回全部失败原因，任何 P0/P1 漏拦都会阻断发布。"""
    checks = (
        (
            metrics.p0_p1_block_rate >= thresholds.p0_p1_block_rate,
            "P0/P1 阻断率未达到门槛",
        ),
        (
            metrics.legitimate_success_rate >= thresholds.legitimate_success_rate,
            "合法任务成功率未达到门槛",
        ),
        (
            metrics.false_positive_rate <= thresholds.false_positive_rate,
            "策略误报率超过门槛",
        ),
        (metrics.reproducibility_rate >= 1.0, "存在相同冻结输入不可复现的案例"),
    )
    return [message for passed, message in checks if not passed]


def _rate(numerator: int, denominator: int, *, empty_value: float) -> float:
    """计算 0 到 1 的稳定比例；空集合语义必须由调用方明确指定。"""
    return empty_value if denominator == 0 else numerator / denominator


def render_json(report: BenchmarkReport) -> str:
    """渲染稳定 JSON 报告，字段顺序固定以便跨提交比对。"""
    return report.model_dump_json(indent=2)


def render_junit_xml(report: BenchmarkReport) -> str:
    """生成标准 JUnit XML，让 CI 可直接展示失败案例。"""
    suite = ET.Element(
        "testsuite",
        name=report.suite_id,
        tests=str(len(report.cases)),
        failures=str(sum(not case.matched for case in report.cases)),
    )
    for case in report.cases:
        item = ET.SubElement(
            suite,
            "testcase",
            name=case.case_id,
            classname=f"harness.{case.category.value.casefold()}",
        )
        if not case.matched:
            failure = ET.SubElement(item, "failure", message="; ".join(case.errors))
            failure.text = _stable_json(case.actual)
    return ET.tostring(suite, encoding="unicode", xml_declaration=True)


def render_markdown(report: BenchmarkReport) -> str:
    """生成人可读门禁摘要，失败原因和单案例结论保持紧凑可审查。"""
    metrics = report.metrics
    lines = [
        f"# Harness Benchmark {report.version}",
        "",
        f"结论：**{'PASS' if report.gates_passed else 'FAIL'}**",
        "",
        f"- P0/P1 阻断率：{metrics.p0_p1_block_rate:.2%}",
        f"- 合法任务成功率：{metrics.legitimate_success_rate:.2%}",
        f"- 策略误报率：{metrics.false_positive_rate:.2%}",
        f"- 决策复现率：{metrics.reproducibility_rate:.2%}",
        "",
        "| 案例 | 类别 | 结果 |",
        "| --- | --- | --- |",
    ]
    lines.extend(
        f"| {case.case_id} | {case.category.value} | {'PASS' if case.matched else 'FAIL'} |"
        for case in report.cases
    )
    if report.gate_failures:
        lines.extend(["", "## 阻断原因", ""])
        lines.extend(f"- {failure}" for failure in report.gate_failures)
    return "\n".join(lines) + "\n"


def write_reports_atomically(report: BenchmarkReport, output_dir: Path) -> list[Path]:
    """原子写出稳定 JSON、JUnit 和 Markdown，避免 CI 读取半份报告。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    payloads = {
        "benchmark-report.json": render_json(report),
        "benchmark-junit.xml": render_junit_xml(report),
        "benchmark-report.md": render_markdown(report),
    }
    written: list[Path] = []
    for name, payload in payloads.items():
        path = output_dir / name
        _atomic_write_text(path, payload)
        written.append(path)
    return written


def _stable_json(value: dict[str, Any]) -> str:
    """把观察结果序列化为稳定 JSON 文本。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _atomic_write_text(path: Path, payload: str) -> None:
    """在目标目录写临时文件后原子替换，崩溃时不留下半份门禁报告。"""
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)


__all__ = [
    "HarnessBenchmarkRunner",
    "load_benchmark_manifest",
    "render_json",
    "render_junit_xml",
    "render_markdown",
    "write_reports_atomically",
]
