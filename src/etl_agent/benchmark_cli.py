"""离线冻结 Benchmark 的命令行入口（第 14 章）。

不依赖数据库、网络或大模型，直接在本地执行确定性场景并原子写出 JSON/JUnit/Markdown 报告：

    python -m etl_agent.benchmark_cli --manifest path/to/manifest.json --output var/benchmarks

门禁全部通过返回退出码 0，任何回归阻断返回退出码 1，便于 CI 直接判断发布放行。
"""

import argparse
import asyncio
import sys
from pathlib import Path

from etl_agent.application.benchmark import (
    HarnessBenchmarkRunner,
    load_benchmark_manifest,
    write_reports_atomically,
)
from etl_agent.application.harness_policy import EmbeddedPolicyDecisionPoint


def build_parser() -> argparse.ArgumentParser:
    """构造只接受冻结场景清单与输出目录的最小参数集，拒绝执行任意 Python 调用。"""
    parser = argparse.ArgumentParser(
        prog="etl-agent-benchmark", description="运行冻结 Harness Benchmark"
    )
    parser.add_argument(
        "--manifest", required=True, type=Path, help="版本化 Benchmark 场景清单 JSON 路径"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("var/benchmarks"),
        help="报告输出目录（默认 var/benchmarks）",
    )
    return parser


def _run(manifest_path: Path, output_dir: Path) -> int:
    """加载清单、离线执行两次复现校验并写出报告，返回进程退出码。"""
    manifest = load_benchmark_manifest(manifest_path)
    report = asyncio.run(HarnessBenchmarkRunner().run(manifest, EmbeddedPolicyDecisionPoint()))
    written = write_reports_atomically(report, output_dir)
    print(f"suite={report.suite_id} version={report.version} gates_passed={report.gates_passed}")
    for path in written:
        print(f"wrote {path}")
    for failure in report.gate_failures:
        print(f"gate-failure: {failure}", file=sys.stderr)
    return 0 if report.gates_passed else 1


def main(argv: list[str] | None = None) -> int:
    """解析参数并运行 Benchmark；返回退出码供控制台脚本直接使用。"""
    args = build_parser().parse_args(argv)
    return _run(args.manifest, args.output)


if __name__ == "__main__":
    raise SystemExit(main())
