"""第 7 章生成适配器单元测试，使用 Mock 后端验证接口契约。"""

import pytest

from etl_agent.application.generation import GenerationRequest, GenerationResult
from etl_agent.domain.enums import GenerationKind
from etl_agent.infrastructure.mock_generation_adapter import MockGenerationAdapter


@pytest.fixture
def mock_config_adapter() -> MockGenerationAdapter:
    """返回 CONFIG 模式的 Mock 适配器。"""
    return MockGenerationAdapter(mode="config")


@pytest.fixture
def mock_question_adapter() -> MockGenerationAdapter:
    """返回 QUESTION 模式的 Mock 适配器。"""
    return MockGenerationAdapter(mode="question", question="目标表的分区键是什么？")


@pytest.fixture
def mock_error_adapter() -> MockGenerationAdapter:
    """返回 ERROR 模式的 Mock 适配器。"""
    return MockGenerationAdapter(mode="error", error_message="模拟生成失败")


@pytest.fixture
def sample_request() -> GenerationRequest:
    """构造标准的生成请求。"""
    return GenerationRequest(
        requirement="将 MySQL orders 表清洗后写入 Doris",
        metadata_context={
            "sources": [
                {
                    "connector_type": "MYSQL",
                    "schema": "source_db",
                    "table": "orders",
                    "columns": [
                        {"name": "order_id", "type": "bigint"},
                        {"name": "email", "type": "varchar(255)"},
                    ],
                }
            ],
            "sinks": [
                {
                    "connector_type": "DORIS",
                    "schema": "analytics",
                    "table": "clean_orders",
                }
            ],
        },
    )


async def test_mock_config_mode_returns_hocon(
    mock_config_adapter: MockGenerationAdapter, sample_request: GenerationRequest
) -> None:
    """验证 CONFIG 模式返回完整 HOCON。"""
    result = await mock_config_adapter.generate(sample_request)

    assert result.kind == GenerationKind.CONFIG
    assert result.hocon is not None
    assert "env" in result.hocon
    assert "source" in result.hocon
    assert "sink" in result.hocon
    assert result.explanation is not None
    assert result.upstream_version == "mock-generator-v1"


async def test_mock_question_mode_returns_question(
    mock_question_adapter: MockGenerationAdapter, sample_request: GenerationRequest
) -> None:
    """验证 QUESTION 模式返回澄清问题。"""
    result = await mock_question_adapter.generate(sample_request)

    assert result.kind == GenerationKind.QUESTION
    assert result.question == "目标表的分区键是什么？"
    assert result.hocon is None
    assert result.upstream_version == "mock-generator-v1"


async def test_mock_error_mode_returns_error(
    mock_error_adapter: MockGenerationAdapter, sample_request: GenerationRequest
) -> None:
    """验证 ERROR 模式返回错误说明。"""
    result = await mock_error_adapter.generate(sample_request)

    assert result.kind == GenerationKind.ERROR
    assert result.explanation == "模拟生成失败"
    assert result.hocon is None
    assert result.upstream_version == "mock-generator-v1"


async def test_mock_adapter_tracks_call_count(
    mock_config_adapter: MockGenerationAdapter, sample_request: GenerationRequest
) -> None:
    """验证 Mock 适配器记录调用次数。"""
    diagnostics_before = mock_config_adapter.generation_diagnostics()
    assert diagnostics_before["provider_calls"] == 0

    await mock_config_adapter.generate(sample_request)
    await mock_config_adapter.generate(sample_request)

    diagnostics_after = mock_config_adapter.generation_diagnostics()
    assert diagnostics_after["provider_calls"] == 2


async def test_generation_result_structure(
    mock_config_adapter: MockGenerationAdapter, sample_request: GenerationRequest
) -> None:
    """验证 GenerationResult 结构符合 Pydantic 约束。"""
    from pydantic import ValidationError

    result = await mock_config_adapter.generate(sample_request)

    # 验证不允许额外字段
    with pytest.raises(ValidationError):
        GenerationResult.model_validate({"kind": "CONFIG", "extra_field": "not allowed"})

    # 验证必填字段
    assert result.kind is not None
    assert result.upstream_version is not None


def test_default_hocon_contains_required_blocks() -> None:
    """验证默认 HOCON 包含 env/source/sink 三个必需块。"""
    adapter = MockGenerationAdapter(mode="config")
    hocon = adapter._hocon  # type: ignore[attr-defined]

    assert "env {" in hocon
    assert "source {" in hocon
    assert "sink {" in hocon
    assert "Jdbc" in hocon
    assert "Doris" in hocon
