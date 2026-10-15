"""生成适配器工厂，根据配置实例化对应的后端。"""

from etl_agent.application.generation import PipelineGenerationPort
from etl_agent.config import get_settings

settings = get_settings()


def create_generation_adapter() -> PipelineGenerationPort:
    """根据 ETL_AGENT_GENERATION_BACKEND 配置创建对应的适配器。

    Returns:
        PipelineGenerationPort 实例

    Raises:
        ValueError: 配置的后端类型不支持
    """
    backend = settings.generation_backend.lower()

    if backend == "mock":
        from etl_agent.infrastructure.mock_generation_adapter import MockGenerationAdapter

        return MockGenerationAdapter(mode="config")
    elif backend == "direct":
        from etl_agent.infrastructure.direct_generation_adapter import (
            DirectOpenAICompatibleGenerationAdapter,
        )

        api_key = settings.llm_api_key  # Already SecretStr | None
        base_url = str(settings.llm_base_url) if settings.llm_base_url else None
        return DirectOpenAICompatibleGenerationAdapter(
            api_key=api_key,
            base_url=base_url,
            model=settings.llm_model,
            timeout_seconds=settings.llm_timeout_seconds,
        )
    elif backend == "seatunnel-cli":
        raise NotImplementedError("SeaTunnel CLI 适配器尚未实现")
    else:
        raise ValueError(f"不支持的生成后端：{backend}（支持：mock / direct / seatunnel-cli）")
