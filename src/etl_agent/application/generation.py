from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field

from etl_agent.domain.enums import GenerationKind


class ConversationMessage(BaseModel):
    role: str
    content: list[dict[str, Any]]


class GenerationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requirement: str = Field(min_length=1, max_length=20_000)
    metadata_context: dict[str, Any] = Field(default_factory=dict)
    conversation_history: list[ConversationMessage] = Field(default_factory=list)
    trace_id: str | None = None


class GenerationResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: GenerationKind
    question: str | None = None
    hocon: str | None = None
    explanation: str | None = None
    dry_run: dict[str, Any] | None = None
    upstream_version: str


class PipelineGenerationPort(Protocol):
    async def generate(self, request: GenerationRequest) -> GenerationResult:
        """根据脱敏上下文生成澄清问题或可校验的 SeaTunnel 配置。"""
        ...
