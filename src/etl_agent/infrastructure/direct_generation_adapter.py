import asyncio
import json
import re
from typing import Any

from pydantic import SecretStr

from etl_agent.application.generation import (
    GenerationRequest,
    GenerationResult,
    PipelineGenerationPort,
)
from etl_agent.application.policy import require_safe_generation_context
from etl_agent.domain.enums import GenerationKind

DIRECT_GENERATOR_VERSION = "direct-openai-compatible-v1"
_QUESTION = re.compile(r"(?is)^\s*QUESTION\s*:\s*(.+?)\s*$")
_FENCED_HOCON = re.compile(r"(?is)```(?:hocon|conf)?\s*\n(.*?)```")


class DirectOpenAICompatibleGenerationAdapter(PipelineGenerationPort):
    """用一次 OpenAI 兼容调用生成候选 HOCON，治理仍由外层 Harness 完成。"""

    def __init__(
        self,
        *,
        api_key: SecretStr | None,
        base_url: str | None,
        model: str | None,
        timeout_seconds: float = 180,
        client: Any | None = None,
    ) -> None:
        """初始化通用模型客户端；测试可注入假客户端且不会读取真实凭据。"""
        if client is None:
            if api_key is None or not api_key.get_secret_value():
                raise ValueError("ETL_AGENT_LLM_API_KEY 未配置")
            if not base_url:
                raise ValueError("ETL_AGENT_LLM_BASE_URL 未配置")
            if not model:
                raise ValueError("ETL_AGENT_LLM_MODEL 未配置")
            import openai

            client = openai.AsyncOpenAI(
                api_key=api_key.get_secret_value(),
                base_url=base_url,
                timeout=timeout_seconds,
            )
        self._client: Any = client
        self._model = model or "test-model"
        self._timeout_seconds = timeout_seconds
        self._provider_calls = 0
        self._last_diagnostic: dict[str, Any] = {}

    async def generate(self, request: GenerationRequest) -> GenerationResult:
        """在硬超时内生成单份候选；禁止模型输出明文凭据并保留澄清语义。"""
        require_safe_generation_context(request.metadata_context)
        messages = self._messages(request)
        try:
            async with asyncio.timeout(self._timeout_seconds):
                self._provider_calls += 1
                response = await self._client.chat.completions.create(
                    model=self._model,
                    messages=messages,
                    temperature=0.1,
                    max_tokens=16384,
                )
        except TimeoutError as exc:
            self._last_diagnostic = {"finish_reason": "timeout"}
            raise RuntimeError(
                f"直接生成超过 Harness 单次超时（{self._timeout_seconds:g} 秒）"
            ) from exc

        choice = response.choices[0]
        message = choice.message
        content = str(getattr(message, "content", None) or "").strip()
        reasoning = str(getattr(message, "reasoning_content", None) or "").strip()
        self._last_diagnostic = {
            "finish_reason": getattr(choice, "finish_reason", None) or "unknown",
            "content_chars": len(content),
            "reasoning_chars": len(reasoning),
        }
        hocon = _extract_complete_hocon(content) or _extract_complete_hocon(reasoning)
        if hocon:
            return GenerationResult(
                kind=GenerationKind.CONFIG,
                hocon=hocon,
                explanation="由单次结构化生成器产生，已交由外层确定性门禁复核。",
                upstream_version=DIRECT_GENERATOR_VERSION,
            )

        question = _question_from_text(content) or _question_from_text(reasoning)
        if question:
            return GenerationResult(
                kind=GenerationKind.QUESTION,
                question=question,
                upstream_version=DIRECT_GENERATOR_VERSION,
            )
        return GenerationResult(
            kind=GenerationKind.ERROR,
            explanation=(
                "直接生成器未返回完整 SeaTunnel HOCON"
                f"（finish_reason={getattr(choice, 'finish_reason', None) or 'unknown'}，"
                f"正文 {len(content)} 字符，推理 {len(reasoning)} 字符）"
            ),
            upstream_version=DIRECT_GENERATOR_VERSION,
        )

    def generation_diagnostics(self) -> dict[str, Any]:
        """返回可持久化的安全摘要，不包含 Prompt、模型正文、HOCON 或凭据。"""
        return {
            "provider_calls": self._provider_calls,
            **self._last_diagnostic,
        }

    @staticmethod
    def _messages(request: GenerationRequest) -> list[dict[str, str]]:
        """构造稳定提示词，把模型职责限制为候选配置生成而非审批或执行。"""
        system = (
            "You generate one production Apache SeaTunnel 2.3.13 HOCON candidate. "
            "The metadata context is authoritative. Copy every managed path, schema, sink, "
            "routing label, and credential placeholder exactly. Never emit cleartext secrets. "
            "Use only documented connector/transform options stated in the context. Return only "
            "one complete configuration inside a ```hocon``` block. If a truly required business "
            "decision is absent, return exactly `QUESTION: <question>` instead. The outer Harness "
            "will perform policy checks, engine validation, approval, and execution."
        )
        messages: list[dict[str, str]] = [{"role": "system", "content": system}]
        for item in request.conversation_history:
            text = "\n".join(
                str(block.get("text") or "") for block in item.content if block.get("text")
            )
            if text:
                role = item.role if item.role in {"user", "assistant"} else "user"
                messages.append({"role": role, "content": text})
        payload = {
            "requirement": request.requirement,
            "metadata_context": request.metadata_context,
        }
        messages.append(
            {
                "role": "user",
                "content": json.dumps(payload, ensure_ascii=False, sort_keys=True),
            }
        )
        return messages


def _extract_complete_hocon(value: str) -> str | None:
    """提取且仅接受结构闭合的 HOCON，并正确处理 SQL 三引号和注释。"""
    if not value:
        return None
    normalized = value.replace("\r\n", "\n").replace("\r", "\n")
    fenced = _FENCED_HOCON.search(normalized)
    candidate = fenced.group(1).strip() if fenced else value.strip()
    if not all(re.search(rf"(?i)\b{name}\s*\{{", candidate) for name in ("env", "source", "sink")):
        return None

    depth = 0
    quote: str | None = None
    triple_quote = False
    escaped = False
    line_comment = False
    index = 0
    while index < len(candidate):
        char = candidate[index]
        if line_comment:
            if char == "\n":
                line_comment = False
            index += 1
            continue
        if quote is not None:
            if triple_quote and candidate.startswith(quote * 3, index):
                quote = None
                triple_quote = False
                index += 3
                continue
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif not triple_quote and char == quote:
                quote = None
            index += 1
            continue
        if candidate.startswith("//", index) or char == "#":
            line_comment = True
            index += 2 if candidate.startswith("//", index) else 1
            continue
        if candidate.startswith('"""', index):
            quote = '"'
            triple_quote = True
            index += 3
            continue
        if char in "'\"":
            quote = char
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth < 0:
                return None
        index += 1
    return candidate if depth == 0 and quote is None else None


def _question_from_text(value: str) -> str | None:
    """识别受控澄清协议，普通解释文本不能被误当成人工输入请求。"""
    match = _QUESTION.fullmatch(value)
    return match.group(1).strip() if match else None
