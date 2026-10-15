"""Mock 生成适配器，用于单元测试和本地开发，不调用真实 LLM API。"""

from typing import Any

from etl_agent.application.generation import (
    GenerationRequest,
    GenerationResult,
    PipelineGenerationPort,
)
from etl_agent.domain.enums import GenerationKind

MOCK_GENERATOR_VERSION = "mock-generator-v1"


class MockGenerationAdapter(PipelineGenerationPort):
    """返回预定义的 HOCON 或澄清问题，用于测试和演示。"""

    def __init__(
        self,
        *,
        mode: str = "config",
        hocon: str | None = None,
        question: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """初始化 Mock 适配器。

        Args:
            mode: "config" / "question" / "error" 三种返回模式
            hocon: CONFIG 模式下返回的 HOCON 字符串
            question: QUESTION 模式下返回的澄清问题
            error_message: ERROR 模式下返回的错误说明
        """
        self._mode = mode
        self._hocon = hocon or _DEFAULT_HOCON
        self._question = question or "目标 Doris 表的分区键是什么？"
        self._error_message = error_message or "模拟生成失败"
        self._call_count = 0

    async def generate(self, request: GenerationRequest) -> GenerationResult:
        """根据初始化模式返回对应的结果。"""
        self._call_count += 1

        if self._mode == "config":
            return GenerationResult(
                kind=GenerationKind.CONFIG,
                hocon=self._hocon,
                explanation="由 Mock 生成器产生的测试配置。",
                upstream_version=MOCK_GENERATOR_VERSION,
            )
        elif self._mode == "question":
            return GenerationResult(
                kind=GenerationKind.QUESTION,
                question=self._question,
                upstream_version=MOCK_GENERATOR_VERSION,
            )
        else:
            return GenerationResult(
                kind=GenerationKind.ERROR,
                explanation=self._error_message,
                upstream_version=MOCK_GENERATOR_VERSION,
            )

    def generation_diagnostics(self) -> dict[str, Any]:
        """返回调用统计。"""
        return {"provider_calls": self._call_count}


_DEFAULT_HOCON = """
env {
  execution.parallelism = 2
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "${CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_JDBC_URL}"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "${CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_USERNAME}"
    password = "${CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_PASSWORD}"
    query = "SELECT * FROM source_db.orders"
    plugin_output = "source_rows"
  }
}

transform {
  Sql {
    plugin_input = "source_rows"
    plugin_output = "cleaned_rows"
    query = \"\"\"
      SELECT 
        order_id,
        REGEXP_REPLACE(email, '(.{3}).*@', '$1***@') AS email,
        TRIM(product_name) AS product_name,
        amount
      FROM source_rows
      WHERE order_id IS NOT NULL AND amount > 0
    \"\"\"
  }
}

sink {
  Doris {
    fenodes = "${CONNECTION_BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB_FENODES}"
    database = "analytics"
    table = "clean_orders"
    username = "${CONNECTION_BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB_USERNAME}"
    password = "${CONNECTION_BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB_PASSWORD}"
    plugin_input = "cleaned_rows"
    doris.config = {
      sink.enable-2pc = "true"
      sink.label-prefix = "etl_agent_clean_orders"
    }
  }
}
""".strip()
