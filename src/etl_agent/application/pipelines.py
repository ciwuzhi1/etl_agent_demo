"""Pipeline 应用服务：版本冻结、生成记录与确定性门禁。

本章实现 PipelineService，负责：
- 创建 Pipeline 和首个 DRAFT 版本
- 记录生成的 Artifact（HOCON + Plan）并计算 SHA-256
- 执行状态机转换（GENERATED -> VALIDATED）
- 审计事件记录

所有状态转换必须通过 require_pipeline_transition()；
制品保存前必须通过 require_generation_contract() 校验。
"""

import hashlib
import json
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.policy import require_no_hardcoded_secrets
from etl_agent.application.schema_contract import (
    require_database_endpoint_contract,
    require_hdfs_file_contract,
    require_managed_file_source_contract,
    require_profile_topology_contract,
    require_target_schema_contract,
)
from etl_agent.domain.enums import PipelineVersionStatus, ProjectRole
from etl_agent.domain.errors import PermissionDenied, ResourceNotFound
from etl_agent.domain.state_machine import require_pipeline_transition
from etl_agent.infrastructure.models import (
    AuditEvent,
    Pipeline,
    PipelineArtifact,
    PipelineVersion,
    ProjectMembership,
)


def artifact_digest(hocon: str, plan: dict[str, Any]) -> str:
    """对规范化 Artifact 计算 SHA-256，用于审批绑定和防篡改校验。
    
    使用 canonical JSON（sorted keys）确保同一计划无论序列化顺序如何，
    都能得到相同的摘要。
    """
    canonical_plan = json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(f"{canonical_plan}\n---HOCON---\n{hocon}".encode()).hexdigest()


def require_generation_contract(plan: dict[str, Any], hocon: str) -> None:
    """生成制品的纯确定性门禁，供 Agent 和外部导入路径共用。
    
    第 9 章简化版：只校验核心契约，不包含质量、增量、行级 LLM 等高级功能。
    """
    # 安全策略：禁止硬编码秘密
    require_no_hardcoded_secrets(hocon)
    
    # Schema 契约：拓扑对齐
    require_profile_topology_contract(plan.get("profile_contracts", []), hocon)
    
    # Schema 契约：受管文件源
    require_managed_file_source_contract(plan.get("profile_contracts", []), hocon)
    
    # Schema 契约：HDFS 端点
    require_hdfs_file_contract(plan.get("profile_contracts", []), hocon)
    
    # Schema 契约：数据库端点
    require_database_endpoint_contract(plan.get("profile_contracts", []), hocon)
    
    # Schema 契约：目标表结构对齐
    require_target_schema_contract(plan.get("profile_contracts", []), hocon)


class PipelineService:
    """Pipeline 版本生命周期管理。
    
    负责创建 Pipeline、记录生成结果、执行状态转换，并确保所有操作
    符合领域规则和安全策略。
    """

    async def create(
        self,
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        actor_id: uuid.UUID,
        name: str,
        requirement: str,
        trace_id: str | None = None,
    ) -> PipelineVersion:
        """创建 Pipeline 和首个 DRAFT 版本。
        
        Args:
            session: 数据库会话
            project_id: 项目 ID
            actor_id: 操作用户 ID
            name: Pipeline 名称
            requirement: 需求描述文本
            trace_id: 追踪 ID（可选）
            
        Returns:
            新创建的 PipelineVersion 对象
            
        Raises:
            PermissionDenied: 如果用户没有 DATA_ENGINEER 或 ADMIN 角色
        """
        # 权限校验
        await self._require_engineer(session, project_id, actor_id)
        
        # 创建 Pipeline 实体
        pipeline = Pipeline(project_id=project_id, name=name)
        session.add(pipeline)
        await session.flush()
        
        # 创建 v1 版本（DRAFT 状态）
        version = PipelineVersion(
            pipeline_id=pipeline.id,
            version_number=1,
            requirement=requirement,
            created_by=actor_id,
        )
        session.add(version)
        await session.flush()
        
        # 记录审计事件
        session.add(
            AuditEvent(
                actor_id=actor_id,
                project_id=project_id,
                action="pipeline.created",
                resource_type="Pipeline",
                resource_id=pipeline.id,
                before=None,
                after={"name": name, "version_id": str(version.id)},
                trace_id=trace_id,
            )
        )
        await session.flush()
        
        return version

    async def record_generation(
        self,
        session: AsyncSession,
        *,
        version_id: uuid.UUID,
        actor_id: uuid.UUID,
        plan: dict[str, Any],
        hocon: str,
        artifact_uri: str,
        upstream_version: str,
        trace_id: str | None = None,
    ) -> PipelineVersion:
        """保存服务端生成结果和不可变 Artifact。
        
        流程：
        1. 校验生成契约（require_generation_contract）
        2. 计算制品 SHA-256
        3. 保存 PipelineArtifact
        4. 状态转换：DRAFT/PROFILING -> GENERATED
        
        Args:
            session: 数据库会话
            version_id: 版本 ID
            actor_id: 操作用户 ID
            plan: 结构化执行计划
            hocon: SeaTunnel 配置文件内容
            artifact_uri: 制品存储 URI
            upstream_version: 上游版本标识（如 Seatunnel CLI commit）
            trace_id: 追踪 ID（可选）
            
        Returns:
            更新后的 PipelineVersion 对象
            
        Raises:
            DomainError: 如果契约校验失败
            InvalidStateTransition: 如果当前状态不允许生成
            PermissionDenied: 如果用户没有权限
        """
        # 1. 校验生成契约（在修改版本前先验证）
        require_generation_contract(plan, hocon)
        
        # 2. 获取版本并校验权限
        version, project_id = await self._version_and_project(session, version_id)
        await self._require_engineer(session, project_id, actor_id)
        
        # 3. 状态转换校验
        require_pipeline_transition(version.status, PipelineVersionStatus.GENERATED)
        
        # 4. 计算制品摘要
        digest = artifact_digest(hocon, plan)
        
        # 5. 保存状态和制品
        before = self._version_state(version)
        version.status = PipelineVersionStatus.GENERATED
        version.plan = plan
        version.artifact_sha256 = digest
        version.lock_version += 1
        
        session.add(
            PipelineArtifact(
                pipeline_version_id=version.id,
                kind="SEATUNNEL_HOCON",
                uri=artifact_uri,
                sha256=digest,
                metadata_json={"seatunnel_cli_commit": upstream_version},
            )
        )
        
        # 6. 记录审计事件
        self._add_transition_audit(
            session,
            version=version,
            project_id=project_id,
            actor_id=actor_id,
            action="pipeline_version.generated",
            before=before,
            trace_id=trace_id,
        )
        await session.flush()
        
        return version

    async def validate(
        self,
        session: AsyncSession,
        *,
        version_id: uuid.UUID,
        actor_id: uuid.UUID,
        trace_id: str | None = None,
    ) -> PipelineVersion:
        """执行确定性输入与契约校验，将版本推进到 VALIDATED 状态。
        
        Args:
            session: 数据库会话
            version_id: 版本 ID
            actor_id: 操作用户 ID
            trace_id: 追踪 ID（可选）
            
        Returns:
            更新后的 PipelineVersion 对象
            
        Raises:
            InvalidStateTransition: 如果当前状态不是 GENERATED
            PermissionDenied: 如果用户没有权限
        """
        return await self._transition(
            session,
            version_id=version_id,
            actor_id=actor_id,
            target=PipelineVersionStatus.VALIDATED,
            action="pipeline_version.validated",
            trace_id=trace_id,
        )

    async def _transition(
        self,
        session: AsyncSession,
        *,
        version_id: uuid.UUID,
        actor_id: uuid.UUID,
        target: PipelineVersionStatus,
        action: str,
        trace_id: str | None = None,
    ) -> PipelineVersion:
        """校验并执行 Pipeline 状态迁移，使状态写入和审计记录处于同一事务。
        
        Args:
            session: 数据库会话
            version_id: 版本 ID
            actor_id: 操作用户 ID
            target: 目标状态
            action: 审计动作名称
            trace_id: 追踪 ID（可选）
            
        Returns:
            更新后的 PipelineVersion 对象
        """
        version, project_id = await self._version_and_project(session, version_id)
        await self._require_engineer(session, project_id, actor_id)
        require_pipeline_transition(version.status, target)
        
        before = self._version_state(version)
        version.status = target
        version.lock_version += 1
        
        self._add_transition_audit(
            session,
            version=version,
            project_id=project_id,
            actor_id=actor_id,
            action=action,
            before=before,
            trace_id=trace_id,
        )
        await session.flush()
        
        return version

    @staticmethod
    async def _version_and_project(
        session: AsyncSession, version_id: uuid.UUID
    ) -> tuple[PipelineVersion, uuid.UUID]:
        """联表解析版本与所属项目，统一处理资源缺失和跨项目访问。
        
        使用 SELECT FOR UPDATE 防止并发修改冲突。
        
        Args:
            session: 数据库会话
            version_id: 版本 ID
            
        Returns:
            (PipelineVersion, project_id) 元组
            
        Raises:
            ResourceNotFound: 如果版本不存在
        """
        statement = (
            select(PipelineVersion, Pipeline.project_id)
            .join(Pipeline, Pipeline.id == PipelineVersion.pipeline_id)
            .where(PipelineVersion.id == version_id)
            .with_for_update()
        )
        row = (await session.execute(statement)).one_or_none()
        if row is None:
            raise ResourceNotFound("Pipeline 版本不存在")
        return row[0], row[1]

    @staticmethod
    async def _require_engineer(
        session: AsyncSession, project_id: uuid.UUID, actor_id: uuid.UUID
    ) -> None:
        """校验用户具有修改 Pipeline 的权限。
        
        只有 ADMIN 和 DATA_ENGINEER 角色可以创建或修改 Pipeline。
        
        Args:
            session: 数据库会话
            project_id: 项目 ID
            actor_id: 用户 ID
            
        Raises:
            PermissionDenied: 如果用户角色不符合要求
        """
        role = await session.scalar(
            select(ProjectMembership.role).where(
                ProjectMembership.project_id == project_id,
                ProjectMembership.user_id == actor_id,
            )
        )
        if role not in {ProjectRole.ADMIN, ProjectRole.DATA_ENGINEER}:
            raise PermissionDenied("只有管理员或数据工程师可以修改 Pipeline")

    @staticmethod
    def _version_state(version: PipelineVersion) -> dict[str, Any]:
        """提取版本状态的快照，用于审计记录的 before/after 字段。
        
        Args:
            version: PipelineVersion 对象
            
        Returns:
            包含 status 和 lock_version 的字典
        """
        return {"status": version.status.value, "lock_version": version.lock_version}

    def _add_transition_audit(
        self,
        session: AsyncSession,
        *,
        version: PipelineVersion,
        project_id: uuid.UUID,
        actor_id: uuid.UUID,
        action: str,
        before: dict[str, Any],
        trace_id: str | None = None,
    ) -> None:
        """添加状态转换的审计事件。
        
        Args:
            session: 数据库会话
            version: 版本对象
            project_id: 项目 ID
            actor_id: 操作用户 ID
            action: 审计动作名称
            before: 转换前的状态快照
            trace_id: 追踪 ID（可选）
        """
        session.add(
            AuditEvent(
                actor_id=actor_id,
                project_id=project_id,
                action=action,
                resource_type="PipelineVersion",
                resource_id=version.id,
                before=before,
                after=self._version_state(version),
                trace_id=trace_id,
            )
        )
