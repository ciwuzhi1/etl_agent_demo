"""Celery tasks for Harness Outbox dispatch.

This module implements the task mapping and dispatch logic for processing
Harness commands from the outbox table, following the pattern established
in the reference implementation.
"""

from celery import current_app as celery_app

from etl_agent.application.harness_outbox import HarnessOutboxService
from etl_agent.asyncio_compat import run_async
from etl_agent.infrastructure.database import SessionFactory
from etl_agent.infrastructure.models import HarnessCommandOutbox


# Task mapping dictionary: maps command types to Celery task names
COMMAND_TYPE_TO_TASK: dict[str, str] = {
    "EXECUTION_SUBMIT": "etl_agent.tasks.submit_execution",
    "EXECUTION_CANCEL": "etl_agent.tasks.cancel_execution",
}


def _dispatch_command(command: HarnessCommandOutbox) -> None:
    """Dispatch a single Harness command to its target Celery task.

    Args:
        command: The HarnessCommandOutbox record containing the command payload.

    Raises:
        ValueError: If the command type is not supported or missing from the mapping.
    """
    task_name = COMMAND_TYPE_TO_TASK.get(command.command_payload.get("command_type"))
    if not task_name:
        raise ValueError(
            f"Unsupported Harness Outbox command type: "
            f"{command.command_payload.get('command_type')}"
        )

    # Extract trace_id if present in payload
    trace_id = command.command_payload.get("trace_id")
    trace_id_str = str(trace_id) if trace_id else None

    # Call target task asynchronously with appropriate arguments
    if command.command_payload.get("command_type") == "EXECUTION_SUBMIT":
        execution_run_id = command.command_payload.get("execution_run_id")
        if execution_run_id:
            celery_app.send_task(task_name, args=[str(execution_run_id), trace_id_str])
    elif command.command_payload.get("command_type") == "EXECUTION_CANCEL":
        aggregate_id = str(command.aggregate_id) if hasattr(command, "aggregate_id") else ""
        force = bool(command.command_payload.get("force", False))
        celery_app.send_task(task_name, args=[aggregate_id, force, trace_id_str])
    else:
        # For other mapped commands, send with minimal payload
        celery_app.send_task(task_name, args=[command.command_payload])


async def _dispatch_harness_outbox() -> int:
    """Claim and dispatch Harness commands from the outbox table.

    This async function:
    1. Claims pending/failed commands from the outbox
    2. Dispatches each command to its target task
    3. Marks commands as SENT or FAILED based on dispatch result

    Returns:
        Number of successfully dispatched commands.
    """
    service = HarnessOutboxService()
    async with SessionFactory() as session:
        async with session.begin():
            claimed = await service.claim_pending(session)

    sent = 0
    for command in claimed:
        try:
            _dispatch_command(command)
        except Exception as exc:
            async with SessionFactory() as session:
                async with session.begin():
                    await service.mark_failed(
                        session,
                        outbox_id=command.id,
                        error=str(exc),
                    )
            continue

        async with SessionFactory() as session:
            async with session.begin():
                await service.mark_sent(session, outbox_id=command.id)
        sent += 1

    return sent


@celery_app.task(name="harness.dispatch-outbox-command")
def dispatch_outbox_command(command_id: str) -> None:
    """Dispatch a single Harness outbox command by ID.

    This Celery task fetches a specific command from the harness_command_outbox
    table, looks up the target task in COMMAND_TYPE_TO_TASK, calls it
    asynchronously, and marks the command as SENT or FAILED.

    Args:
        command_id: UUID string of the HarnessCommandOutbox record to dispatch.
    """
    import uuid

    from sqlalchemy import select

    outbox_uuid = uuid.UUID(command_id)

    async def _process_single_command() -> None:
        async with SessionFactory() as session:
            async with session.begin():
                command = await session.scalar(
                    select(HarnessCommandOutbox).where(
                        HarnessCommandOutbox.id == outbox_uuid
                    )
                )
                if command is None:
                    raise ValueError(f"Harness outbox command not found: {command_id}")

                _dispatch_command(command)
                await HarnessOutboxService().mark_sent(session, outbox_id=command.id)

    run_async(_process_single_command())


@celery_app.task(name="etl_agent.harness_outbox_tasks.dispatch_harness_outbox")
def dispatch_harness_outbox() -> int:
    """Celery task entry point for batch dispatching Harness outbox commands.

    This task is designed to be called by Celery Beat or manually triggered
    to process all pending commands in the outbox table.

    Returns:
        Number of commands successfully dispatched.
    """
    return run_async(_dispatch_harness_outbox())
