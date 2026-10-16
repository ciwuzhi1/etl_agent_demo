"""SeaTunnel Zeta REST Adapter for Chapter 10."""

from typing import Any, Protocol, runtime_checkable

import httpx
from pydantic import BaseModel, Field

from etl_agent.domain.errors import DomainError


class SeatunnelSubmitRequest(BaseModel):
    """Request to submit a SeaTunnel job."""

    hocon: str
    job_name: str | None = None


class SeatunnelJobInfo(BaseModel):
    """Information about a SeaTunnel job."""

    job_id: str
    status: str
    create_time: str | None = None
    # 引擎原始指标（SourceReceivedCount / SinkCommittedCount 等）。
    # 归一化交给 application/execution_metrics，本层只原样透传，不做解释。
    metrics: dict[str, Any] = Field(default_factory=dict)


@runtime_checkable
class SeatunnelExecutionPort(Protocol):
    """Protocol for SeaTunnel execution adapters."""

    async def submit(self, request: SeatunnelSubmitRequest) -> str:
        """Submit a SeaTunnel job and return the job_id."""
        ...

    async def get_job(self, job_id: str) -> SeatunnelJobInfo:
        """Retrieve information about a SeaTunnel job."""
        ...

    async def cancel(self, job_id: str, force: bool = False) -> None:
        """Cancel a running SeaTunnel job."""
        ...


class SeaTunnelApiError(DomainError):
    """Error raised when SeaTunnel API calls fail."""


class ZetaRestAdapter(SeatunnelExecutionPort):
    """REST adapter for SeaTunnel Zeta engine."""

    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        """Initialize the adapter with connection parameters.

        Args:
            base_url: Base URL of the SeaTunnel Zeta REST API.
            api_key: Optional API key for authentication.
            timeout_seconds: Request timeout in seconds.
        """
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=httpx.Timeout(timeout_seconds),
        )

    async def submit(self, request: SeatunnelSubmitRequest) -> str:
        """Submit a SeaTunnel job and return the job_id.

        Args:
            request: The job submission request containing HOCON config and optional name.

        Returns:
            The SeaTunnel job_id string.

        Raises:
            SeaTunnelApiError: If the API call fails or response is invalid.
        """
        try:
            headers: dict[str, str] = {
                "Content-Type": "text/plain; charset=utf-8",
            }
            if self._api_key:
                headers["Authorization"] = f"Bearer {self._api_key}"

            params: dict[str, Any] = {
                "format": "hocon",
            }
            if request.job_name:
                params["jobName"] = request.job_name

            response = await self._client.post(
                "/submit-job",
                params=params,
                content=request.hocon.encode("utf-8"),
                headers=headers,
            )
            response.raise_for_status()
            payload = response.json()

            job_id = payload.get("jobId")
            if not job_id:
                raise SeaTunnelApiError("SeaTunnel submit response missing jobId")

            return str(job_id)

        except httpx.ConnectError as exc:
            raise SeaTunnelApiError(f"Connection refused: {exc}") from exc
        except httpx.TimeoutException as exc:
            raise SeaTunnelApiError(f"Request timed out: {exc}") from exc
        except httpx.HTTPStatusError as exc:
            raise SeaTunnelApiError(f"HTTP error {exc.response.status_code}: {exc}") from exc
        except ValueError as exc:
            raise SeaTunnelApiError(f"Invalid JSON response: {exc}") from exc

    async def get_job(self, job_id: str) -> SeatunnelJobInfo:
        """Retrieve information about a SeaTunnel job.

        Args:
            job_id: The SeaTunnel job identifier.

        Returns:
            SeatunnelJobInfo with job details.

        Raises:
            SeaTunnelApiError: If the job doesn't exist or API call fails.
        """
        try:
            headers: dict[str, str] = {}
            if self._api_key:
                headers["Authorization"] = f"Bearer {self._api_key}"

            response = await self._client.get(
                f"/job-info/{job_id}",
                headers=headers,
            )
            response.raise_for_status()
            payload = response.json()

            if not payload.get("jobId"):
                raise SeaTunnelApiError(f"SeaTunnel job {job_id} not found")

            status = payload.get("jobStatus")
            if not status:
                raise SeaTunnelApiError(f"SeaTunnel job {job_id} has no status")

            create_time = payload.get("createTime")
            metrics = payload.get("metrics")

            return SeatunnelJobInfo(
                job_id=str(payload["jobId"]),
                status=str(status),
                create_time=str(create_time) if create_time else None,
                metrics=metrics if isinstance(metrics, dict) else {},
            )

        except httpx.ConnectError as exc:
            raise SeaTunnelApiError(f"Connection refused: {exc}") from exc
        except httpx.TimeoutException as exc:
            raise SeaTunnelApiError(f"Request timed out: {exc}") from exc
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise SeaTunnelApiError(f"SeaTunnel job {job_id} not found") from exc
            raise SeaTunnelApiError(f"HTTP error {exc.response.status_code}: {exc}") from exc
        except ValueError as exc:
            raise SeaTunnelApiError(f"Invalid JSON response: {exc}") from exc

    async def cancel(self, job_id: str, force: bool = False) -> None:
        """Cancel a running SeaTunnel job.

        Args:
            job_id: The SeaTunnel job identifier to cancel.
            force: Whether to force cancellation without savepoint.

        Raises:
            SeaTunnelApiError: If the API call fails.
        """
        try:
            headers: dict[str, str] = {}
            if self._api_key:
                headers["Authorization"] = f"Bearer {self._api_key}"

            response = await self._client.post(
                "/stop-job",
                json={
                    "jobId": int(job_id),
                    "isStopWithSavePoint": not force,
                    "force": force,
                },
                headers=headers,
            )
            response.raise_for_status()

        except httpx.ConnectError as exc:
            raise SeaTunnelApiError(f"Connection refused: {exc}") from exc
        except httpx.TimeoutException as exc:
            raise SeaTunnelApiError(f"Request timed out: {exc}") from exc
        except httpx.HTTPStatusError as exc:
            raise SeaTunnelApiError(f"HTTP error {exc.response.status_code}: {exc}") from exc
        except ValueError as exc:
            raise SeaTunnelApiError(f"Invalid job_id format: {exc}") from exc

    async def aclose(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()
