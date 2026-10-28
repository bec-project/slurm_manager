"""Small authenticated client for the slurmrestd v0.0.44 API."""

from __future__ import annotations

import json
import os
import re
from http import HTTPStatus
from http.client import HTTPException
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from dotenv import dotenv_values

INVALID_JOB_ID = 2017


def _missing_job(result: dict[str, Any]) -> bool:
    """Check whether every reported Slurm error means an unknown job ID.

    Args:
        result (dict[str, Any]): Decoded Slurm REST response envelope.

    Returns:
        bool: True only for a nonempty list of invalid-job-ID errors.
    """
    errors = result.get("errors")
    return (
        isinstance(errors, list)
        and bool(errors)
        and all(
            isinstance(error, dict) and error.get("error_number") == INVALID_JOB_ID
            for error in errors
        )
    )


class SlurmRestError(RuntimeError):
    """A transport, HTTP, or Slurm application error."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        # Never forward the authentication headers to a redirect target.
        """Reject redirects so authentication headers cannot reach another endpoint.

        Args:
            *_args (Any): Positional redirect-hook arguments supplied by urllib.
            **_kwargs (Any): Keyword redirect-hook arguments supplied by urllib.

        Returns:
            None: Redirect rejection, preventing urllib from following the response.
        """
        return None


class SlurmRestClient:
    """Load connection settings and a JWT from a local dotenv file or environment.

    Environment variables override the file. The file is read again for every
    request so replacing an expired token does not require restarting a manager.
    """

    def __init__(self, env_file: str | Path = ".env", timeout: float = 30) -> None:
        """Load and validate slurmrestd connection settings.

        Args:
            env_file (str | Path): Local credential dotenv path. Environment variables
                take precedence.
            timeout (float): Positive HTTP request timeout in seconds.

        Raises:
            ValueError: The URL, API version, token, or timeout is invalid.
        """
        self.env_file = Path(env_file).resolve()
        if timeout <= 0:
            raise ValueError("REST timeout must be positive")
        self.timeout = timeout
        settings = self._settings()
        self.url = settings.get("SLURM_REST_URL", "").rstrip("/")
        parsed = urlsplit(self.url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("Set SLURM_REST_URL to the slurmrestd HTTP(S) URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("SLURM_REST_URL must not contain credentials, query, or fragment")
        self.api_version = settings.get("SLURM_REST_API_VERSION") or "v0.0.44"
        if not re.fullmatch(r"v\d+\.\d+\.\d+", self.api_version):
            raise ValueError("SLURM_REST_API_VERSION must look like v0.0.44")
        self._headers(settings)
        self._opener = build_opener(_NoRedirect())

    ################################################
    #  Public Job Operations                       #
    ################################################

    def submit(self, script: str, job: dict[str, Any]) -> dict[str, Any]:
        """Submit a batch script and its compute-node job settings.

        Args:
            script (str): Complete batch script text sent directly to slurmrestd.
            job (dict[str, Any]): REST job-description fields, including compute-node
                paths and resources.

        Returns:
            dict[str, Any]: Submission response containing a numeric job_id.

        Raises:
            SlurmRestError: Submission fails or no numeric job ID is returned.
        """
        result = self.request(
            "POST", f"slurm/{self.api_version}/job/submit", payload={"script": script, "job": job}
        )
        if not str(result.get("job_id", "")).isdigit():
            raise SlurmRestError("Slurm REST submission did not return a job ID")
        return result

    def release(self, job_id: str) -> None:
        """Release a job that was submitted with hold enabled.

        Args:
            job_id (str): Numeric scheduler job ID on this REST endpoint.

        Raises:
            SlurmRestError: The job update fails.
        """
        self.request("POST", self._job_path(job_id), payload={"hold": False})

    def cancel(self, job_id: str) -> None:
        """Cancel a job on the cluster served by this REST endpoint.

        Args:
            job_id (str): Numeric scheduler job ID on this REST endpoint.

        Raises:
            SlurmRestError: Cancellation fails.
        """
        self.request("DELETE", self._job_path(job_id))

    def get_job(self, job_id: str, *, accounting: bool = False) -> dict[str, Any] | None:
        """Fetch a controller or accounting job record.

        Args:
            job_id (str): Numeric scheduler job ID on this REST endpoint.
            accounting (bool): If true, query slurmdb instead of the controller.

        Returns:
            dict[str, Any] | None: First returned job record, or None when the job is
                unavailable.

        Raises:
            SlurmRestError: The query fails or its records are malformed.
        """
        path = self._job_path(job_id, accounting=accounting)
        result = self.request("GET", path, missing_ok=True)
        jobs = result.get("jobs") or []
        if not isinstance(jobs, list) or any(not isinstance(job, dict) for job in jobs):
            raise SlurmRestError("Slurm REST returned invalid job records")
        return jobs[0] if jobs else None

    ################################################
    #  Public HTTP Requests                        #
    ################################################

    def request(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        missing_ok: bool = False,
    ) -> dict[str, Any]:
        """Send one authenticated JSON request and validate the response envelope.

        Args:
            method (str): HTTP method such as GET, POST, or DELETE.
            path (str): Relative REST endpoint appended to the configured service URL.
            payload (dict[str, Any] | None): JSON-compatible request body or incoming
                message payload.
            missing_ok (bool): If true, treat missing-job responses as an empty result.

        Returns:
            dict[str, Any]: Decoded response object; an empty object for an allowed
                missing job.

        Raises:
            SlurmRestError: HTTP, transport, JSON, or Slurm application validation
                fails.
            ValueError: The current token is missing.

        Notes:
            Requests are not automatically retried. Authentication headers and response
            bodies are not included in raised transport errors.
        """
        headers = self._headers(self._settings())
        data = None if payload is None else json.dumps(payload).encode()
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = Request(f"{self.url}/{path}", data=data, headers=headers, method=method)
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                raw = response.read()
        except HTTPError as exc:
            try:
                # slurmrestd may use HTTP 500 for a purged controller job. Check
                # only its numeric error code; never include the body in logs.
                error_body = json.loads(exc.read()) if missing_ok else {}
            except (ValueError, UnicodeDecodeError, OSError, HTTPException):
                error_body = {}
            finally:
                exc.close()
            if missing_ok and (
                exc.code == HTTPStatus.NOT_FOUND
                or (
                    exc.code in {HTTPStatus.BAD_REQUEST, HTTPStatus.INTERNAL_SERVER_ERROR}
                    and isinstance(error_body, dict)
                    and _missing_job(error_body)
                )
            ):
                return {}
            raise SlurmRestError(f"Slurm REST {method} failed (HTTP {exc.code})") from None
        except (URLError, OSError, TimeoutError, HTTPException):
            raise SlurmRestError(f"Slurm REST {method} failed (connection or timeout)") from None
        try:
            result = json.loads(raw) if raw else {}
        except (ValueError, UnicodeDecodeError):
            raise SlurmRestError("Slurm REST returned invalid JSON") from None
        if not isinstance(result, dict):
            raise SlurmRestError("Slurm REST returned an invalid response")
        errors = result.get("errors") or []
        if errors:
            if missing_ok and _missing_job(result):
                return {}
            raise SlurmRestError("Slurm REST reported an application error")
        return result

    ################################################
    #  Internal Authentication                     #
    ################################################

    def _settings(self) -> dict[str, str]:
        """Read local dotenv values and apply environment-variable overrides.

        Returns:
            dict[str, str]: Connection and authentication settings without modifying
                os.environ.
        """
        values = dotenv_values(self.env_file, interpolate=False)
        keys = ("SLURM_REST_URL", "SLURM_REST_API_VERSION", "SLURM_JWT", "SLURM_USER")
        return {key: os.environ.get(key, values.get(key) or "") for key in keys}

    @staticmethod
    def _headers(settings: dict[str, str]) -> dict[str, str]:
        """Build authentication headers from the current settings.

        Args:
            settings (dict[str, str]): Connection settings containing the JWT and
                optional user name.

        Returns:
            dict[str, str]: JWT, optional user-name, and response-format headers.

        Raises:
            ValueError: The token is empty.
        """
        token = settings.get("SLURM_JWT", "")
        if not token:
            raise ValueError("Set SLURM_JWT in the local .env file or environment")
        headers = {"X-SLURM-USER-TOKEN": token, "Accept": "application/json"}
        if settings.get("SLURM_USER"):
            headers["X-SLURM-USER-NAME"] = settings["SLURM_USER"]
        return headers

    ################################################
    #  Internal Endpoints                          #
    ################################################

    def _job_path(self, job_id: str, *, accounting: bool = False) -> str:
        """Build a versioned controller or accounting job endpoint.

        Args:
            job_id (str): Numeric scheduler job ID on this REST endpoint.
            accounting (bool): If true, query slurmdb instead of the controller.

        Returns:
            str: Relative REST endpoint path.

        Raises:
            ValueError: The scheduler job ID is not numeric.
        """
        if not str(job_id).isdigit():
            raise ValueError("Slurm job ID must be numeric")
        service = "slurmdb" if accounting else "slurm"
        return f"{service}/{self.api_version}/job/{job_id}"
