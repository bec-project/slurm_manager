"""REST wire-format, authentication, and error handling tests."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from slurm_manager.slurm_rest_client import SlurmRestClient, SlurmRestError


@pytest.fixture
def endpoint(tmp_path, monkeypatch):
    for key in ("SLURM_REST_URL", "SLURM_REST_API_VERSION", "SLURM_JWT", "SLURM_USER"):
        monkeypatch.delenv(key, raising=False)
    calls = []
    replies = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.respond()

        def do_POST(self):
            self.respond()

        def do_DELETE(self):
            self.respond()

        def respond(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            calls.append(
                (self.command, self.path, dict(self.headers), json.loads(body) if body else None)
            )
            status, payload = replies.pop(0)
            self.send_response(status)
            if status == 302:
                self.send_header("Location", "/redirect-target")
            self.end_headers()
            self.wfile.write(
                payload if isinstance(payload, bytes) else json.dumps(payload).encode()
            )

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    env_file = tmp_path / ".env"
    env_file.write_text(
        f"SLURM_REST_URL=http://127.0.0.1:{server.server_port}\n"
        "SLURM_REST_API_VERSION=v0.0.44\nSLURM_JWT=test-secret\n"
    )
    try:
        yield env_file, calls, replies
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        assert not thread.is_alive()


def test_submit_release_cancel_and_accounting_wire_format(endpoint):
    env_file, calls, replies = endpoint
    replies.extend(
        [
            (200, {"job_id": 123, "errors": []}),
            (200, {"errors": []}),
            (200, {"jobs": [{"job_state": ["RUNNING"]}]}),
            (200, {"jobs": [{"state": {"current": ["COMPLETED"]}}]}),
            (200, {"errors": []}),
        ]
    )
    client = SlurmRestClient(env_file)
    job = {
        "current_working_directory": "/remote/work",
        "environment": ["PATH=/usr/bin"],
        "hold": True,
    }
    assert client.submit("#!/bin/bash\necho hello", job)["job_id"] == 123
    client.release("123")
    assert client.get_job("123")["job_state"] == ["RUNNING"]
    assert client.get_job("123", accounting=True)["state"]["current"] == ["COMPLETED"]
    client.cancel("123")
    assert [(method, path) for method, path, _, _ in calls] == [
        ("POST", "/slurm/v0.0.44/job/submit"),
        ("POST", "/slurm/v0.0.44/job/123"),
        ("GET", "/slurm/v0.0.44/job/123"),
        ("GET", "/slurmdb/v0.0.44/job/123"),
        ("DELETE", "/slurm/v0.0.44/job/123"),
    ]
    assert calls[0][3] == {"script": "#!/bin/bash\necho hello", "job": job}
    assert calls[1][3] == {"hold": False}
    assert calls[0][2]["X-Slurm-User-Token"] == "test-secret"
    assert "test-secret" not in json.dumps(calls[0][3])


def test_token_rotation_and_environment_override(endpoint, monkeypatch):
    env_file, calls, replies = endpoint
    replies.extend([(200, {}), (200, {})])
    client = SlurmRestClient(env_file)
    env_file.write_text(env_file.read_text().replace("test-secret", "replacement-token"))
    client.cancel("1")
    monkeypatch.setenv("SLURM_JWT", "environment-token")
    monkeypatch.setenv("SLURM_USER", "alice")
    client.cancel("2")
    assert calls[0][2]["X-Slurm-User-Token"] == "replacement-token"
    assert calls[1][2]["X-Slurm-User-Token"] == "environment-token"
    assert calls[1][2]["X-Slurm-User-Name"] == "alice"


@pytest.mark.parametrize(
    "status,payload",
    [
        (401, b"test-secret"),
        (500, b"test-secret"),
        (200, {"errors": [{"description": "test-secret", "error_number": 1}]}),
        (200, b"bad JSON test-secret"),
        (200, []),
        (302, b"redirect"),
    ],
)
def test_errors_are_raised_without_exposing_credentials(endpoint, status, payload):
    env_file, calls, replies = endpoint
    replies.append((status, payload))
    client = SlurmRestClient(env_file)
    with pytest.raises(SlurmRestError) as error:
        client.cancel("123")
    assert "test-secret" not in str(error.value)
    assert len(calls) == 1


@pytest.mark.parametrize(
    "reply",
    [
        (404, {}),
        (500, {"errors": [{"error_number": 2017}]}),
        (200, {"jobs": []}),
        (200, {"errors": [{"error_number": 2017}]}),
    ],
)
def test_missing_job_is_not_a_terminal_failure(endpoint, reply):
    env_file, _, replies = endpoint
    replies.append(reply)
    assert SlurmRestClient(env_file).get_job("123") is None


def test_missing_token_fails_before_any_request(endpoint):
    env_file, calls, _ = endpoint
    env_file.write_text(env_file.read_text().replace("test-secret", ""))
    with pytest.raises(ValueError, match="SLURM_JWT"):
        SlurmRestClient(env_file)
    assert calls == []


def test_submission_requires_job_id(endpoint):
    env_file, _, replies = endpoint
    replies.append((200, {"errors": []}))
    with pytest.raises(SlurmRestError, match="job ID"):
        SlurmRestClient(env_file).submit("#!/bin/bash", {})


def test_manager_submission_and_completion_over_http(endpoint):
    from functools import partial
    from unittest import mock

    import fakeredis

    from slurm_manager.job_tracker import TopicInfo
    from slurm_manager.redis_sub_client import RedisSubClient
    from slurm_manager.slurm_manager import SlurmManager

    env_file, calls, replies = endpoint
    replies.extend([(200, {"job_id": 123}), (200, {})])
    redis = RedisSubClient(redis_cls=partial(fakeredis.FakeRedis, decode_responses=True))
    with mock.patch("slurm_manager.slurm_manager.RedisSubClient", return_value=redis):
        manager = SlurmManager(env_file=env_file)
    try:
        future = manager.submit_job("examples/print_for_10s.sh", working_directory="/remote/work")
        assert calls[0][3]["job"]["hold"] is True
        assert calls[1][3] == {"hold": False}
        topics = TopicInfo(future._job_id)
        assert redis._subscription_ready[topics.event].is_set()
        redis._redis.publish(topics.event, '{"msg_type":"status","status":"finished"}')
        future.wait(timeout=1)
        assert future.success()
    finally:
        manager.shutdown(timeout=3)
    assert not manager._heartbeat_monitoring_thread.is_alive()
    assert not redis._listener_thread.is_alive()


@pytest.mark.parametrize("error_response", [False, True])
def test_truncated_responses_raise_a_transport_error(endpoint, error_response):
    from http.client import IncompleteRead
    from unittest import mock
    from urllib.error import HTTPError

    env_file, _, _ = endpoint
    client = SlurmRestClient(env_file)
    response = mock.MagicMock()
    response.read.side_effect = IncompleteRead(b"partial", 100)
    if error_response:
        error = HTTPError("http://local/job", 500, "Server error", {}, response)
        patch = mock.patch.object(client._opener, "open", side_effect=error)
    else:
        opened = mock.MagicMock()
        opened.__enter__.return_value = response
        patch = mock.patch.object(client._opener, "open", return_value=opened)
    with patch, pytest.raises(SlurmRestError):
        client.get_job("123")
