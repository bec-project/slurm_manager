"""Exercise verified TLS pub/sub against a local Redis server."""

import shutil
import socket
import subprocess
import threading
import time

import pytest
from redis.exceptions import ConnectionError

from slurm_manager.redis_sub_client import RedisSubClient


@pytest.fixture
def tls_redis(tmp_path):
    redis_server = shutil.which("redis-server")
    openssl = shutil.which("openssl")
    if not redis_server or not openssl:
        pytest.skip("redis-server and openssl are needed for TLS integration tests")
    cert = tmp_path / "server.pem"
    key = tmp_path / "server.key"
    subprocess.run(
        [
            openssl,
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
            "-keyout",
            str(key),
            "-out",
            str(cert),
        ],
        check=True,
        capture_output=True,
    )
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    log = tmp_path / "redis.log"
    with log.open("w") as output:
        process = subprocess.Popen(
            [
                redis_server,
                "--bind",
                "127.0.0.1",
                "--port",
                "0",
                "--tls-port",
                str(port),
                "--tls-cert-file",
                str(cert),
                "--tls-key-file",
                str(key),
                "--tls-auth-clients",
                "no",
                "--save",
                "",
                "--appendonly",
                "no",
            ],
            stdout=output,
            stderr=subprocess.STDOUT,
            cwd=tmp_path,
        )
        try:
            deadline = time.monotonic() + 5
            while "Ready to accept connections" not in log.read_text():
                if process.poll() is not None or time.monotonic() > deadline:
                    pytest.fail(log.read_text())
                time.sleep(0.02)
            yield port, cert
        finally:
            process.terminate()
            process.wait(timeout=5)


def test_tls_pubsub(tls_redis, monkeypatch):
    port, cert = tls_redis
    monkeypatch.setenv("SSL_CERT_FILE", str(cert))
    client = RedisSubClient(
        bootstrap=f"localhost:{port}", ssl=True, ssl_cert_reqs="required", ssl_check_hostname=True
    )
    received = threading.Event()
    try:
        client.subscribe("info/test/status", lambda message: received.set())
        assert client._subscription_ready["info/test/status"].wait(3)
        client._redis.publish("info/test/status", '{"msg_type":"status","status":"finished"}')
        assert received.wait(3)
    finally:
        client.shutdown()


@pytest.mark.parametrize("wrong_hostname", [False, True])
def test_tls_rejects_untrusted_or_wrong_hostname(tls_redis, wrong_hostname, monkeypatch):
    port, cert = tls_redis
    if wrong_hostname:
        monkeypatch.setenv("SSL_CERT_FILE", str(cert))
    client = RedisSubClient(
        bootstrap=f"{'127.0.0.1' if wrong_hostname else 'localhost'}:{port}",
        ssl=True,
        ssl_cert_reqs="required",
        ssl_check_hostname=True,
        socket_connect_timeout=1,
    )
    try:
        with pytest.raises(ConnectionError, match="CERTIFICATE_VERIFY_FAILED"):
            client._redis.ping()
    finally:
        client.shutdown()
