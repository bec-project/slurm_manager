import json
import os
import sys
from pathlib import Path


def main() -> None:
    """Simulate immutable generic and RPM registries without any network access."""
    args = sys.argv[1:]
    calls = Path(os.environ["MOCK_CURL_CALLS"])
    with calls.open("a") as output:
        output.write(json.dumps(args) + "\n")
    if os.environ.get("MOCK_CURL_EXIT"):
        sys.exit(int(os.environ["MOCK_CURL_EXIT"]))
    state_path = Path(os.environ["MOCK_CURL_STATE"])
    state = json.loads(state_path.read_text()) if state_path.exists() else {"files": {}}
    url = args[-1]
    body = b""
    if "--upload-file" in args:
        source = Path(args[args.index("--upload-file") + 1])
        target = url
        if url.endswith("/rpm/upload"):
            name = source.name.removesuffix(".rpm").removeprefix("bec-slurm-worker-")
            version, architecture = name.rsplit(".", 1)
            target = f"https://gitea.psi.ch/api/packages/bec/rpm/package/bec-slurm-worker/{version.replace('_', '~')}/{architecture}"
        fail_at = int(os.environ.get("MOCK_CURL_FAIL_AT", "0"))
        request_count = len(calls.read_text().splitlines())
        if not state.get("failed") and request_count == fail_at:
            state["failed"] = True
            status = 503
        elif target in state["files"]:
            status = 409
        else:
            state["files"][target] = source.read_bytes().hex()
            status = 201
    else:
        status = 200 if url in state["files"] else 404
        if status == 200:
            body = bytes.fromhex(state["files"][url])
    state_path.write_text(json.dumps(state))
    if "--output" in args:
        Path(args[args.index("--output") + 1]).write_bytes(body)
    if "--write-out" in args:
        print(status, end="")
    if status >= 400 and "--fail-with-body" in args:
        sys.exit(22)


if __name__ == "__main__":
    main()
