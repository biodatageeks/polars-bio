"""Exercise the OpenDAL adapters against a local read-only object endpoint."""

import base64
import gzip
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

import pytest

import polars_bio as pb

SAMPLE = Path(__file__).parent / "data/genbank/example.gbk"


@pytest.mark.parametrize("backend", ["s3", "gcs", "azure", "http", "stream_only"])
@pytest.mark.parametrize("compressed", [False, True])
def test_remote_genbank(monkeypatch, backend, compressed):
    data = SAMPLE.read_bytes()
    if compressed:
        data = gzip.compress(data)
    requests = []

    class ObjectHandler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            self.send_object(head=True)

        def do_GET(self):
            self.send_object(head=False)

        def log_message(self, *args):
            pass

        def send_object(self, head):
            requests.append(self.path)
            if "missing.gb" in self.path:
                self.send_error(404)
                return
            if backend == "stream_only" and (head or self.headers.get("Range")):
                self.send_error(403)
                return
            if "/storage/v1/" in self.path and "alt=media" not in self.path:
                content = json.dumps(
                    {
                        "size": str(len(data)),
                        "etag": "abc",
                        "name": "example.gb",
                        "updated": "2026-01-01T00:00:00Z",
                        "generation": "1",
                        "md5Hash": "",
                        "contentType": "application/octet-stream",
                    }
                ).encode()
                self.send_response(200)
            else:
                content = data
                match = re.fullmatch(
                    r"bytes=(\d+)-(\d*)",
                    self.headers.get("Range", self.headers.get("x-ms-range", "")),
                )
                if match:
                    start = int(match[1])
                    end = min(len(data), int(match[2]) + 1) if match[2] else len(data)
                    self.send_response(206)
                    self.send_header(
                        "Content-Range", f"bytes {start}-{end - 1}/{len(data)}"
                    )
                    content = data[start:end]
                else:
                    self.send_response(200)
            self.send_header("Content-Length", str(len(content)))
            self.send_header("ETag", '"abc"')
            self.send_header("Last-Modified", "Thu, 01 Jan 2026 00:00:00 GMT")
            self.end_headers()
            if not head:
                self.wfile.write(content)

    with ThreadingHTTPServer(("127.0.0.1", 0), ObjectHandler) as server:
        worker = Thread(target=server.serve_forever, daemon=True)
        worker.start()
        endpoint = f"http://127.0.0.1:{server.server_port}"
        name = "example.gb.gz" if compressed else "example.gb"
        if backend == "s3":
            monkeypatch.setenv("AWS_ENDPOINT_URL", endpoint)
            monkeypatch.setenv("AWS_REGION", "us-east-1")
            path = f"s3://genbank/{name}"
        elif backend == "gcs":
            monkeypatch.setenv("STORAGE_EMULATOR_HOST", endpoint)
            monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
            path = f"gs://genbank/{name}"
        elif backend == "azure":
            monkeypatch.setenv("AZURE_ENDPOINT_URL", endpoint)
            monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "devstoreaccount1")
            monkeypatch.setenv(
                "AZURE_STORAGE_KEY", base64.b64encode(b"test-account").decode()
            )
            path = f"{endpoint}/devstoreaccount1/genbank/{name}"
        else:
            path = f"{endpoint}/genbank/{name}"
        partition_key = "datafusion.execution.target_partitions"
        previous = pb.get_option(partition_key)
        pb.set_option(partition_key, "3")
        try:
            remote = pb.read_genbank(path, "features", max_retries=0, timeout=5)
            assert remote.equals(pb.read_genbank(SAMPLE, "features"))
            assert requests
            if backend == "http" and not compressed:
                with pytest.raises(Exception, match="missing.gb"):
                    pb.read_genbank(
                        f"{endpoint}/genbank/missing.gb", max_retries=0, timeout=5
                    )
        finally:
            pb.set_option(partition_key, previous)
            server.shutdown()
            worker.join()
