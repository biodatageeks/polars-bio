"""Exercise object-store defaults through the native reader, without cloud access."""

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import polars_bio as pb


@pytest.fixture
def remote_file(monkeypatch, request):
    # Three default-sized chunks make a sequential S3 implementation observable.
    sequence = b"ACGT" * (5 * 1024 * 1024)
    payload = b">seq1\n" + sequence + b"\n"
    if request.param == "http":
        payload = (
            b"##fileformat=VCFv4.3\n"
            b"##contig=<ID=chr1,length=100>\n"
            b"#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
            b"chr1\t10\t.\tA\tT\t.\tPASS\t.\n"
        )
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_HEAD(self):
            requests.append(("HEAD", self.headers.get("Range")))
            if request.param == "http":
                # Model a GET-only URL: chunked reads must fall back after HEAD.
                self.send_response(403)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()

        def do_GET(self):
            byte_range = self.headers.get("Range")
            requests.append(("GET", byte_range))
            start, end = 0, len(payload) - 1
            if byte_range:
                first, last = byte_range.removeprefix("bytes=").split("-", 1)
                start = int(first)
                if last:
                    end = min(int(last), end)
            body = payload[start : end + 1]
            self.send_response(206 if byte_range else 200)
            self.send_header("Content-Length", str(len(body)))
            if byte_range:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(payload)}")
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    endpoint = f"http://127.0.0.1:{server.server_port}"
    monkeypatch.setenv("AWS_ENDPOINT_URL", endpoint)
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    thread.start()
    try:
        path = (
            "s3://polars-bio-test/test.fasta"
            if request.param == "s3"
            else f"{endpoint}/test.vcf"
        )
        yield path, requests, sequence.decode()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize("concurrent_fetches", [None, 1], ids=["default", "sequential"])
@pytest.mark.parametrize("remote_file", ["s3"], indirect=True)
def test_s3_whole_object_request_mode(remote_file, concurrent_fetches):
    path, requests, sequence = remote_file
    options = {} if concurrent_fetches is None else {"concurrent_fetches": 1}

    frame = pb.read_fasta(path, max_retries=0, timeout=10, **options)
    assert frame["name"].to_list() == ["seq1"]
    assert frame["sequence"].to_list() == [sequence]
    # Compression detection has its own small ranged GET before streaming.
    get_ranges = [
        byte_range
        for method, byte_range in requests
        if method == "GET" and byte_range != "bytes=0-17"
    ]
    if concurrent_fetches is None:
        assert any(method == "HEAD" for method, _ in requests)
        assert "bytes=8388608-16777215" in get_ranges, requests
        assert any(
            byte_range and byte_range.startswith("bytes=16777216-")
            for byte_range in get_ranges
        ), requests
    else:
        assert all(method != "HEAD" for method, _ in requests), requests
        assert get_ranges
        assert all(byte_range in (None, "bytes=0-") for byte_range in get_ranges)


@pytest.mark.parametrize("concurrent_fetches", [None, 1], ids=["default", "sequential"])
@pytest.mark.parametrize("remote_file", ["http"], indirect=True)
def test_http_schema_inference_falls_back_after_refused_head(
    remote_file, concurrent_fetches
):
    path, requests, _sequence = remote_file
    options = {} if concurrent_fetches is None else {"concurrent_fetches": 1}

    # VCF schema inference supports HTTP; its full scan is a separate reader path.
    schema = pb.scan_vcf(path, max_retries=0, timeout=10, **options).collect_schema()

    assert "chrom" in schema and "alt" in schema
    assert any(method == "HEAD" for method, _ in requests), requests
    assert ("GET", None) in requests or ("GET", "bytes=0-") in requests
