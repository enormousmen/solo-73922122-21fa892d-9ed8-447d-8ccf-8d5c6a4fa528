"""End-to-end tests: real origin + proxy processes, real HTTP.

Run from the repo root:  python3 -m pytest tests/ -v
Requires: fastapi, uvicorn, aiohttp (app deps) + httpx, pytest (test deps).
"""

from __future__ import annotations

import hashlib
import os
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
MIB = 1024 * 1024
PKG_BIG = "pkg-32m.bin"
PKG_SMALL = "pkg-1m.bin"


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_health(url: str, proc: subprocess.Popen, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early with code {proc.returncode}")
        try:
            r = httpx.get(url, timeout=2.0)
            if r.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"server at {url} did not become healthy")


def start_server(appdir: str, port: int, env_extra: dict, log: Path,
                 control: Path) -> subprocess.Popen:
    env = dict(os.environ)
    env.update(env_extra)
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "tests" / "run_server.py"),
         str(ROOT / appdir), str(port), str(control)],
        env=env,
        stdout=log.open("ab"),
        stderr=subprocess.STDOUT,
    )
    wait_health(f"http://127.0.0.1:{port}/healthz", proc)
    return proc


def stop(proc: subprocess.Popen, control: Path) -> None:
    """Ask the server to exit via its control file (signals are blocked here)."""
    if proc.poll() is None:
        control.touch(exist_ok=True)
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except PermissionError:
                pass
            try:
                proc.wait(5)
            except subprocess.TimeoutExpired:
                pass


def client(timeout_read: float = 120.0) -> httpx.Client:
    return httpx.Client(timeout=httpx.Timeout(5.0, read=timeout_read, write=30.0, pool=5.0))


def download_bytes(base: str, path: str) -> bytes:
    with client() as c:
        r = c.get(f"{base}/packages/{path}")
        r.raise_for_status()
        return r.content


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def origin_stats(origin_url: str) -> dict:
    with client() as c:
        return c.get(f"{origin_url}/control/stats").json()


def origin_reset(origin_url: str) -> None:
    with client() as c:
        c.post(f"{origin_url}/control/reset")


def set_behavior(origin_url: str, delay_ms: int = 0, truncate: int | None = None,
                 chunk_size: int = 256 * 1024) -> None:
    with client() as c:
        r = c.post(
            f"{origin_url}/control/behavior",
            json={
                "chunk_delay_ms": delay_ms,
                "truncate_after_bytes": truncate,
                "chunk_size": chunk_size,
            },
        )
        r.raise_for_status()


@pytest.fixture(scope="session")
def origin(tmp_path_factory):
    pkg_dir = tmp_path_factory.mktemp("pkgs")
    log_dir = tmp_path_factory.mktemp("logs")
    log = log_dir / "origin.log"
    control = log_dir / "origin.stop"
    port = free_port()
    proc = start_server("origin", port, {"PKG_DIR": str(pkg_dir)}, log, control)
    yield SimpleNamespace(url=f"http://127.0.0.1:{port}", port=port, dir=pkg_dir, log=log)
    stop(proc, control)


class ProxyRunner:
    def __init__(self, origin_port: int, cache_dir: Path, log: Path, control: Path):
        self.origin_port = origin_port
        self.cache_dir = cache_dir
        self.log = log
        self.control = control
        self.proc: subprocess.Popen | None = None
        self.url: str = ""

    def start(self) -> None:
        port = free_port()
        if self.control.exists():
            self.control.unlink()
        self.proc = start_server(
            "proxy",
            port,
            {
                "ORIGIN_BASE_URL": f"http://127.0.0.1:{self.origin_port}",
                "CACHE_DIR": str(self.cache_dir),
            },
            self.log,
            self.control,
        )
        self.url = f"http://127.0.0.1:{port}"

    def restart(self) -> None:
        stop(self.proc, self.control)
        self.start()

    def stop(self) -> None:
        if self.proc:
            stop(self.proc, self.control)

    def rss_bytes(self) -> int:
        with open(f"/proc/{self.proc.pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS"):
                    return int(line.split()[1]) * 1024
        raise RuntimeError("VmRSS not found")


@pytest.fixture()
def proxy(origin, tmp_path):
    runner = ProxyRunner(origin.port, tmp_path / "cache", tmp_path / "proxy.log",
                         tmp_path / "proxy.stop")
    runner.start()
    yield runner
    runner.stop()


# --------------------------------------------------------------------- #
# tests                                                                  #
# --------------------------------------------------------------------- #

def test_basic_download_then_cache_hit(proxy, origin):
    origin_reset(origin.url)
    body1 = download_bytes(proxy.url, PKG_SMALL)
    stats = origin_stats(origin.url)
    assert stats["paths"][PKG_SMALL] == 1
    assert sha256(body1) == stats["packages"][PKG_SMALL]["sha256"]

    with client() as c:
        r2 = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
    assert r2.headers["x-cache"] == "HIT"
    assert r2.content == body1
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1  # no second fetch


def test_concurrent_miss_single_fill(proxy, origin):
    set_behavior(origin.url, delay_ms=80, chunk_size=64 * 1024)  # ~1.2s fill window
    try:
        origin_reset(origin.url)
        with ThreadPoolExecutor(8) as ex:
            bodies = list(ex.map(lambda _: download_bytes(proxy.url, PKG_SMALL), range(8)))
        assert origin_stats(origin.url)["total_hits"] == 1
        assert all(b == bodies[0] for b in bodies)
        assert len(bodies[0]) == MIB
    finally:
        set_behavior(origin.url)


def test_first_chunk_arrives_before_full_package(proxy, origin):
    # 32MiB at 100ms per 256KiB chunk -> full fill takes ~13s.
    set_behavior(origin.url, delay_ms=100, chunk_size=256 * 1024)
    try:
        t0 = time.monotonic()
        with client(timeout_read=60).stream("GET", f"{proxy.url}/packages/{PKG_BIG}") as r:
            first = next(r.iter_bytes(256 * 1024))
            ttfb = time.monotonic() - t0
            assert len(first) > 0
        assert ttfb < 3.0, f"first chunk took {ttfb:.2f}s"
    finally:
        set_behavior(origin.url)
        with client() as c:
            c.post(f"{proxy.url}/purge/{PKG_BIG}")


def test_slow_client_blocks_nobody(proxy, origin):
    # Fill pace ~20ms/256KiB -> ~2.6s for 32MiB; slow client crawls far slower.
    set_behavior(origin.url, delay_ms=20, chunk_size=256 * 1024)
    try:
        origin_reset(origin.url)
        result: dict = {}

        def slow_reader():
            with client(timeout_read=300).stream(
                "GET", f"{proxy.url}/packages/{PKG_BIG}"
            ) as r:
                got = 0
                for chunk in r.iter_bytes(64 * 1024):
                    got += len(chunk)
                    time.sleep(0.2)
                    if got >= 5 * 64 * 1024:
                        break
                result["slow_got"] = got

        t = threading.Thread(target=slow_reader)
        t.start()
        time.sleep(0.3)  # slow client attaches first
        t0 = time.monotonic()
        fast = download_bytes(proxy.url, PKG_BIG)
        fast_dur = time.monotonic() - t0
        t.join(60)

        assert len(fast) == 32 * MIB
        assert fast_dur < 15, f"fast client dragged to {fast_dur:.1f}s by slow client"
        assert origin_stats(origin.url)["total_hits"] == 1  # shared single fill

        # Fill completed in the background despite the slow client leaving.
        with client() as c:
            r = c.get(f"{proxy.url}/packages/{PKG_BIG}")
        assert r.headers["x-cache"] == "HIT"
        assert sha256(r.content) == origin_stats(origin.url)["packages"][PKG_BIG]["sha256"]
    finally:
        set_behavior(origin.url)


def test_purge_refills_and_serves_new_content(proxy, origin):
    origin_reset(origin.url)
    body1 = download_bytes(proxy.url, PKG_SMALL)
    sha1 = sha256(body1)

    with client() as c:
        new = c.post(f"{origin.url}/control/content/{PKG_SMALL}").json()
    assert new["sha256"] != sha1

    # Still cached: old content, no new origin fetch.
    assert sha256(download_bytes(proxy.url, PKG_SMALL)) == sha1
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1

    with client() as c:
        pr = c.post(f"{proxy.url}/purge/{PKG_SMALL}")
    assert pr.status_code == 200 and pr.json()["was_cached"] is True

    # First download after purge re-fills exactly once; followers share it.
    with ThreadPoolExecutor(6) as ex:
        bodies = list(ex.map(lambda _: download_bytes(proxy.url, PKG_SMALL), range(6)))
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 2
    assert all(sha256(b) == new["sha256"] for b in bodies)


def test_purge_during_fill_keeps_old_download_alive(proxy, origin):
    set_behavior(origin.url, delay_ms=60, chunk_size=64 * 1024)  # ~1s fill
    try:
        origin_reset(origin.url)
        result: dict = {}

        def reader_a():
            with client(timeout_read=60).stream(
                "GET", f"{proxy.url}/packages/{PKG_SMALL}"
            ) as r:
                chunks = []
                for chunk in r.iter_bytes(64 * 1024):
                    chunks.append(chunk)
                    time.sleep(0.03)
                result["a"] = b"".join(chunks)

        ta = threading.Thread(target=reader_a)
        ta.start()
        time.sleep(0.3)  # A is mid-fill on generation 1

        with client() as c:
            pr = c.post(f"{proxy.url}/purge/{PKG_SMALL}")
        assert pr.json()["was_cached"] is True

        body_b = download_bytes(proxy.url, PKG_SMALL)  # triggers generation 2
        ta.join(30)

        assert origin_stats(origin.url)["paths"][PKG_SMALL] == 2  # old fill + new fill
        assert len(result["a"]) == MIB  # old download completed in full
        assert sha256(result["a"]) == sha256(body_b)
    finally:
        set_behavior(origin.url)


def test_upstream_disconnect_fails_all_and_caches_nothing(proxy, origin):
    set_behavior(origin.url, delay_ms=50, chunk_size=64 * 1024, truncate=200_000)
    try:
        origin_reset(origin.url)

        def expect_failure():
            with pytest.raises(httpx.HTTPError):
                download_bytes(proxy.url, PKG_SMALL)

        with ThreadPoolExecutor(4) as ex:
            list(ex.map(lambda _: expect_failure(), range(4)))

        assert origin_stats(origin.url)["total_hits"] == 1  # single shared fill
        with client() as c:
            status = c.get(f"{proxy.url}/cache/status").json()
        assert PKG_SMALL not in status
        leftovers = list(proxy.cache_dir.glob(f"{PKG_SMALL}*"))
        assert leftovers == [], f"half package left behind: {leftovers}"
    finally:
        set_behavior(origin.url)

    # Origin healthy again -> retry succeeds and caches.
    body = download_bytes(proxy.url, PKG_SMALL)
    assert len(body) == MIB
    assert origin_stats(origin.url)["total_hits"] == 2


def test_restart_keeps_complete_cache_and_cleans_partials(proxy, origin):
    origin_reset(origin.url)
    body1 = download_bytes(proxy.url, PKG_SMALL)
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1

    stray = proxy.cache_dir / f"{PKG_SMALL}.part-deadbeef"
    stray.write_bytes(b"x" * 100)
    orphan_meta = proxy.cache_dir / "ghost.meta"
    orphan_meta.write_text("{}")

    proxy.restart()

    with client() as c:
        r = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
    assert r.headers["x-cache"] == "HIT"
    assert r.content == body1
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1  # served from disk cache
    assert not stray.exists()
    assert not orphan_meta.exists()


def test_memory_does_not_grow_with_package_size(proxy, origin):
    rss0 = proxy.rss_bytes()
    body = download_bytes(proxy.url, PKG_BIG)
    assert len(body) == 32 * MIB
    time.sleep(0.5)
    rss1 = proxy.rss_bytes()
    delta = rss1 - rss0
    print(f"\nproxy RSS delta while streaming 32MiB: {delta / MIB:.1f} MiB")
    # Buffering the whole package would cost >=32MiB; streaming stays far below.
    assert delta < 24 * MIB


def test_missing_package_returns_404_and_no_negative_cache(proxy, origin):
    origin_reset(origin.url)
    with client() as c:
        r1 = c.get(f"{proxy.url}/packages/nope.bin")
        r2 = c.get(f"{proxy.url}/packages/nope.bin")
    assert r1.status_code == 404 and r2.status_code == 404
    assert origin_stats(origin.url)["paths"]["nope.bin"] == 2


def test_path_traversal_rejected(proxy):
    with client() as c:
        r = c.get(f"{proxy.url}/packages/..%2f..%2fetc%2fpasswd")
    assert r.status_code in (400, 404)


# --------------------------------------------------------------------- #
# origin metadata recovery: custom packages on the volume, backfill,     #
# generation-safe swaps (standalone origin with a pre-populated volume)  #
# --------------------------------------------------------------------- #


class OriginRunner:
    def __init__(self, pkg_dir: Path, log: Path, control: Path, env_extra=None):
        self.pkg_dir = pkg_dir
        self.log = log
        self.control = control
        self.env_extra = env_extra or {}
        self.proc: subprocess.Popen | None = None
        self.url = ""

    def start(self) -> None:
        port = free_port()
        if self.control.exists():
            self.control.unlink()
        self.proc = start_server(
            "origin", port, {"PKG_DIR": str(self.pkg_dir), **self.env_extra},
            self.log, self.control,
        )
        self.url = f"http://127.0.0.1:{port}"

    def restart(self) -> None:
        stop(self.proc, self.control)
        self.start()

    def stop(self) -> None:
        stop(self.proc, self.control)


@pytest.fixture()
def origin_standalone(tmp_path):
    pkg_dir = tmp_path / "pkgs"
    pkg_dir.mkdir(parents=True)
    runner = OriginRunner(
        pkg_dir, tmp_path / "origin.log", tmp_path / "origin.stop"
    )
    yield runner
    runner.stop()


def _wait_digest(stats_url: str, key: str, timeout: float = 30.0) -> dict:
    deadline = time.monotonic() + timeout
    last = {}
    while time.monotonic() < deadline:
        with client() as c:
            last = c.get(stats_url).json()
        info = last["packages"].get(key)
        if info and info["sha256"]:
            return info
        time.sleep(0.05)
    raise AssertionError(f"digest for {key} never backfilled: {last}")


def test_origin_recovers_custom_packages_after_restart(origin_standalone, tmp_path):
    # Simulate packages left on the volume by a previous build team, including
    # one in a subdirectory; origin never saw them before restart.
    pkg_dir = origin_standalone.pkg_dir
    sub = pkg_dir / "team-x" / "libs"
    sub.mkdir(parents=True)
    custom = b"custom-build-artifact-" + os.urandom(200_000)
    nested = b"nested-package-" + os.urandom(50_000)
    (pkg_dir / "custom-1.bin").write_bytes(custom)
    (sub / "widget-7.tar.gz").write_bytes(nested)
    want_custom = sha256(custom)
    want_nested = sha256(nested)

    origin_standalone.start()

    # Downloads keep working and carry the digest of the served bytes.
    got = download_bytes(origin_standalone.url, "custom-1.bin")
    assert got == custom
    with client() as c:
        r = c.get(f"{origin_standalone.url}/packages/custom-1.bin")
    assert r.headers["x-package-sha256"] == want_custom

    got_nested = download_bytes(origin_standalone.url, "team-x/libs/widget-7.tar.gz")
    assert got_nested == nested
    with client() as c:
        r = c.get(f"{origin_standalone.url}/packages/team-x/libs/widget-7.tar.gz")
    assert r.headers["x-package-sha256"] == want_nested

    # Stats list every volume package, nested paths included, with current
    # size and digest.
    info_custom = _wait_digest(f"{origin_standalone.url}/control/stats", "custom-1.bin")
    info_nested = _wait_digest(
        f"{origin_standalone.url}/control/stats", "team-x/libs/widget-7.tar.gz"
    )
    assert info_custom == {"size": len(custom), "sha256": want_custom}
    assert info_nested == {"size": len(nested), "sha256": want_nested}

    # Restart: same volume, metadata must be recovered again.
    origin_standalone.restart()
    with client() as c:
        r = c.get(f"{origin_standalone.url}/packages/custom-1.bin")
    assert r.status_code == 200
    assert r.headers["x-package-sha256"] == want_custom
    stats = origin_stats(origin_standalone.url)
    assert stats["packages"]["team-x/libs/widget-7.tar.gz"] == {
        "size": len(nested),
        "sha256": want_nested,
    }


def test_origin_startup_does_not_wait_for_backfill(origin_standalone, tmp_path):
    # Several large files on the volume: hashing all of them takes noticeably
    # longer than a healthy boot, yet the origin must serve /healthz promptly.
    pkg_dir = origin_standalone.pkg_dir
    big = 96 * MIB
    for i in range(6):
        (pkg_dir / f"big-{i}.bin").write_bytes(b"\0" * big)

    t0 = time.monotonic()
    origin_standalone.start()
    boot = time.monotonic() - t0
    assert boot < 5.0, f"startup blocked on digest backfill for {boot:.1f}s"

    # Immediately after boot the backlog may still be pending (sha256 null) or
    # already done on fast machines; either way every digest eventually
    # converges to the real file hash.
    stats = origin_stats(origin_standalone.url)
    pending = [
        k for k, v in stats["packages"].items()
        if k.startswith("big-") and not v["sha256"]
    ]
    for i in range(6):
        info = _wait_digest(
            f"{origin_standalone.url}/control/stats", f"big-{i}.bin", timeout=60
        )
        assert info["size"] == big
        assert info["sha256"] == sha256(b"\0" * big)
    if not pending:
        print("\nbackfill already finished before first stats poll (fast host)")


def test_repeated_downloads_reuse_backfilled_digest(origin_standalone):
    pkg_dir = origin_standalone.pkg_dir
    blob = os.urandom(300_000)
    (pkg_dir / "reuse.bin").write_bytes(blob)
    want = sha256(blob)
    origin_standalone.start()

    # First download triggers/awaits the backfill...
    with client() as c:
        r1 = c.get(f"{origin_standalone.url}/packages/reuse.bin")
        assert r1.headers["x-package-sha256"] == want
        # ...and every later download reuses the cached digest for the version.
        r2 = c.get(f"{origin_standalone.url}/packages/reuse.bin")
        r3 = c.get(f"{origin_standalone.url}/packages/reuse.bin")
    assert r2.headers["x-package-sha256"] == want
    assert r3.headers["x-package-sha256"] == want
    assert r1.content == r2.content == blob

    info = _wait_digest(f"{origin_standalone.url}/control/stats", "reuse.bin")
    assert info == {"size": len(blob), "sha256": want}


def test_swap_during_backfill_wins_and_late_digest_is_discarded(
    origin_standalone, tmp_path
):
    # One slow-to-hash file keeps the backfill busy while a new version is
    # published at the same path.
    pkg_dir = origin_standalone.pkg_dir
    big = b"\xa5" * (128 * MIB)
    (pkg_dir / "rolling.bin").write_bytes(big)
    origin_standalone.env_extra["DIGEST_WORKERS"] = "1"
    origin_standalone.start()

    # Wait until the backfill is actually in flight (stats shows the package
    # with a null digest), then publish a new, small version.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        stats = origin_stats(origin_standalone.url)
        if "rolling.bin" in stats["packages"]:
            break
        time.sleep(0.05)

    with client() as c:
        swapped = c.post(
            f"{origin_standalone.url}/control/content/rolling.bin",
            params={"size": 123_456},
        ).json()
    new_sha = swapped["sha256"]
    assert new_sha != sha256(big)

    # New downloads and stats immediately reflect the new version; wait long
    # enough for the stale backfill of the 128MiB file to finish afterwards.
    time.sleep(2.0)
    with client() as c:
        r = c.get(f"{origin_standalone.url}/packages/rolling.bin")
    assert r.status_code == 200
    assert r.headers["x-package-sha256"] == new_sha
    assert sha256(r.content) == new_sha
    assert len(r.content) == 123_456

    stats = origin_stats(origin_standalone.url)
    assert stats["packages"]["rolling.bin"] == {
        "size": 123_456,
        "sha256": new_sha,
    }

    # The on-disk file is the new version; no stale content leaked through.
    assert sha256((pkg_dir / "rolling.bin").read_bytes()) == new_sha


def test_swap_then_download_and_stats_serve_new_version(origin_standalone):
    pkg_dir = origin_standalone.pkg_dir
    first = os.urandom(256 * 1024)
    (pkg_dir / "swap.bin").write_bytes(first)
    origin_standalone.start()
    old_sha = sha256(first)

    # Let the original version finish backfilling.
    info = _wait_digest(f"{origin_standalone.url}/control/stats", "swap.bin")
    assert info["sha256"] == old_sha

    with client() as c:
        resp = c.post(f"{origin_standalone.url}/control/content/swap.bin").json()
    new_sha = resp["sha256"]
    assert new_sha != old_sha

    with client() as c:
        r = c.get(f"{origin_standalone.url}/packages/swap.bin")
    assert r.headers["x-package-sha256"] == new_sha
    assert sha256(r.content) == new_sha

    stats = origin_stats(origin_standalone.url)
    assert stats["packages"]["swap.bin"] == {
        "size": len(r.content),
        "sha256": new_sha,
    }


def test_stats_handles_out_of_band_file_replacement(origin_standalone):
    pkg_dir = origin_standalone.pkg_dir
    (pkg_dir / "oob.bin").write_bytes(b"v1" * 1000)
    origin_standalone.start()
    _wait_digest(f"{origin_standalone.url}/control/stats", "oob.bin")

    # Replace the file directly on the volume (no control API involved).
    new_blob = b"v2-content" * 5000
    (pkg_dir / "oob.bin").write_bytes(new_blob)
    origin_stats(origin_standalone.url)  # trigger out-of-band re-registration
    info = _wait_digest(f"{origin_standalone.url}/control/stats", "oob.bin")
    assert info == {"size": len(new_blob), "sha256": sha256(new_blob)}
    with client() as c:
        r = c.get(f"{origin_standalone.url}/packages/oob.bin")
    assert r.headers["x-package-sha256"] == sha256(new_blob)
    assert r.content == new_blob
