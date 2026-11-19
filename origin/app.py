"""Controllable local package origin used to demo/test the caching proxy.

Serves complete packages from PKG_DIR (a 32 MiB package is generated on
first boot). Control endpoints let you change content, add per-chunk delay,
cut the stream mid-body, and inspect how many times the proxy fetched.

Metadata recovery
-----------------
The package volume outlives this process, so on boot every regular file under
PKG_DIR (including files in subdirectories) is registered from disk. sha256
backfill runs in background tasks: startup never blocks on hashing, while
each package keeps a monotonic generation counter. A backfill task only
publishes a digest if the file identity it hashed is still the current
generation, so a late digest of an old version can never overwrite the
metadata of a newer package published at the same path. Downloads needing a
not-yet-computed digest single-flight on the package's backfill task and
reuse the cached digest afterwards.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel, Field

logger = logging.getLogger("pkg-origin")

PKG_DIR = Path(os.environ.get("PKG_DIR", "./packages")).resolve()
BACKFILL_CONCURRENCY = int(os.environ.get("DIGEST_WORKERS", "4"))
DEFAULT_PACKAGES = {
    "pkg-32m.bin": 32 * 1024 * 1024,
    "pkg-1m.bin": 1 * 1024 * 1024,
}

_SEGMENT_RE = re.compile(r"\A[A-Za-z0-9._~!$&'()*+,;=:@-]+\Z")

behavior = {
    "chunk_delay_ms": 0,
    "truncate_after_bytes": None,  # int | None
    "chunk_size": 256 * 1024,
}
hits = {"total": 0, "paths": {}}


class UpstreamAbort(Exception):
    """Raised mid-stream to simulate an upstream connection abort."""


class PackageRecord:
    """Metadata for one on-disk generation of a package key.

    ``gen`` is a per-key monotonic generation counter. Every content change
    bumps it; backfill results from older generations are discarded.
    """

    def __init__(self, key: str, file_id: tuple, size: int, gen: int) -> None:
        self.key = key
        self.file_id = file_id
        self.size = size
        self.gen = gen
        self.sha256: str | None = None
        self.task: asyncio.Task | None = None
        self.lock = asyncio.Lock()
        self.ready = asyncio.Event()
        self.error: str | None = None

    def snapshot(self) -> dict:
        return {"size": self.size, "sha256": self.sha256}


# key -> current generation record
records: dict[str, PackageRecord] = {}
registry_lock = asyncio.Lock()
backfill_semaphore: asyncio.Semaphore | None = None
background_tasks: set[asyncio.Task] = set()


def validate_key(raw: str) -> str:
    if not raw or raw.endswith("/"):
        raise ValueError("empty package path")
    parts = raw.split("/")
    for seg in parts:
        if seg in ("", ".", ".."):
            raise ValueError(f"illegal path segment: {seg!r}")
        if not _SEGMENT_RE.match(seg):
            raise ValueError(f"illegal characters in segment: {seg!r}")
        return "/".join(parts)


def _file_id(p: Path) -> tuple:
    """Stable identity of a file's content; changes on every replacement."""
    st = p.stat()
    return (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)


def _hash_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _write_package(key: str, size: int) -> tuple[str, tuple]:
    """Write a fresh random package atomically; return (sha256, file_id)."""
    p = PKG_DIR / key
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + ".tmp")
    h = hashlib.sha256()
    with open(tmp, "wb") as f:
        left = size
        while left > 0:
            b = os.urandom(min(1024 * 1024, left))
            f.write(b)
            h.update(b)
            left -= len(b)
    os.replace(tmp, p)
    return h.hexdigest(), _file_id(p)


async def _backfill(rec: PackageRecord) -> None:
    """Hash one file off the event loop; publish only if still current."""
    assert backfill_semaphore is not None
    gen = rec.gen
    file_id = rec.file_id
    path = PKG_DIR / rec.key
    async with backfill_semaphore:
        try:
            digest = await asyncio.to_thread(_hash_file, path)
        except OSError as exc:
            logger.warning("digest backfill failed for %s: %s", rec.key, exc)
            async with registry_lock:
                cur = records.get(rec.key)
                if cur is rec:
                    rec.error = f"{type(exc).__name__}: {exc}"
                    rec.ready.set()
            return
        async with registry_lock:
            cur = records.get(rec.key)
            if cur is not rec:
                # The path was republished while hashing: this result belongs
                # to a stale generation and must not touch the new metadata.
                logger.info(
                    "discard stale digest for %s (old gen %d, current gen %d)",
                    rec.key, gen, cur.gen if cur is not None else None,
                )
                return
            if cur.file_id != file_id:
                # Same record object but the file was replaced underneath us.
                logger.info("discard stale digest for %s (file id changed)", rec.key)
                return
            cur.sha256 = digest
            cur.ready.set()


def _schedule_backfill(rec: PackageRecord) -> None:
    task = asyncio.create_task(_backfill(rec))
    rec.task = task
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)


def _register_existing_locked(key: str) -> PackageRecord | None:
    """Adopt an on-disk file as a new generation (caller holds registry lock)."""
    p = PKG_DIR / key
    try:
        st = p.stat()
    except OSError:
        return None
    if not p.is_file():
        return None
    gen = (records[key].gen + 1) if key in records else 1
    rec = PackageRecord(key, (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns),
                        st.st_size, gen)
    records[key] = rec
    _schedule_backfill(rec)
    return rec


async def ensure_record(key: str) -> PackageRecord | None:
    """Return the current record, adopting an out-of-band disk file if needed."""
    async with registry_lock:
        rec = records.get(key)
        if rec is not None:
            return rec
        return _register_existing_locked(key)


async def ensure_digest(key: str) -> PackageRecord | None:
    """Resolve a ready digest for key, single-flighting backfill per version."""
    for _attempt in range(3):
        rec = await ensure_record(key)
        if rec is None:
            return None
        if rec.ready.is_set() and rec.sha256 is not None:
            return rec
        async with rec.lock:
            if rec.ready.is_set() and rec.sha256 is not None:
                return rec
            if rec.task is None or rec.task.done():
                # Backfill never started or died (e.g. transient IO error).
                rec.ready.clear()
                rec.error = None
                _schedule_backfill(rec)
            await rec.ready.wait()
        cur = await ensure_record(key)
        if cur is rec and cur.sha256 is not None:
            return cur
        # Replaced while waiting: loop and resolve the new generation.
    return records.get(key)


def _recover_from_disk() -> list[str]:
    """Register every package already on the volume, including subdirectories."""
    found: list[str] = []
    for p in sorted(PKG_DIR.rglob("*")):
        if not p.is_file():
            continue
        name = p.name
        if name.endswith((".tmp", ".meta")):
            continue  # leftovers of an interrupted publish / foreign tooling
        key = p.relative_to(PKG_DIR).as_posix()
        found.append(key)
    return found


@asynccontextmanager
async def lifespan(app: FastAPI):
    global backfill_semaphore
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    PKG_DIR.mkdir(parents=True, exist_ok=True)
    backfill_semaphore = asyncio.Semaphore(BACKFILL_CONCURRENCY)

    # Default demo packages: create missing ones synchronously so the first
    # request never races package generation, with digest in hand.
    for name, size in DEFAULT_PACKAGES.items():
        p = PKG_DIR / name
        if not p.is_file():
            digest, file_id = await asyncio.to_thread(_write_package, name, size)
            async with registry_lock:
                rec = PackageRecord(name, file_id, size, 1)
                rec.sha256 = digest
                rec.ready.set()
                records[name] = rec

    # Recover everything else already on the volume. Hashing happens in
    # background tasks; startup completes without waiting for any digest.
    keys = await asyncio.to_thread(_recover_from_disk)
    async with registry_lock:
        for key in keys:
            if key in records:
                continue
            _register_existing_locked(key)

    try:
        yield
    finally:
        for task in tuple(background_tasks):
            task.cancel()


app = FastAPI(
    title="pkg-origin",
    version="2.0.0",
    lifespan=lifespan,
    description=(
        "本地包源站。演示流程（均在 Swagger UI 可直接执行）：\n\n"
        "1. `POST /control/content/{path}` —— 换包：同路径发布新版本，返回新摘要；\n"
        "2. `GET /packages/{path}` —— 下载，响应头 `X-Package-Sha256` 为所供版本摘要；\n"
        "3. `GET /control/stats` —— 查询统计，列出当前版大小与摘要。\n\n"
        "卷上已有包（含子目录）在启动后于后台补算摘要，无需等待即可开服。"
    ),
)


@app.get("/healthz", tags=["meta"])
async def healthz():
    return {"ok": True}


@app.get(
    "/packages/{path:path}",
    tags=["packages"],
    summary="下载包（响应头 X-Package-Sha256 为所供版本的摘要）",
)
async def get_package(path: str):
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)
    # Count every fetch attempt against the origin, successful or not.
    hits["total"] += 1
    hits["paths"][key] = hits["paths"].get(key, 0) + 1

    rec = await ensure_digest(key)
    if rec is None:
        return PlainTextResponse("package not found", status_code=404)

    p = PKG_DIR / key
    # Re-check against disk: a swap could land between digest resolution and
    # opening the file. Retry with the fresh generation instead of serving a
    # body whose digest does not match X-Package-Sha256.
    try:
        current_id = _file_id(p)
    except OSError:
        current_id = None
    if current_id is None or current_id != rec.file_id:
        rec = await ensure_digest(key)
        if rec is None or not p.is_file():
            return PlainTextResponse("package not found", status_code=404)

    size = rec.size
    digest = rec.sha256
    beh = dict(behavior)  # snapshot for this request

    def open_file():
        return open(p, "rb")

    f = await asyncio.to_thread(open_file)

    async def body():
        sent = 0
        chunk_size = max(1, int(beh["chunk_size"]))
        delay = max(0, int(beh["chunk_delay_ms"])) / 1000.0
        trunc = beh["truncate_after_bytes"]
        try:
            while True:
                if delay:
                    await asyncio.sleep(delay)
                chunk = await asyncio.to_thread(f.read, chunk_size)
                if not chunk:
                    break
                if trunc is not None and sent + len(chunk) > trunc:
                    remain = max(0, int(trunc) - sent)
                    if remain:
                        sent += remain
                        yield chunk[:remain]
                    # Simulate upstream abort: die mid-body after a partial
                    # response so the client sees a truncated payload.
                    raise UpstreamAbort("simulated upstream abort")
                sent += len(chunk)
                yield chunk
        finally:
            await asyncio.to_thread(f.close)

    headers = {
        "Content-Length": str(size),
        "X-Package-Sha256": digest,
    }
    return StreamingResponse(
        body(), media_type="application/octet-stream", headers=headers
    )


class Behavior(BaseModel):
    chunk_delay_ms: int = Field(0, ge=0, description="sleep per streamed chunk")
    truncate_after_bytes: int | None = Field(
        None, ge=0, description="abort the stream after this many bytes"
    )
    chunk_size: int = Field(256 * 1024, ge=1, description="origin stream chunk size")


@app.post("/control/behavior", tags=["control"])
async def set_behavior(b: Behavior):
    """Set delay / truncation for subsequent package downloads."""
    behavior.update(b.model_dump())
    return behavior


@app.get("/control/behavior", tags=["control"])
async def get_behavior():
    return behavior


@app.post(
    "/control/content/{path:path}",
    tags=["control"],
    summary="换包：同路径发布新版本（原子替换，晚到的旧摘要不会覆盖新版）",
)
async def change_content(path: str, size: int | None = None):
    """Regenerate the package with fresh random content (new sha256)."""
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)
    p = PKG_DIR / key
    if size is None:
        try:
            size = p.stat().st_size
        except OSError:
            size = 32 * 1024 * 1024
    if size <= 0:
        return PlainTextResponse("size must be positive", status_code=400)
    # Heavy IO stays off the event loop; in-flight backfill of the old version
    # keeps running but its result will be rejected by the generation check.
    digest, file_id = await asyncio.to_thread(_write_package, key, size)
    async with registry_lock:
        gen = (records[key].gen + 1) if key in records else 1
        rec = PackageRecord(key, file_id, size, gen)
        rec.sha256 = digest
        rec.ready.set()
        records[key] = rec
    return {"path": key, "size": size, "sha256": digest, "generation": gen}


@app.get(
    "/control/stats",
    tags=["control"],
    summary="查询统计：回源计数与每个包当前版的大小、摘要（含子目录包）",
)
async def stats():
    """Origin fetch counters and current per-version size/digest metadata."""
    # Disk IO happens outside the registry lock; only the registry itself is
    # serialized. Files dropped onto the volume out-of-band are adopted too.
    disk_keys = await asyncio.to_thread(_recover_from_disk)
    changed: list[str] = []
    for key in disk_keys:
        cur = records.get(key)
        try:
            fid = _file_id(PKG_DIR / key)
        except OSError:
            continue
        if cur is None or cur.file_id != fid:
            changed.append(key)
    async with registry_lock:
        for key in changed:
            _register_existing_locked(key)
        packages = {key: rec.snapshot() for key, rec in sorted(records.items())}
    return {
        "total_hits": hits["total"],
        "paths": dict(hits["paths"]),
        "packages": packages,
        "behavior": dict(behavior),
    }


@app.post("/control/reset", tags=["control"])
async def reset():
    hits["total"] = 0
    hits["paths"] = {}
    return {"ok": True}
