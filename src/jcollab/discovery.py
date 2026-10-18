"""Find running Jupyter servers of the current user via Jupyter's own runtime files.

Jupyter Server writes ``jpserver-<pid>.json`` (url, port, root_dir, pid, token; mode 600) into the
runtime dir for every server and removes it on clean shutdown. Crashed servers leave stale files, so
every candidate is checked for a live pid and probed on ``/api/status``.
"""
from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path

import httpx
from jupyter_core.paths import jupyter_runtime_dir


@dataclass(frozen=True)
class Instance:
    url: str            # always ends with "/" (includes base_url)
    token: str
    root_dir: str
    pid: int
    port: int

    @property
    def name(self) -> str:
        return f"{Path(self.root_dir).name or self.root_dir}:{self.port}"

    def public(self) -> dict:
        return {"name": self.name, "url": self.url, "root_dir": self.root_dir, "pid": self.pid, "port": self.port,
                "token_protected": bool(self.token)}


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _candidates() -> list[Instance]:
    out = []
    for f in sorted(Path(jupyter_runtime_dir()).glob("jpserver-*.json")):
        try:
            d = json.loads(f.read_text())
            if not _pid_alive(int(d["pid"])):
                continue
            url = d["url"] if d["url"].endswith("/") else d["url"] + "/"
            out.append(Instance(url=url, token=d.get("token") or "", root_dir=d.get("root_dir") or d.get("notebook_dir") or "",
                                pid=int(d["pid"]), port=int(d["port"])))
        except (OSError, ValueError, KeyError):
            continue
    return out


async def _probe(inst: Instance) -> bool:
    """Reach the server on loopback first: the advertised hostname (e.g. 0.0.0.0 or the host name) may not resolve."""
    try:
        async with httpx.AsyncClient(timeout=2) as c:
            r = await c.get(local_url(inst) + "api/status", headers=auth_headers(inst))
            return r.status_code == 200
    except httpx.HTTPError:
        return False


def local_url(inst: Instance) -> str:
    """URL on 127.0.0.1 with the instance's base_url path (servers bound to 0.0.0.0 or a host name are still reachable here)."""
    from urllib.parse import urlparse
    p = urlparse(inst.url)
    return f"http://127.0.0.1:{inst.port}{p.path}"


def auth_headers(inst: Instance) -> dict:
    return {"Authorization": f"token {inst.token}"} if inst.token else {}


async def discover() -> list[Instance]:
    cands = _candidates()
    ok = await asyncio.gather(*[_probe(i) for i in cands])
    live = [i for i, good in zip(cands, ok) if good]
    # A stale file can share a port with a live server only if the pid is alive but is a different process; keep the
    # entry that answered, drop duplicates by port.
    seen, out = set(), []
    for i in live:
        if i.port not in seen:
            seen.add(i.port)
            out.append(i)
    return out


async def resolve(selector: str | int | None) -> Instance:
    """Pick an instance by port, name (``Trading:8889``), root_dir basename or substring. Optional if exactly one is running."""
    live = await discover()
    if not live:
        raise RuntimeError("No running Jupyter server found (runtime dir has no live jpserver-*.json).")
    if selector in (None, ""):
        if len(live) == 1:
            return live[0]
        raise ValueError("Several Jupyter servers are running, pass `instance`: " + ", ".join(i.name for i in live))
    s = str(selector)
    exact = [i for i in live if s in (i.name, str(i.port), i.root_dir, Path(i.root_dir).name)]
    hits = exact or [i for i in live if s.lower() in i.name.lower() or s in i.root_dir]
    if len(hits) == 1:
        return hits[0]
    raise ValueError(f"Instance {s!r} " + ("is ambiguous: " if hits else "not found. Running: ") + ", ".join(i.name for i in (hits or live)))


_VERSIONS: dict[int, dict] = {}


def server_versions(inst: Instance) -> dict | None:
    """jupyter-ydoc / pycrdt versions of the Jupyter server process (same machine only), via its own interpreter."""
    import subprocess
    if inst.pid in _VERSIONS:
        return _VERSIONS[inst.pid]
    try:
        py = os.readlink(f"/proc/{inst.pid}/exe")
        out = subprocess.run([py, "-c", "import importlib.metadata as m;print(m.version('jupyter-ydoc'),m.version('pycrdt'))"],
                             capture_output=True, text=True, timeout=15).stdout.split()
        _VERSIONS[inst.pid] = {"jupyter-ydoc": out[0], "pycrdt": out[1], "python": py}
    except (OSError, IndexError, subprocess.SubprocessError):
        return None
    return _VERSIONS[inst.pid]


def client_versions() -> dict:
    import importlib.metadata as m
    return {"jupyter-ydoc": m.version("jupyter-ydoc"), "pycrdt": m.version("pycrdt")}
