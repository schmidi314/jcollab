"""Live notebook document: one Yjs connection per (server, notebook), with health checks and verified writes.

Why the care: a Yjs peer can silently stop being a peer (websocket closed, session expired, document room recreated)
while its in-memory copy keeps accepting edits that nobody ever sees. ``DocSession.ensure`` therefore re-checks the
connection before every operation, and ``DocSession.persisted`` confirms that an edit reached the file on the server.
"""
from __future__ import annotations

import asyncio
import copy
import difflib
import time
from typing import Any

from jupyter_nbmodel_client import NbModelClient, get_jupyter_notebook_websocket_url
from pycrdt import Assoc, YMessageType, read_message

from .discovery import client_versions, server_versions
from .rest import Rest

CLIENT_NAME = "metis"


class DocError(RuntimeError):
    pass


def set_text(ytext, new: str) -> None:
    """Replace a Yjs text with ``new`` using minimal edits, so open editors keep cursor and scroll position."""
    old = str(ytext)
    if old == new:
        return
    if len(old) + len(new) > 20000:
        ytext.clear()
        ytext += new
        return
    ops = difflib.SequenceMatcher(None, old, new, autojunk=False).get_opcodes()
    for tag, i1, i2, j1, j2 in reversed(ops):
        if tag == "equal":
            continue
        if i2 > i1:
            del ytext[i1:i2]
        if j2 > j1:
            ytext.insert(i1, new[j1:j2])


def youtput(o: dict):
    """One nbformat output as the Yjs value JupyterLab expects (stream text is a shared Text)."""
    from pycrdt import Map, Text
    o = copy.deepcopy(o)
    if o.get("output_type") == "stream":
        t = o.get("text", "")
        o["text"] = Text(t if isinstance(t, str) else "".join(t))
    return Map(o)


def set_outputs(yc, outputs: list[dict]) -> None:
    from pycrdt import Array
    if not isinstance(yc.get("outputs"), Array):
        yc["outputs"] = Array()
    youts = yc["outputs"]
    youts.clear()
    youts.extend([youtput(o) for o in outputs])


class LiveClient(NbModelClient):
    """NbModelClient that also applies incoming awareness messages (the stock client drops them), so the
    cursors/selections JupyterLab tabs publish are visible."""

    async def _on_message(self, websocket, message: bytes) -> None:
        if message and message[0] == YMessageType.AWARENESS:
            try:
                self._doc.awareness.apply_awareness_update(read_message(message[1:]), "remote")
            except Exception:  # noqa: BLE001 - awareness is advisory, never break syncing over it
                pass
            return
        await super()._on_message(websocket, message)


class DocSession:
    def __init__(self, rest: Rest, path: str):
        self.rest = rest
        self.path = path
        self._nb: NbModelClient | None = None
        self.lock = asyncio.Lock()
        self.reconnects = 0
        self._pointer: asyncio.Task | None = None

    # -- connection ------------------------------------------------------------------------------------------------

    @staticmethod
    def _running(nb: NbModelClient) -> bool:
        task = getattr(nb, "_NbModelClient__run", None)
        return task is not None and not task.done()

    async def ensure(self) -> NbModelClient:
        nb = self._nb
        if nb is not None and nb.synced and self._running(nb):
            return nb
        await self.close()
        hdr = await self.rest.xsrf_headers()
        token = self.rest.inst.token or None
        url = await asyncio.to_thread(get_jupyter_notebook_websocket_url, self.rest.base.rstrip("/"), self.path, token,
                                      10.0, None, hdr or None)
        nb = LiveClient(url, path=self.path, username=CLIENT_NAME, additional_headers=hdr or None)
        await nb.start()
        if not nb.synced:
            await nb.stop()
            raise DocError(f"Could not sync the shared document for {self.path} (is jupyter-collaboration installed and enabled?)")
        self._nb = nb
        self.reconnects += 1
        return nb

    async def close(self) -> None:
        nb, self._nb = self._nb, None
        if nb is not None:
            try:
                await nb.stop()
            except Exception:
                pass

    async def doc(self):
        """The ``jupyter_ydoc.YNotebook`` behind the live connection."""
        return (await self.ensure())._doc

    # -- addressing ------------------------------------------------------------------------------------------------

    async def index(self, ref: int | str) -> int:
        """Resolve a cell reference: index (negative counts from the end), digit string, or cell id."""
        doc = await self.doc()
        n = doc.cell_number
        if isinstance(ref, str):                      # an exact id wins, also when it looks like a number
            for i in range(n):
                if doc.ycells[i].get("id") == ref:
                    return i
            if not ref.lstrip("-").isdigit():
                raise DocError(f"No cell with id {ref!r} in {self.path}")
        i = int(ref)
        if i < 0:
            i += n
        if not 0 <= i < n:
            raise DocError(f"Cell index {ref} out of range (notebook has {n} cells)")
        return i

    async def ycell(self, ref: int | str):
        return (await self.doc()).ycells[await self.index(ref)]

    # -- reading ---------------------------------------------------------------------------------------------------

    async def cell(self, ref: int | str, outputs: bool = False) -> dict[str, Any]:
        doc = await self.doc()
        i = await self.index(ref)
        return self.describe(doc.ycells[i], i, outputs)

    @staticmethod
    def describe(yc, i: int, outputs: bool = False) -> dict[str, Any]:
        d = {"index": i, "id": yc.get("id"), "cell_type": yc.get("cell_type"), "source": str(yc["source"]),
             "metadata": yc["metadata"].to_py() if yc.get("metadata") is not None else {}}
        if d["cell_type"] == "code":
            ec = yc.get("execution_count")
            d["execution_count"] = int(ec) if ec is not None else None
            d["execution_state"] = yc.get("execution_state")
            outs = yc.get("outputs")
            d["n_outputs"] = len(outs) if outs is not None else 0
            if outputs:
                d["outputs"] = outs.to_py() if outs is not None else []
        return d

    async def cells(self, outputs: bool = False) -> list[dict[str, Any]]:
        doc = await self.doc()
        return [self.describe(doc.ycells[i], i, outputs) for i in range(doc.cell_number)]

    # -- verification ----------------------------------------------------------------------------------------------

    async def persisted(self, timeout: float = 4.0) -> tuple[bool, str]:
        """Wait until the file on the server shows the same cell sources as the live document."""
        doc = await self.doc()
        ids = [doc.ycells[i].get("id") for i in range(doc.cell_number)]
        if len(set(ids)) != len(ids):
            return False, ("live document has duplicate cell ids: client/server Yjs version skew. "
                           + self.skew_hint())
        want = [str(doc.ycells[i]["source"]) for i in range(doc.cell_number)]
        deadline = asyncio.get_running_loop().time() + timeout
        have: list[str] = []
        while True:
            try:
                nb = (await self.rest.contents(self.path))["content"]
                have = ["".join(c["source"]) if isinstance(c["source"], list) else c["source"] for c in nb["cells"]]
            except Exception as e:  # noqa: BLE001 - report, do not hide
                return False, f"could not read file back: {e}"
            if have == want:
                return True, "ok"
            if asyncio.get_running_loop().time() > deadline:
                diff = [i for i in range(max(len(have), len(want)))
                        if i >= len(have) or i >= len(want) or have[i] != want[i]][:5]
                ids = [str(doc.ycells[i].get("id")) for i in range(doc.cell_number)]
                return False, f"file on server differs from the live document (cells {diff}, file has {len(have)} cells, live has {len(want)}: ids {ids})"
            await asyncio.sleep(0.25)

    async def point_at(self, ref: int | str, seconds: float = 30.0, select: bool = True, scroll: bool = True,
                       force_scroll: bool = False, label: str | None = None) -> dict[str, Any]:
        """Show a labelled "Metis" cursor/selection in a cell for a while (visible in every open tab, and reachable via
        the Collaborators sidebar of JupyterLab)."""
        nb = await self.ensure()
        doc = nb._doc
        i = await self.index(ref)
        t = doc.ycells[i]["source"]
        n = len(t)
        if n == 0:
            raise DocError(f"Cell {i} is empty, nothing to point at (put some text in it first)")
        anchor = t.sticky_index(0, Assoc.AFTER).to_json()
        head = t.sticky_index(n - 1 if select else 0, Assoc.AFTER).to_json()
        nb.set_local_state_field("user", {"username": "metis", "name": "Metis", "display_name": "Metis", "initials": "M",
                                          "avatar_url": None, "color": "#d97757"})
        nb.set_local_state_field("cursors", [{"anchor": anchor, "head": head, "primary": True, "empty": not (select and n > 1)}])
        if scroll:
            nb.set_local_state_field("scrollTo", {"cell": doc.ycells[i].get("id"), "nonce": str(time.time_ns()), "force": force_scroll, "label": label})
        if self._pointer is not None:
            self._pointer.cancel()

        async def clear():
            await asyncio.sleep(seconds)
            try:
                nb.set_local_state_field("cursors", [])
                nb.set_local_state_field("scrollTo", None)
            except Exception:  # noqa: BLE001
                pass
        self._pointer = asyncio.create_task(clear())
        return {"pointing_at": i, "id": doc.ycells[i].get("id"), "seconds": seconds}

    async def user_focus(self) -> list[dict[str, Any]]:
        """Where other people (browser tabs) have their cursor/selection: cell, line, selected text.

        JupyterLab publishes cursors through Yjs awareness as relative positions naming a character item. The offset
        is resolved with pycrdt; the cell is the one whose text holds the same item at that offset."""
        from pycrdt import StickyIndex
        nb = await self.ensure()
        doc, aw = nb._doc, nb._doc.awareness
        out: list[dict[str, Any]] = []
        for cid, st in list(aw.states.items()):
            if cid == aw.client_id or not st.get("cursors"):
                continue
            user = st.get("user", {})
            who = user.get("display_name") or user.get("name") or user.get("owner") or str(cid)
            for cur in st["cursors"]:
                try:
                    a = StickyIndex.from_json(cur["anchor"]).get_index()
                    h = StickyIndex.from_json(cur["head"]).get_index()
                except Exception:  # noqa: BLE001
                    continue
                item = cur["anchor"].get("item")
                cands = []
                for i in range(doc.cell_number):
                    t = doc.ycells[i]["source"]
                    if a is None or len(t) < a:
                        continue
                    if item is None:
                        cands.append(i)           # cursor at the very end/start: no item to match
                        continue
                    if a >= len(t):
                        continue
                    try:
                        if t.sticky_index(a, Assoc(cur["anchor"].get("assoc", 0))).to_json().get("item") == item:
                            cands.append(i)
                    except BaseException:  # noqa: BLE001 - pycrdt panics on some positions
                        continue
                if not cands:
                    continue
                i = cands[0]
                src = str(doc.ycells[i]["source"])
                lo, hi = sorted((a, h if h is not None else a))
                out.append({"user": who, "cell_index": i, "cell_id": doc.ycells[i].get("id"),
                            "cell_type": doc.ycells[i].get("cell_type"),
                            "line": src.count("\n", 0, lo), "column": lo - (src.rfind("\n", 0, lo) + 1),
                            "selection": src[lo:hi] if hi > lo else None,
                            **({"ambiguous_cells": cands} if len(cands) > 1 else {})})
        return out

    def skew_hint(self) -> str:
        srv, cli = server_versions(self.rest.inst), client_versions()
        if srv is None:
            return f"Client has {cli}; install the same jupyter-ydoc and pycrdt versions as the Jupyter server."
        fix = f"{self.rest.inst.name} runs jupyter-ydoc {srv['jupyter-ydoc']}, pycrdt {srv['pycrdt']}; client has {cli}."
        return fix + f" Fix: pip install jupyter-ydoc=={srv['jupyter-ydoc']} pycrdt=={srv['pycrdt']} in the jcollab venv."

    async def health(self) -> dict[str, Any]:
        nb = await self.ensure()
        peers = []
        try:
            for cid in nb.get_connected_peers():
                u = (nb.get_peer_state(cid) or {}).get("user", {})
                peers.append(u.get("display_name") or u.get("agent") or u.get("owner") or str(cid))
        except Exception:  # noqa: BLE001
            pass
        viewers = None
        sess = await self.rest.session_for(self.path)
        if sess:  # awareness peers are not reliably reported; open frontends show up as kernel websocket connections
            viewers = (await self.rest.request("GET", f"api/kernels/{sess['kernel']['id']}")).json().get("connections")
        ok, detail = await self.persisted()
        srv, cli = server_versions(self.rest.inst), client_versions()
        return {"yjs_versions": {"client": cli, "server": srv and {k: srv[k] for k in ("jupyter-ydoc", "pycrdt")},
                                 "match": bool(srv) and all(srv[k] == cli[k] for k in cli)},
                "synced": nb.synced, "connections_made": self.reconnects, "awareness_peers": peers,
                "frontends_attached_to_kernel": viewers, "persisted": ok, "detail": detail}


class Sessions:
    """Cache of DocSession per (instance, notebook path)."""

    def __init__(self):
        self._rest: dict[int, Rest] = {}
        self._docs: dict[tuple[int, str], DocSession] = {}

    def rest(self, inst) -> Rest:
        r = self._rest.get(inst.port)
        if r is None or r.inst != inst:
            r = self._rest[inst.port] = Rest(inst)
        return r

    def doc(self, inst, path: str) -> DocSession:
        key = (inst.port, path)
        d = self._docs.get(key)
        if d is None or d.rest.inst != inst:
            d = self._docs[key] = DocSession(self.rest(inst), path)
        return d
