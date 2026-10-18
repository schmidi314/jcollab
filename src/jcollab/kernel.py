"""Kernel execution that leaves cells exactly as if the user had run them in JupyterLab.

A cell run by the user shows: ``[*]`` while busy (shared cell field ``execution_state``), outputs streaming in, the
kernel's own ``execution_count``, and ``metadata.execution`` with the iopub/shell timestamps that
jupyterlab-execute-time displays. ``run_cell`` writes all of that into the shared document while the kernel runs.

Code that is only inspected (``run_code``) uses ``store_history=False`` so it never consumes an execution count.
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Any

import websockets

from .doc import DocError, DocSession, set_outputs, youtput
from .discovery import auth_headers
from .rest import Rest


class KernelError(RuntimeError):
    pass


def _ws_url(rest: Rest, kernel_id: str) -> str:
    base = rest.base.replace("http://", "ws://", 1)
    return f"{base}api/kernels/{kernel_id}/channels"


def _request(code: str, store_history: bool, session: str) -> tuple[str, dict]:
    msg_id = uuid.uuid4().hex
    return msg_id, {
        "header": {"msg_id": msg_id, "msg_type": "execute_request", "username": "metis", "session": session,
                   "version": "5.3", "date": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())},
        "parent_header": {}, "metadata": {}, "channel": "shell", "buffers": [],
        "content": {"code": code, "silent": False, "store_history": store_history, "user_expressions": {},
                    "allow_stdin": False, "stop_on_error": True},
    }


def _nb_output(mt: str, ct: dict) -> dict | None:
    if mt == "stream":
        return {"output_type": "stream", "name": ct.get("name", "stdout"), "text": ct.get("text", "")}
    if mt == "display_data":
        return {"output_type": "display_data", "data": ct.get("data", {}), "metadata": ct.get("metadata", {})}
    if mt == "execute_result":
        return {"output_type": "execute_result", "data": ct.get("data", {}), "metadata": ct.get("metadata", {}),
                "execution_count": ct.get("execution_count")}
    if mt == "error":
        return {"output_type": "error", "ename": ct.get("ename", ""), "evalue": ct.get("evalue", ""),
                "traceback": ct.get("traceback", [])}
    return None


class _Sink:
    """Receives kernel messages for one execute_request; optionally mirrors them into a shared-document cell."""

    def __init__(self, doc=None, ycell=None):
        self.doc, self.yc = doc, ycell
        self.outputs: list[dict] = []
        self.timing: dict[str, str] = {}
        self.count: int | None = None
        self.status = "unknown"
        self.display_ids: dict[str, int] = {}
        self._clear_next = False

    # ydoc helpers -----------------------------------------------------------------------------------------------
    def _youts(self):
        return self.yc["outputs"]

    def _push_timing(self):
        if self.yc is not None:
            with self.doc._ydoc.transaction():
                self.yc["metadata"]["execution"] = dict(self.timing)

    def _add(self, out: dict):
        if self._clear_next:
            self._clear_next = False
            self.outputs.clear()
            if self.yc is not None:
                with self.doc._ydoc.transaction():
                    self._youts().clear()
        last = self.outputs[-1] if self.outputs else None
        merged = out["output_type"] == "stream" and last and last["output_type"] == "stream" and last["name"] == out["name"]
        if merged:
            last["text"] += out["text"]
        else:
            self.outputs.append(out)
        if self.yc is None:
            return
        with self.doc._ydoc.transaction():
            youts = self._youts()
            if merged:
                youts[len(youts) - 1]["text"] += out["text"]
            else:
                youts.append(youtput(out))

    def _replace(self, idx: int, out: dict):
        self.outputs[idx] = out
        if self.yc is not None:
            with self.doc._ydoc.transaction():
                self._youts()[idx] = youtput(out)

    def clear(self):
        self.outputs.clear()
        if self.yc is not None:
            with self.doc._ydoc.transaction():
                self._youts().clear()

    # message handling -------------------------------------------------------------------------------------------
    def on_iopub(self, msg: dict):
        mt, ct = msg["msg_type"], msg.get("content", {})
        date = msg.get("header", {}).get("date")
        if mt == "execute_input":
            self.count = ct.get("execution_count")
            if date:
                self.timing["iopub.execute_input"] = date
            self._push_timing()
        elif mt == "status":
            state = ct.get("execution_state")
            if state == "busy" and date and "iopub.status.busy" not in self.timing:
                self.timing["iopub.status.busy"] = date
                self._push_timing()
            elif state == "idle":
                if date:
                    self.timing["iopub.status.idle"] = date
                self.status = "idle"
        elif mt == "clear_output":
            if ct.get("wait"):
                self._clear_next = True
            else:
                self.clear()
        elif mt == "update_display_data":
            did = ct.get("transient", {}).get("display_id")
            if did in self.display_ids:
                self._replace(self.display_ids[did], {"output_type": "display_data", "data": ct.get("data", {}),
                                                      "metadata": ct.get("metadata", {})})
        else:
            out = _nb_output(mt, ct)
            if out:
                if mt == "execute_result":
                    self.count = ct.get("execution_count", self.count)
                did = ct.get("transient", {}).get("display_id")
                self._add(out)
                if did:
                    self.display_ids[did] = len(self.outputs) - 1

    def on_reply(self, msg: dict):
        date = msg.get("header", {}).get("date")
        if date:
            self.timing["shell.execute_reply"] = date
        started = msg.get("metadata", {}).get("started")
        if started:
            self.timing["shell.execute_reply.started"] = started
        ct = msg.get("content", {})
        self.reply_status = ct.get("status")
        if ct.get("execution_count") is not None:
            self.count = ct["execution_count"]


async def _execute(rest: Rest, kernel_id: str, code: str, sink: _Sink, store_history: bool, timeout: float) -> bool:
    """Run code, feed messages into ``sink``. Returns False if the timeout hit (the kernel is interrupted then)."""
    msg_id, req = _request(code, store_history, uuid.uuid4().hex)
    sink.reply_status = None
    async with websockets.connect(_ws_url(rest, kernel_id), additional_headers=auth_headers(rest.inst) or None,
                                  max_size=64 * 1024 * 1024) as ws:
        await ws.send(json.dumps(req))
        deadline = time.monotonic() + timeout
        got_reply = False
        while not (got_reply and sink.status == "idle"):
            left = deadline - time.monotonic()
            if left <= 0:
                await rest.kernel_action(kernel_id, "interrupt")
                return False
            try:
                msg = json.loads(await asyncio.wait_for(ws.recv(), min(left, 5)))
            except asyncio.TimeoutError:
                continue
            if msg.get("parent_header", {}).get("msg_id") != msg_id:
                continue
            if msg.get("channel") == "shell" or msg["msg_type"] == "execute_reply":
                sink.on_reply(msg)
                got_reply = True
            else:
                sink.on_iopub(msg)
    return True


async def kernel_for(rest: Rest, ds_path: str, doc=None) -> str:
    name = "python3"
    if doc is not None:
        try:
            name = doc._ymeta["metadata"].to_py().get("kernelspec", {}).get("name") or name
        except Exception:  # noqa: BLE001
            pass
    sess = await rest.ensure_session(ds_path, name)
    return sess["kernel"]["id"]


async def run_cell(ds: DocSession, ref: int | str, timeout: float = 300) -> dict[str, Any]:
    doc = await ds.doc()
    i = await ds.index(ref)
    yc = doc.ycells[i]
    if yc.get("cell_type") != "code":
        raise DocError(f"Cell {i} is a {yc.get('cell_type')} cell, only code cells can run")
    code = str(yc["source"])
    kid = await kernel_for(ds.rest, ds.path, doc)
    sink = _Sink(doc, yc)
    t0 = time.monotonic()
    with doc._ydoc.transaction():              # same state a user click produces: [*], no outputs, no old timing
        yc["execution_state"] = "running"
        yc["execution_count"] = None
        set_outputs(yc, [])
        if "execution" in yc["metadata"]:
            del yc["metadata"]["execution"]
    finished = False
    try:
        finished = await _execute(ds.rest, kid, code, sink, True, timeout)
    finally:
        with doc._ydoc.transaction():
            yc["execution_count"] = sink.count
            yc["execution_state"] = "idle"
        sink._push_timing()
    return {"index": i, "id": yc.get("id"), "execution_count": int(sink.count) if sink.count is not None else None, "status": sink.reply_status,
            "timed_out": not finished, "seconds": round(time.monotonic() - t0, 2), "outputs": sink.outputs}


async def run_code(rest: Rest, path: str, code: str, timeout: float = 60, doc=None) -> dict[str, Any]:
    """Inspection run: not stored in kernel history, no execution count consumed, nothing written to the notebook."""
    kid = await kernel_for(rest, path, doc)
    sink = _Sink()
    finished = await _execute(rest, kid, code, sink, False, timeout)
    return {"status": sink.reply_status, "timed_out": not finished, "outputs": sink.outputs}
