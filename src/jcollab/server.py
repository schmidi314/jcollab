"""jcollab MCP server (stdio). Tools address a notebook by ``instance`` (optional if only one server runs) and ``path``."""
from __future__ import annotations

import base64
import copy
import re
import uuid
from pathlib import PurePosixPath
from typing import Any

from mcp.server.mcpserver import Image, MCPServer

from . import kernel
from .discovery import discover, resolve
from .doc import DocError, DocSession, Sessions, set_outputs, set_text

INSTRUCTIONS = """Live co-editing of JupyterLab notebooks. Edits go through the shared Yjs document, so an open browser
tab shows them immediately. Every mutating tool reports `persisted` (the file on the server matches the live document);
if it is false, tell the user instead of assuming the edit is visible. Cells are addressed by index or cell id.
Rules: small inspections go through `run_code` (not stored in history, no execution count used). Anything big or
worth keeping runs as a visible code cell (`insert_cell` + `run_cell`) so the user sees the code. Mentions ("@claude" in a cell): handle only when the user asks (find_mentions, answer via add_comment). Put every markdown
heading in its own cell. Pass cell_type='markdown' explicitly for prose."""

mcp = MCPServer("jcollab", instructions=INSTRUCTIONS)
S = Sessions()
MAX_TEXT = 6000


async def _ds(instance: str | None, path: str) -> DocSession:
    inst = await resolve(instance)
    path = path.lstrip("/")
    root = inst.root_dir.rstrip("/") + "/"
    if path.startswith(root):
        path = path[len(root):]
    return S.doc(inst, path)


def _clip(s: str) -> str:
    return s if len(s) <= MAX_TEXT else s[: MAX_TEXT // 2] + f"\n…[{len(s) - MAX_TEXT} chars cut]…\n" + s[-MAX_TEXT // 2:]


_ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def render(outputs: list[dict]) -> list[Any]:
    """Outputs as MCP content: text, and real images for image/png|jpeg."""
    parts: list[Any] = []
    for o in outputs:
        t = o.get("output_type")
        if t == "stream":
            parts.append(_clip(o["text"] if isinstance(o["text"], str) else "".join(o["text"])))
        elif t == "error":
            tb = _ANSI.sub("", "\n".join(o.get("traceback") or [])[-1500:])
            parts.append(f"{o['ename']}: {o['evalue']}\n{tb}")
        elif t in ("execute_result", "display_data"):
            data = o.get("data", {})
            for mime, fmt in (("image/png", "png"), ("image/jpeg", "jpeg")):
                if mime in data:
                    parts.append(Image(data=base64.b64decode(data[mime]), format=fmt))
                    break
            txt = data.get("text/plain")
            if txt is not None and not any(isinstance(p, Image) for p in parts[-1:]):
                parts.append(_clip("".join(txt) if isinstance(txt, list) else txt))
    return parts or ["(no output)"]


async def _done(ds: DocSession, **info) -> dict:
    ok, detail = await ds.persisted()
    return {**info, "persisted": ok, **({} if ok else {"warning": detail})}


def _brief(c: dict, width: int = 160) -> dict:
    src = c["source"]
    c = {k: v for k, v in c.items() if k != "source"}
    c["source"] = src if len(src) <= width else src[:width] + "…"
    return c


# ── instances / files ──────────────────────────────────────────────────────────────────────────────────────────

@mcp.tool()
async def list_instances() -> list[dict]:
    """Running Jupyter servers of this user (found via Jupyter's runtime files, no setup needed)."""
    return [i.public() for i in await discover()]


@mcp.tool()
async def list_notebooks(instance: str | None = None, subdir: str = "") -> list[str]:
    """All .ipynb paths (relative to the server root) below `subdir`."""
    inst = await resolve(instance)
    rest = S.rest(inst)
    out: list[str] = []

    async def walk(d: str):
        for e in (await rest.contents(d, content=True))["content"]:
            if e["type"] == "directory" and not e["name"].startswith("."):
                await walk(e["path"])
            elif e["name"].endswith(".ipynb"):
                out.append(e["path"])
    await walk(subdir.strip("/"))
    return sorted(out)


# ── reading ────────────────────────────────────────────────────────────────────────────────────────────────────

@mcp.tool()
async def read_notebook(path: str, instance: str | None = None, start: int = 0, end: int | None = None,
                        full_source: bool = False) -> dict:
    """Overview of cells (index, id, type, execution count/state, metadata, source, number of outputs)."""
    ds = await _ds(instance, path)
    doc = await ds.doc()
    cells = (await ds.cells())[start:end]
    return {"path": ds.path, "n_cells": doc.cell_number, "notebook_metadata": doc._ymeta["metadata"].to_py(),
            "cells": cells if full_source else [_brief(c) for c in cells]}


@mcp.tool()
async def read_cell(path: str, cell: int | str, instance: str | None = None, include_outputs: bool = True) -> list[Any]:
    """One cell with full source, metadata and (rendered, incl. images) outputs."""
    ds = await _ds(instance, path)
    c = await ds.cell(cell, outputs=include_outputs)
    outs = c.pop("outputs", [])
    return [c] + (render(outs) if include_outputs and outs else [])


# ── editing ────────────────────────────────────────────────────────────────────────────────────────────────────

def _new_ycell(doc, cell_type: str, source: str, metadata: dict | None):
    cell = {"cell_type": cell_type, "source": source, "metadata": metadata or {}, "id": uuid.uuid4().hex[:8]}
    if cell_type == "code":
        cell.update(outputs=[], execution_count=None)
    return doc.create_ycell(cell)


@mcp.tool()
async def insert_cell(path: str, source: str, cell_type: str = "code", after: int | str | None = None,
                      instance: str | None = None, metadata: dict | None = None) -> dict:
    """Insert a cell after `after` (index or id); appends at the end if `after` is None (use insert_cell_at_start for the top).
    Always pass cell_type='markdown' for prose; keep each heading in its own cell."""
    if cell_type not in ("code", "markdown", "raw"):
        raise DocError("cell_type must be code, markdown or raw")
    ds = await _ds(instance, path)
    async with ds.lock:
        doc = await ds.doc()
        pos = doc.cell_number if after is None else await ds.index(after) + 1
        with doc._ydoc.transaction():
            doc.ycells.insert(pos, _new_ycell(doc, cell_type, source, metadata))
        return await _done(ds, index=pos, id=doc.ycells[pos].get("id"))


RESPONSE_MARKER = "<!-- jcollab-response -->"


@mcp.tool()
async def find_mentions(path: str, instance: str | None = None) -> list[dict]:
    """List cells containing "@claude" that are not answered yet (no response cell directly below).
    Only look when the user asks; never answer mentions on your own initiative."""
    ds = await _ds(instance, path)
    doc = await ds.doc()
    out, n = [], doc.cell_number
    for i in range(n):
        src = str(doc.ycells[i]["source"])
        if "@claude" not in src or RESPONSE_MARKER in src:
            continue
        nxt = str(doc.ycells[i + 1]["source"]) if i + 1 < n else ""
        if RESPONSE_MARKER in nxt:
            continue
        head = next((str(doc.ycells[j]["source"]).splitlines()[0] for j in range(i, -1, -1)
                     if doc.ycells[j].get("cell_type") == "markdown" and str(doc.ycells[j]["source"]).startswith("#")), None)
        out.append({"index": i, "id": doc.ycells[i].get("id"), "cell_type": doc.ycells[i].get("cell_type"),
                    "heading_above": head, "source": src})
    return out


@mcp.tool()
async def add_comment(path: str, after: int | str, response: str, instance: str | None = None) -> dict:
    """Answer a mention: insert a markdown response cell (carrying the jcollab-response marker) below cell `after`."""
    ds = await _ds(instance, path)
    async with ds.lock:
        doc = await ds.doc()
        pos = await ds.index(after) + 1
        with doc._ydoc.transaction():
            doc.ycells.insert(pos, _new_ycell(doc, "markdown", f"{RESPONSE_MARKER}\n{response}", None))
        return await _done(ds, index=pos, id=doc.ycells[pos].get("id"))


@mcp.tool()
async def insert_cell_at_start(path: str, source: str, cell_type: str = "markdown", instance: str | None = None) -> dict:
    """Insert a cell as the first cell of the notebook."""
    ds = await _ds(instance, path)
    async with ds.lock:
        doc = await ds.doc()
        with doc._ydoc.transaction():
            doc.ycells.insert(0, _new_ycell(doc, cell_type, source, None))
        return await _done(ds, index=0, id=doc.ycells[0].get("id"))


@mcp.tool()
async def edit_cell(path: str, cell: int | str, source: str, instance: str | None = None) -> dict:
    """Replace a cell's source with minimal text edits (open editors keep cursor/scroll). Does not change the cell type."""
    ds = await _ds(instance, path)
    async with ds.lock:
        yc = await ds.ycell(cell)
        with (await ds.doc())._ydoc.transaction():
            set_text(yc["source"], source)
        return await _done(ds, index=await ds.index(cell))


@mcp.tool()
async def change_cell_type(path: str, cell: int | str, cell_type: str, instance: str | None = None) -> dict:
    """Convert a cell to code/markdown/raw (keeps id, source and metadata; drops outputs when leaving code)."""
    ds = await _ds(instance, path)
    async with ds.lock:
        doc = await ds.doc()
        i = await ds.index(cell)
        old = doc.ycells[i]
        new = _new_ycell(doc, cell_type, str(old["source"]), old["metadata"].to_py())
        new["id"] = old.get("id")
        with doc._ydoc.transaction():
            del doc.ycells[i]
            doc.ycells.insert(i, new)
        return await _done(ds, index=i)


@mcp.tool()
async def delete_cell(path: str, cell: int | str, instance: str | None = None) -> dict:
    ds = await _ds(instance, path)
    async with ds.lock:
        doc = await ds.doc()
        i = await ds.index(cell)
        with doc._ydoc.transaction():
            del doc.ycells[i]
        return await _done(ds, deleted=i)


@mcp.tool()
async def move_cell(path: str, cell: int | str, to: int, instance: str | None = None) -> dict:
    """Move a cell so that it ends up at index `to` (counted after removal)."""
    ds = await _ds(instance, path)
    async with ds.lock:
        doc = await ds.doc()
        i = await ds.index(cell)
        data = doc.get_cell(i)
        with doc._ydoc.transaction():
            del doc.ycells[i]
            doc.ycells.insert(max(0, min(to, doc.cell_number)), doc.create_ycell(data))
        return await _done(ds, moved_from=i, to=to)


@mcp.tool()
async def split_cell(path: str, cell: int | str, at_line: int, instance: str | None = None) -> dict:
    """Split a cell before line number `at_line` (0-based); the second part becomes a new cell right after."""
    ds = await _ds(instance, path)
    async with ds.lock:
        doc = await ds.doc()
        i = await ds.index(cell)
        yc = doc.ycells[i]
        lines = str(yc["source"]).split("\n")
        head, tail = "\n".join(lines[:at_line]), "\n".join(lines[at_line:])
        with doc._ydoc.transaction():
            set_text(yc["source"], head)
            doc.ycells.insert(i + 1, _new_ycell(doc, yc["cell_type"], tail, yc["metadata"].to_py()))
        return await _done(ds, first=i, second=i + 1)


@mcp.tool()
async def merge_cells(path: str, first: int | str, last: int | str, instance: str | None = None) -> dict:
    """Merge cells first..last (inclusive) into the first one, joined by blank lines."""
    ds = await _ds(instance, path)
    async with ds.lock:
        doc = await ds.doc()
        a, b = await ds.index(first), await ds.index(last)
        if b <= a:
            raise DocError("last must be after first")
        joined = "\n\n".join(str(doc.ycells[k]["source"]) for k in range(a, b + 1))
        with doc._ydoc.transaction():
            set_text(doc.ycells[a]["source"], joined)
            for k in range(b, a, -1):
                del doc.ycells[k]
        return await _done(ds, merged_into=a)


# ── metadata ───────────────────────────────────────────────────────────────────────────────────────────────────

@mcp.tool()
async def get_metadata(path: str, cell: int | str | None = None, key: str | None = None, instance: str | None = None) -> Any:
    """Metadata of a cell (cell given) or of the notebook (cell=None, e.g. key='scenes_data'). Whole dict if key is None."""
    ds = await _ds(instance, path)
    doc = await ds.doc()
    meta = doc._ymeta["metadata"].to_py() if cell is None else (await ds.cell(cell))["metadata"]
    return meta if key is None else meta.get(key)


@mcp.tool()
async def set_metadata(path: str, key: str, value: Any = None, cell: int | str | None = None, delete: bool = False,
                       instance: str | None = None) -> dict:
    """Set (or delete) one metadata key on a cell (cell given) or the notebook (cell=None). Values are JSON."""
    ds = await _ds(instance, path)
    async with ds.lock:
        doc = await ds.doc()
        meta = doc._ymeta["metadata"] if cell is None else (await ds.ycell(cell))["metadata"]
        with doc._ydoc.transaction():
            if delete:
                if key in meta:
                    del meta[key]
            else:
                meta[key] = copy.deepcopy(value)
        return await _done(ds, scope="notebook" if cell is None else f"cell {cell}", key=key, deleted=delete)


# ── execution ──────────────────────────────────────────────────────────────────────────────────────────────────

@mcp.tool()
async def run_cell(path: str, cell: int | str, instance: str | None = None, timeout: float = 300) -> list[Any]:
    """Run a code cell like the user would: shows [*] while busy, streams outputs, sets the kernel's execution count
    and the execute-time timing metadata. Returns the outputs (images included)."""
    ds = await _ds(instance, path)
    r = await kernel.run_cell(ds, cell, timeout)
    head = {k: v for k, v in r.items() if k != "outputs"}
    head.update(await _done(ds))
    return [head] + render(r["outputs"])


@mcp.tool()
async def run_cells(path: str, start: int | str, end: int | str, instance: str | None = None, timeout: float = 300,
                    stop_on_error: bool = True) -> list[dict]:
    """Run code cells start..end (inclusive) one after another. Returns one status line per cell (no outputs)."""
    ds = await _ds(instance, path)
    a, b = await ds.index(start), await ds.index(end)
    res = []
    for i in range(a, b + 1):
        if (await ds.cell(i))["cell_type"] != "code":
            continue
        r = await kernel.run_cell(ds, i, timeout)
        res.append({k: v for k, v in r.items() if k != "outputs"} | {"errors": [o["ename"] for o in r["outputs"] if o["output_type"] == "error"]})
        if stop_on_error and (r["status"] != "ok" or r["timed_out"]):
            break
    return res


@mcp.tool()
async def run_code(path: str, code: str, instance: str | None = None, timeout: float = 60) -> list[Any]:
    """Quick inspection in the notebook's kernel (variables, shapes, small checks). Not recorded in history, uses no
    execution count, leaves the notebook untouched. Do NOT use for real computations; put those in a visible cell."""
    ds = await _ds(instance, path)
    r = await kernel.run_code(ds.rest, ds.path, code, timeout, await ds.doc())
    return render(r["outputs"]) + (["[timed out, kernel interrupted]"] if r["timed_out"] else [])


@mcp.tool()
async def clear_outputs(path: str, cell: int | str | None = None, instance: str | None = None) -> dict:
    """Clear outputs and execution count of one cell, or of all cells if `cell` is None."""
    ds = await _ds(instance, path)
    async with ds.lock:
        doc = await ds.doc()
        idx = range(doc.cell_number) if cell is None else [await ds.index(cell)]
        with doc._ydoc.transaction():
            for i in idx:
                yc = doc.ycells[i]
                if yc.get("cell_type") == "code":
                    set_outputs(yc, [])
                    yc["execution_count"] = None
                    if "execution" in yc["metadata"]:
                        del yc["metadata"]["execution"]
        return await _done(ds, cleared=len(idx))


@mcp.tool()
async def inspect_variables(path: str, instance: str | None = None, pattern: str = "") -> list[Any]:
    """Names, types and shapes of user variables in the kernel (silent, no history entry)."""
    code = (
        "import re as _re\n"
        "_rows=[]\n"
        "for _n,_v in list(globals().items()):\n"
        "    if _n.startswith('_') or _n in ('In','Out','get_ipython','exit','quit'): continue\n"
        f"    if not _re.search({pattern!r}, _n): continue\n"
        "    if type(_v).__name__ in ('module','function','builtin_function_or_method','type'): continue\n"
        "    _s=getattr(_v,'shape',None); _l=len(_v) if hasattr(_v,'__len__') else None\n"
        "    _rows.append(f'{_n}: {type(_v).__name__}'+(f' shape={_s}' if _s is not None else (f' len={_l}' if _l is not None else f' = {_v!r:.60}')))\n"
        "print('\\n'.join(_rows) or '(no user variables)')\n"
        "del _re,_rows")
    return await run_code(path, code, instance)


# ── kernel ─────────────────────────────────────────────────────────────────────────────────────────────────────

async def _kid(ds: DocSession) -> str:
    s = await ds.rest.session_for(ds.path)
    if not s:
        raise kernel.KernelError(f"{ds.path} has no kernel session (run a cell first)")
    return s["kernel"]["id"]


@mcp.tool()
async def kernel_status(path: str, instance: str | None = None) -> dict:
    ds = await _ds(instance, path)
    s = await ds.rest.session_for(ds.path)
    if not s:
        return {"kernel": None}
    k = (await ds.rest.request("GET", f"api/kernels/{s['kernel']['id']}")).json()
    return {"id": k["id"], "name": k["name"], "execution_state": k["execution_state"], "connections": k["connections"],
            "last_activity": k["last_activity"]}


@mcp.tool()
async def interrupt_kernel(path: str, instance: str | None = None) -> str:
    ds = await _ds(instance, path)
    await ds.rest.kernel_action(await _kid(ds), "interrupt")
    return "interrupted"


@mcp.tool()
async def restart_kernel(path: str, instance: str | None = None, run_all: bool = False, clear: bool = False,
                         timeout: float = 300) -> Any:
    """Restart the notebook's kernel (all in-memory state is lost). Optional: clear outputs first, run all cells after."""
    ds = await _ds(instance, path)
    if clear:
        await clear_outputs(path, None, instance)
    await ds.rest.kernel_action(await _kid(ds), "restart")
    doc = await ds.doc()
    with doc._ydoc.transaction():                      # a restart abandons whatever was running
        for i in range(doc.cell_number):
            if doc.ycells[i].get("execution_state") == "running":
                doc.ycells[i]["execution_state"] = "idle"
    if run_all:
        return await run_cells(path, 0, -1, instance, timeout)
    return "restarted"


@mcp.tool()
async def point_at(path: str, cell: int | str, seconds: float = 30, select: bool = True, instance: str | None = None) -> dict:
    """Show the user which cell you mean: a labelled "Metis" selection appears in that cell in every open tab (and the
    user can jump to it via JupyterLab's Collaborators sidebar). Still also name the cell in words (heading above it,
    first line), the user may not have the notebook scrolled there."""
    return await (await _ds(instance, path)).point_at(cell, seconds, select)


@mcp.tool()
async def user_focus(path: str, instance: str | None = None) -> list[dict]:
    """Where the user is: for every open browser tab the cell (index + id) holding the cursor, line/column and the
    selected text. Use this to resolve "this cell" / "the selected code" / "here". Empty if nobody has a cursor in the
    notebook (the user must have clicked into a cell; cursors expire after ~30 s without activity)."""
    return await (await _ds(instance, path)).user_focus()


@mcp.tool()
async def sync_health(path: str, instance: str | None = None) -> dict:
    """Is the shared document alive? Connection state, peers (e.g. browser tabs), and whether the file matches."""
    return await (await _ds(instance, path)).health()


def main() -> None:
    import logging
    for n in ("httpx", "httpcore", "websockets", "jupyter_nbmodel_client"):
        logging.getLogger(n).setLevel(logging.WARNING)
    mcp.run()


if __name__ == "__main__":
    main()
