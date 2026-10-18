"""End-to-end check against a throwaway JupyterLab (root with spike.ipynb). Run: python tests/integration.py [instance]"""
import asyncio, logging, sys, time
logging.getLogger('httpx').setLevel(logging.WARNING)
from jcollab import server as s
from jupyter_nbmodel_client import NbModelClient, get_jupyter_notebook_websocket_url

INST = sys.argv[1] if len(sys.argv) > 1 else "8899"
NB = "spike.ipynb"
ok = True
def check(name, cond, extra=""):
    global ok; ok &= bool(cond); print(("PASS " if cond else "FAIL ") + name, extra)

async def main():
    print([i["name"] for i in await s.list_instances()])
    ds = await s._ds(INST, NB); doc = await ds.doc()
    # browser stand-in: a second peer that records execution_state of the cell we will run
    inst = await s.resolve(INST); rest = s.S.rest(inst); hdr = await rest.xsrf_headers()
    url = get_jupyter_notebook_websocket_url(rest.base.rstrip("/"), NB, None, 10.0, None, hdr)
    peer = NbModelClient(url, path=NB, username="browser-sim", additional_headers=hdr); await peer.start()
    seen = set()
    async def watch():
        while True:
            for i in range(peer._doc.cell_number):
                seen.add((peer._doc.ycells[i].get("id"), peer._doc.ycells[i].get("execution_state")))
            await asyncio.sleep(0.02)
    w = asyncio.create_task(watch())

    r = await s.insert_cell(NB, "import time\nprint('start'); time.sleep(1.5); print('end')\n1+1", "code", instance=INST)
    check("insert persisted", r["persisted"], r)
    cid = r["id"]
    r = await s.insert_cell(NB, "## Heading only", "markdown", after=cid, instance=INST)
    await asyncio.sleep(0.5)
    check("peer sees inserted cells", len(peer._doc.ycells) == doc.cell_number, (len(peer._doc.ycells), doc.cell_number))
    out = await s.run_cell(NB, cid, INST)
    head = out[0]; print(head, [o for o in out[1:]])
    c = await ds.cell(cid)
    check("busy state visible to peer", (cid, "running") in seen, sorted(str(x[1]) for x in seen if x[0] == cid))
    check("idle at end", c["execution_state"] == "idle")
    n1 = c["execution_count"]; check("execution_count set", isinstance(n1, int), n1)
    ex = c["metadata"].get("execution", {})
    check("timing keys", {"iopub.status.busy", "iopub.execute_input", "shell.execute_reply", "iopub.status.idle"} <= set(ex), list(ex))
    check("peer sees same count", peer._doc.ycells[await ds.index(cid)].get("execution_count") == n1)
    check("outputs written", c["n_outputs"] >= 2, c["n_outputs"])
    # inspection must not burn an execution count
    await s.run_code(NB, "z = 41", INST); await s.run_code(NB, "z + 1", INST)
    out2 = await s.run_cell(NB, cid, INST)
    n2 = (await ds.cell(cid))["execution_count"]
    check("run_code leaves counter alone", n2 == n1 + 1, (n1, n2))
    # image output
    r = await s.insert_cell(NB, "import matplotlib; matplotlib.use('module://matplotlib_inline.backend_inline')\nimport matplotlib.pyplot as plt\nplt.plot([1,2,3])\nplt.show()", "code", instance=INST)
    out3 = await s.run_cell(NB, r["id"], INST)
    check("image returned", any(type(p).__name__ == "Image" for p in out3), [type(p).__name__ for p in out3])
    # metadata + scenes
    m = await s.set_metadata(NB, "scenes_data", {"active_scene": "Default Scene", "init_scene": "", "scenes": ["Default Scene"]}, instance=INST)
    check("nb metadata set + persisted", m["persisted"], m)
    check("nb metadata read", (await s.get_metadata(NB, key="scenes_data", instance=INST))["active_scene"] == "Default Scene")
    await s.set_metadata(NB, "tags", ["demo"], cell=cid, instance=INST)
    check("cell metadata", (await s.get_metadata(NB, cid, "tags", INST)) == ["demo"])
    # structure ops
    sp = await s.split_cell(NB, cid, 1, INST); check("split", sp["persisted"], sp)
    mg = await s.merge_cells(NB, sp["first"], sp["second"], INST); check("merge", mg["persisted"], mg)
    hl = await s.sync_health(NB, INST); check("health", hl["synced"] and hl["persisted"], hl)
    st = await s.kernel_status(NB, INST); print(st)
    await s.restart_kernel(NB, INST); st = await s.kernel_status(NB, INST); print("after restart", st["execution_state"])
    w.cancel(); await peer.stop(); await ds.close()
    print("ALL OK" if ok else "SOME FAILED")
asyncio.run(main())
