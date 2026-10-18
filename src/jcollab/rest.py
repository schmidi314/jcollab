"""Async REST access to one Jupyter server (contents, sessions, kernels), XSRF-safe for token-less servers."""
from __future__ import annotations

import httpx

from .discovery import Instance, auth_headers, local_url


class Rest:
    def __init__(self, inst: Instance):
        self.inst = inst
        self.base = local_url(inst)
        self._c = httpx.AsyncClient(timeout=15)

    async def _headers(self, write: bool) -> dict:
        h = auth_headers(self.inst)
        if write and not self.inst.token:
            if not self._c.cookies.get("_xsrf"):
                await self._c.get(self.base + "login", follow_redirects=True)  # sets the _xsrf cookie even if the page 404s
            xs = self._c.cookies.get("_xsrf")
            if xs:
                h["X-XSRFToken"] = xs
                h["Cookie"] = f"_xsrf={xs}"
        return h

    async def request(self, method: str, path: str, *, write: bool = False, **kw) -> httpx.Response:
        r = await self._c.request(method, self.base + path.lstrip("/"), headers=await self._headers(write), **kw)
        r.raise_for_status()
        return r

    async def xsrf_headers(self) -> dict:
        return await self._headers(True)

    async def contents(self, path: str, content: bool = True) -> dict:
        return (await self.request("GET", f"api/contents/{path}", params={"content": int(content)})).json()

    async def sessions(self) -> list[dict]:
        return (await self.request("GET", "api/sessions")).json()

    async def session_for(self, path: str) -> dict | None:
        for s in await self.sessions():
            if s.get("path") == path or s.get("notebook", {}).get("path") == path:
                return s
        return None

    async def ensure_session(self, path: str, kernel_name: str = "python3") -> dict:
        s = await self.session_for(path)
        if s:
            return s
        r = await self.request("POST", "api/sessions", write=True, json={
            "kernel": {"name": kernel_name}, "path": path, "name": path, "type": "notebook"})
        return r.json()

    async def kernel_action(self, kernel_id: str, action: str) -> None:
        await self.request("POST", f"api/kernels/{kernel_id}/{action}", write=True)

    async def aclose(self) -> None:
        await self._c.aclose()
