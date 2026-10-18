import asyncio, sys
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
async def main():
    p = StdioServerParameters(command=sys.executable, args=["-m", "jcollab.server"])
    async with stdio_client(p) as (r, w):
        async with ClientSession(r, w) as s:
            await s.initialize()
            print(len((await s.list_tools()).tools), "tools")
            res = await s.call_tool("list_instances", {})
            print(res.content[0].text[:300])
            res = await s.call_tool("run_cell", {"path": "spike.ipynb", "cell": 3, "instance": "8899"})
            print([c.type for c in res.content], res.is_error)
asyncio.run(main())
