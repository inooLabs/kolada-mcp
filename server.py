import sys
from mcp.server.fastmcp import FastMCP
from starlette.responses import JSONResponse

from lifespan import app_lifespan
from entry_prompt import kolada_entry_point
from tools import (
    list_operating_areas,
    get_kpis_by_operating_area,
    get_kpi_metadata,
    search_kpis,
    fetch_kolada_data,
    analyze_kpi_across_municipalities,
    compare_kpis,
    list_municipalities,
    filter_municipalities_by_kpi,
)

mcp: FastMCP = FastMCP("KoladaServerLite", lifespan=app_lifespan, port=8001, host="0.0.0.0")

mcp.tool()(list_operating_areas)  # type: ignore[Context]
mcp.tool()(get_kpis_by_operating_area)  # type: ignore[Context]
mcp.tool()(get_kpi_metadata)  # type: ignore[Context]
mcp.tool()(search_kpis)  # type: ignore[Context]
mcp.tool()(fetch_kolada_data)  # type: ignore[Context]
mcp.tool()(analyze_kpi_across_municipalities)  # type: ignore[Context]
mcp.tool()(compare_kpis)  # type: ignore[Context]
mcp.tool()(list_municipalities)  # type: ignore[Context]
mcp.tool()(filter_municipalities_by_kpi)  # type: ignore[Context]

mcp.prompt()(kolada_entry_point)

@mcp.custom_route("/health", methods=["GET"])
async def health_check(request):
    return JSONResponse({"status": "ok"})

def main():
    print("[Kolada MCP Lite] Starting server on streamable-http...", file=sys.stderr)
    mcp.run("streamable-http")

if __name__ == "__main__":
    main()
