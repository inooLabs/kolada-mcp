import sys

from mcp.server.fastmcp import FastMCP

from entry_prompt import kolada_entry_point
from lifespan import app_lifespan
from tools import (
    analyze_kpi_across_municipalities,
    compare_kpis,
    fetch_kolada_data,
    filter_municipalities_by_kpi,
    get_kpi_metadata,
    get_kpis_by_operating_area,
    list_municipalities,
    list_operating_areas,
    search_kpis,
)

mcp: FastMCP = FastMCP(
    "KoladaServerLite", lifespan=app_lifespan, port=8001, host="0.0.0.0", stateless_http=True
)

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


def main():
    print("[Kolada MCP Lite] Starting server on streamable-http...", file=sys.stderr)
    mcp.run("streamable-http")


if __name__ == "__main__":
    main()
