import sys
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, TypedDict, Required, cast

import httpx
from mcp.server.fastmcp import FastMCP

from config import BASE_URL, KPI_PER_PAGE


class Kpi(TypedDict, total=False):
    id: Required[str]
    title: str
    description: str
    operating_area: str


class Municipality(TypedDict, total=False):
    id: str
    title: str
    type: str


class LifespanContext(TypedDict):
    kpi_cache: list[Kpi]
    kpi_map: dict[str, Kpi]
    municipality_cache: list[Municipality]
    municipality_map: dict[str, Municipality]
    operating_areas_summary: list[dict[str, str | int]]
    simple_search_index: list[dict[str, str]]


def get_operating_areas_summary(kpis: list[Kpi]) -> list[dict[str, str | int]]:
    grouped: dict[str, list[Kpi]] = {}
    for kpi in kpis:
        areas = [a.strip() for a in (kpi.get("operating_area", "")).split(",")]
        for area in areas:
            if not area:
                continue
            grouped.setdefault(area, []).append(kpi)
    return [
        {"operating_area": area, "kpi_count": len(items)}
        for area, items in sorted(grouped.items())
    ]


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[LifespanContext]:
    print("[Kolada MCP Lite] Starting lifespan setup...", file=sys.stderr)

    kpi_list: list[Kpi] = []
    municipality_list: list[Municipality] = []

    async with httpx.AsyncClient() as client:
        next_url: str | None = f"{BASE_URL}/kpi?per_page={KPI_PER_PAGE}"
        while next_url:
            print(f"[Kolada MCP Lite] Fetching page: {next_url}", file=sys.stderr)
            resp = await client.get(next_url, timeout=180.0)
            resp.raise_for_status()
            data: dict[str, Any] = resp.json()
            kpi_list.extend(cast(list[Kpi], data.get("values", [])))
            next_url = data.get("next_page")

    print(
        f"[Kolada MCP Lite] Fetched {len(kpi_list)} total KPIs from Kolada.",
        file=sys.stderr,
    )

    async with httpx.AsyncClient() as client:
        muni_url = f"{BASE_URL}/municipality"
        print(f"[Kolada MCP Lite] Fetching municipalities: {muni_url}", file=sys.stderr)
        resp = await client.get(muni_url, timeout=180.0)
        resp.raise_for_status()
        mun_data: dict[str, Any] = resp.json()
        municipality_list = cast(list[Municipality], mun_data.get("values", []))

    kpi_map: dict[str, Kpi] = {}
    for k in kpi_list:
        kid = k.get("id")
        if kid:
            kpi_map[kid] = k

    municipality_map: dict[str, Municipality] = {}
    for m in municipality_list:
        mid = m.get("id")
        if mid:
            municipality_map[mid] = m

    operating_areas_summary = get_operating_areas_summary(kpi_list)

    simple_index = []
    for k in kpi_list:
        kid = k.get("id")
        if not kid:
            continue
        simple_index.append(
            {
                "id": kid,
                "title_lc": (k.get("title") or "").lower(),
                "desc_lc": (k.get("description") or "").lower(),
            }
        )

    ctx: LifespanContext = {
        "kpi_cache": kpi_list,
        "kpi_map": kpi_map,
        "municipality_cache": municipality_list,
        "municipality_map": municipality_map,
        "operating_areas_summary": operating_areas_summary,
        "simple_search_index": simple_index,
    }

    print("[Kolada MCP Lite] Initialization complete.", file=sys.stderr)
    try:
        yield ctx
    finally:
        print("[Kolada MCP Lite] Shutdown.", file=sys.stderr)


