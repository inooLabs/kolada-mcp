import sys
from typing import Any

from mcp.server.fastmcp.server import Context

from config import BASE_URL
from lifespan import LifespanContext


def _safe_ctx(ctx: Context) -> LifespanContext | None:  # type: ignore[Context]
    if (
        not ctx
        or not hasattr(ctx, "request_context")  # type: ignore[Context]
        or not ctx.request_context  # type: ignore[Context]
        or not hasattr(ctx.request_context, "lifespan_context")  # type: ignore[Context]
        or not ctx.request_context.lifespan_context  # type: ignore[Context]
    ):
        print("[Kolada MCP Lite] Invalid or incomplete context structure.", file=sys.stderr)
        return None
    return ctx.request_context.lifespan_context  # type: ignore[Context]


async def list_operating_areas(ctx: Context) -> list[dict[str, str | int]]:  # type: ignore[Context]
    lifespan_ctx: LifespanContext | None = _safe_ctx(ctx)
    if not lifespan_ctx:
        return []
    return lifespan_ctx.get("operating_areas_summary", [])


async def get_kpis_by_operating_area(
    operating_area: str,
    ctx: Context,  # type: ignore[Context]
) -> list[dict[str, Any]]:
    lifespan_ctx: LifespanContext | None = _safe_ctx(ctx)
    if not lifespan_ctx:
        return []
    kpi_list = lifespan_ctx.get("kpi_cache", [])
    target = operating_area.lower().strip()
    results: list[dict[str, Any]] = []
    for k in kpi_list:
        field = (k.get("operating_area") or "").lower()
        areas = {a.strip() for a in field.split(",")}
        if target in areas:
            results.append(k)
    return results


async def get_kpi_metadata(
    kpi_id: str,
    ctx: Context,  # type: ignore[Context]
) -> dict[str, Any]:
    lifespan_ctx: LifespanContext | None = _safe_ctx(ctx)
    if not lifespan_ctx:
        return {"error": "Server context structure invalid or incomplete."}
    k = lifespan_ctx.get("kpi_map", {}).get(kpi_id)
    if not k:
        return {"error": f"No KPI metadata found in cache for ID: {kpi_id}"}
    return k


async def search_kpis(
    keyword: str,
    ctx: Context,  # type: ignore[Context]
    limit: int = 20,
) -> list[dict[str, Any]]:
    lifespan_ctx: LifespanContext | None = _safe_ctx(ctx)
    if not lifespan_ctx:
        return []
    query = (keyword or "").lower().strip()
    if not query:
        return []
    tokens = [t for t in query.split() if t]
    simple_index = lifespan_ctx.get("simple_search_index", [])
    kpi_map = lifespan_ctx.get("kpi_map", {})
    scored: list[tuple[float, str]] = []
    for entry in simple_index:
        kid = entry.get("id")
        title_lc = entry.get("title_lc", "")
        desc_lc = entry.get("desc_lc", "")
        score = 0.0
        if query in title_lc:
            score += 5.0
        if query in desc_lc:
            score += 2.0
        for tok in tokens:
            if tok in title_lc:
                score += 1.0
            elif tok in desc_lc:
                score += 0.5
        if score > 0 and kid:
            scored.append((score, kid))
    scored.sort(key=lambda x: x[0], reverse=True)
    results: list[dict[str, Any]] = []
    for _, kid in scored[:limit]:
        k = kpi_map.get(kid)
        if k:
            results.append(k)
    return results


async def fetch_kolada_data(
    kpi_id: str,
    municipality_id: str,
    ctx: Context,  # type: ignore[Context]
    year: str | None = None,
    municipality_type: str = "K",
) -> dict[str, Any]:
    lifespan_ctx: LifespanContext | None = _safe_ctx(ctx)
    if not lifespan_ctx:
        return {"error": "Server context structure invalid or incomplete."}
    municipality_map = lifespan_ctx.get("municipality_map", {})
    muni_ids = [mid.strip() for mid in municipality_id.split(",") if mid.strip()]
    if not muni_ids:
        return {"error": "No valid municipality ID provided."}
    for mid in muni_ids:
        if mid not in municipality_map:
            return {"error": f"Municipality ID '{mid}' not found in system."}
        if municipality_type and municipality_map[mid].get("type") != municipality_type:
            return {"error": f"Municipality '{mid}' is not type '{municipality_type}'."}
    muni_ids_clean = ",".join(muni_ids)
    url: str = f"{BASE_URL}/data/kpi/{kpi_id}/municipality/{muni_ids_clean}"
    if year:
        url += f"/year/{year}"

    import httpx

    async with httpx.AsyncClient() as client:
        resp = await client.get(url, timeout=180.0)
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        values_list: list[dict[str, Any]] = data.get("values", [])
        for item in values_list:
            m_id: str = item.get("municipality", "Unknown")
            item["municipality_name"] = municipality_map.get(m_id, {}).get(
                "title", f"Kommun {m_id}"
            )
        return data


async def analyze_kpi_across_municipalities(
    kpi_id: str,
    ctx: Context,  # type: ignore[Context]
    year: str,
    sort_order: str = "desc",
    limit: int = 10,
    gender: str = "T",
    only_return_rate: bool = False,
    municipality_type: str = "K",
    municipality_ids: str | None = None,
) -> dict[str, Any]:
    from tools import get_kpi_metadata as _get_meta  # self import ok

    kpi_metadata_result: dict[str, Any] | dict[str, str] = await _get_meta(kpi_id, ctx)
    kpi_metadata: dict[str, Any] = {
        "id": kpi_id,
        "title": kpi_metadata_result.get("title", ""),
        "description": kpi_metadata_result.get("description", ""),
        "operating_area": kpi_metadata_result.get("operating_area", ""),
    }

    from tools import fetch_kolada_data as _fetch

    # Build URL like original util
    def _parse_years(year_str: str) -> list[str]:
        if not year_str:
            return []
        return [y.strip() for y in year_str.split(",") if y.strip()]

    year_list: list[str] = _parse_years(year)

    def _group(data: dict[str, Any], gender: str) -> dict[str, dict[str, float]]:
        raw_rows: list[dict[str, Any]] = []
        for item in data.get("values", []):
            municipality_id: str | None = item.get("municipality")
            raw_period: int | str | None = item.get("period")
            if not municipality_id or raw_period is None:
                continue
            period_str: str = str(raw_period)
            for subval in item.get("values", []):
                if subval.get("gender") != gender:
                    continue
                val = subval.get("value")
                raw_rows.append({
                    "municipality": municipality_id,
                    "period": period_str,
                    "value": val,
                })
        muni: dict[str, dict[str, float]] = {}
        for r in raw_rows:
            m_id = r["municipality"]
            p = r["period"]
            try:
                v = float(r["value"]) if r["value"] is not None else None
            except (TypeError, ValueError):
                v = None
            if v is None:
                continue
            muni.setdefault(m_id, {})[p] = v
        return muni

    lifespan_ctx: LifespanContext | None = _safe_ctx(ctx)
    if not lifespan_ctx:
        return {"error": "Server context structure invalid or incomplete."}
    municipality_map = lifespan_ctx.get("municipality_map", {})

    # Fetch live data
    muni_ids = None if not municipality_ids else municipality_ids
    if muni_ids:
        url = f"{BASE_URL}/data/kpi/{kpi_id}/municipality/{muni_ids}"
        if year:
            url += f"/year/{year}"
    else:
        if year:
            url = f"{BASE_URL}/data/kpi/{kpi_id}/year/{year}"
        else:
            url = f"{BASE_URL}/data/kpi/{kpi_id}"
    data = await _fetch(kpi_id, municipality_ids or ",".join(municipality_map.keys()), ctx, year)
    if "error" in data:
        return {"error": data["error"], "kpi_info": kpi_metadata}

    municipality_data = _group(data, gender)

    # Filter by type
    filtered: dict[str, dict[str, float]] = {}
    for m_id, vals in municipality_data.items():
        if m_id in municipality_map:
            actual_type = municipality_map[m_id].get("type", "")
            if not municipality_type or actual_type == municipality_type:
                filtered[m_id] = vals

    # If municipality_ids supplied: return flat list with deltas
    def _build_flat_with_delta(municipality_data: dict[str, dict[str, float]], years: list[str]):
        flat_list = []
        for m_id, year_vals in municipality_data.items():
            available_years = [y for y in years if y in year_vals]
            available_years.sort()
            entry: dict[str, Any] = {
                "municipality_id": m_id,
                "municipality_name": municipality_map.get(m_id, {}).get("title", f"Kommun {m_id}"),
                "data": {y: year_vals[y] for y in available_years},
            }
            if available_years:
                entry["latest_year"] = available_years[-1]
                entry["latest_value"] = year_vals[available_years[-1]]
                if len(available_years) >= 2:
                    entry["earliest_year"] = available_years[0]
                    entry["earliest_value"] = year_vals[available_years[0]]
                    entry["delta_value"] = year_vals[available_years[-1]] - year_vals[available_years[0]]
            flat_list.append(entry)
        return flat_list

    if municipality_ids:
        result_list = _build_flat_with_delta(filtered, year_list)
        return {
            "kpi_info": kpi_metadata,
            "selected_years": year_list,
            "selected_gender": gender,
            "municipalities_count": len(result_list),
            "municipalities_data": result_list,
        }

    # Build ranking lists
    latest_values: list[float] = []
    full_list: list[dict[str, Any]] = []
    delta_list: list[dict[str, Any]] = []
    delta_values: list[float] = []

    sorted_years = sorted(year_list)
    for m_id, year_vals in filtered.items():
        available_years = [y for y in sorted_years if y in year_vals]
        if not available_years:
            continue
        earliest = available_years[0]
        latest = available_years[-1]
        earliest_val = year_vals[earliest]
        latest_val = year_vals[latest]

        full_list.append({
            "municipality_id": m_id,
            "municipality_name": municipality_map.get(m_id, {}).get("title", f"Kommun {m_id}"),
            "latest_year": latest,
            "latest_value": latest_val,
            "years_in_data": available_years,
        })
        latest_values.append(latest_val)

        if len(available_years) >= 2:
            delta_value = latest_val - earliest_val
            delta_entry = {
                "municipality_id": m_id,
                "municipality_name": municipality_map.get(m_id, {}).get("title", f"Kommun {m_id}"),
                "earliest_year": earliest,
                "earliest_value": earliest_val,
                "latest_year": latest,
                "latest_value": latest_val,
                "delta_value": delta_value,
            }
            delta_list.append(delta_entry)
            delta_values.append(delta_value)

    def _summary_stats(values: list[float]) -> dict[str, Any]:
        import statistics
        if not values:
            return {"min": None, "max": None, "mean": None, "median": None, "count": 0}
        return {
            "min": min(values),
            "max": max(values),
            "mean": statistics.mean(values),
            "median": statistics.median(values),
            "count": len(values),
        }

    def _rank_slice(data: list[dict[str, Any]], key: str, order: str, limit: int):
        is_desc = order.lower() == "desc"
        sorted_data = sorted(data, key=lambda x: (x.get(key, 0.0), x.get("municipality_id", "")), reverse=is_desc)
        n = len(sorted_data)
        if n == 0:
            return [], [], []
        safe_limit = max(1, min(limit, n))
        top_list = sorted_data[:safe_limit]
        bottom_list = list(reversed(sorted_data[-safe_limit:]))
        median_start = max(0, (n - 1) // 2 - (safe_limit // 2))
        median_start = min(median_start, n - safe_limit)
        median_list = sorted_data[median_start:median_start + safe_limit]
        return top_list, bottom_list, median_list

    top_main, bottom_main, median_main = _rank_slice(full_list, "latest_value", sort_order, limit)
    top_delta, bottom_delta, median_delta = _rank_slice(delta_list, "delta_value", sort_order, limit)

    return {
        "kpi_info": kpi_metadata,
        "selected_years": year_list,
        "selected_gender": gender,
        "municipalities_count": len(full_list),
        "summary_stats": {
            "min_latest": _summary_stats(latest_values).get("min"),
            "max_latest": _summary_stats(latest_values).get("max"),
            "mean_latest": _summary_stats(latest_values).get("mean"),
            "median_latest": _summary_stats(latest_values).get("median"),
            "count": _summary_stats(latest_values).get("count"),
        },
        "top_municipalities": top_main,
        "bottom_municipalities": bottom_main,
        "median_municipalities": median_main,
        "top_delta_municipalities": top_delta,
        "bottom_delta_municipalities": bottom_delta,
        "median_delta_municipalities": median_delta,
        "multi_year_delta": len(sorted_years) > 1,
        "only_return_rate": only_return_rate,
    }


async def compare_kpis(
    kpi1_id: str,
    kpi2_id: str,
    year: str,
    ctx: Context,  # type: ignore[Context]
    gender: str = "T",
    municipality_type: str = "K",
    municipality_ids: str | None = None,
) -> dict[str, Any]:
    # Reuse analyze code by fetching both datasets and computing correlation/difference
    def _parse_years(year_str: str) -> list[str]:
        if not year_str:
            return []
        return [y.strip() for y in year_str.split(",") if y.strip()]

    year_list = _parse_years(year)
    is_multi_year = len(year_list) > 1

    from tools import fetch_kolada_data as _fetch

    data1 = await _fetch(kpi1_id, municipality_ids or "", ctx, year, municipality_type)
    data2 = await _fetch(kpi2_id, municipality_ids or "", ctx, year, municipality_type)
    if "error" in data1:
        return {"error": data1["error"]}
    if "error" in data2:
        return {"error": data2["error"]}

    def _group(data: dict[str, Any]) -> dict[str, dict[str, float]]:
        grouped: dict[str, dict[str, float]] = {}
        for item in data.get("values", []):
            m = item.get("municipality")
            p = str(item.get("period"))
            for sub in item.get("values", []):
                if sub.get("gender") == gender and sub.get("value") is not None:
                    grouped.setdefault(m, {})[p] = float(sub.get("value"))
        return grouped

    g1 = _group(data1)
    g2 = _group(data2)

    # Restrict to municipality_type present in context
    lifespan_ctx: LifespanContext | None = _safe_ctx(ctx)
    if not lifespan_ctx:
        return {"error": "Server context structure invalid or incomplete."}
    municipality_map = lifespan_ctx.get("municipality_map", {})

    def _filter_type(g: dict[str, dict[str, float]]):
        res: dict[str, dict[str, float]] = {}
        for m_id, vals in g.items():
            if municipality_type and municipality_map.get(m_id, {}).get("type") != municipality_type:
                continue
            res[m_id] = vals
        return res

    g1 = _filter_type(g1)
    g2 = _filter_type(g2)

    import statistics

    def _corr(xs: list[float], ys: list[float]) -> float | None:
        if len(xs) < 2 or len(ys) < 2:
            return None
        try:
            return statistics.correlation(xs, ys)
        except (statistics.StatisticsError, ValueError):
            return None

    result: dict[str, Any] = {
        "kpi1_info": {"id": kpi1_id},
        "kpi2_info": {"id": kpi2_id},
        "selected_years": year_list,
        "gender": gender,
        "municipality_type": municipality_type,
        "multi_year": is_multi_year,
    }

    if not is_multi_year:
        if not year_list:
            return {**result, "error": "No valid year specified for single-year analysis."}
        y = year_list[0]
        diffs = []
        entries = []
        for m_id, vals1 in g1.items():
            vals2 = g2.get(m_id)
            if not vals2:
                continue
            if y in vals1 and y in vals2:
                v1 = vals1[y]
                v2 = vals2[y]
                entries.append({
                    "municipality_id": m_id,
                    "kpi1_value": v1,
                    "kpi2_value": v2,
                    "difference": v2 - v1,
                })
                diffs.append(v2 - v1)
        if not entries:
            return {**result, "error": f"No overlapping data for single year {y}."}
        entries.sort(key=lambda e: e["difference"]) 
        n = len(entries)
        sl = min(10, n)
        median_start = max(0, (n - 1) // 2 - (sl // 2))
        median_end = min(median_start + sl, n)
        result.update({
            "municipality_differences": entries,
            "top_difference_municipalities": list(reversed(entries[-sl:])),
            "bottom_difference_municipalities": entries[:sl],
            "median_difference_municipalities": entries[median_start:median_end],
            "overall_correlation": _corr(diffs, diffs),
        })
        return result

    # Multi-year: per-municipality correlations and overall
    municipality_correlations = []
    big_x: list[float] = []
    big_y: list[float] = []
    for m_id, vals1 in g1.items():
        vals2 = g2.get(m_id)
        if not vals2:
            continue
        years = sorted(set(vals1.keys()) & set(vals2.keys()))
        if len(years) < 2:
            continue
        xs = [vals1[y] for y in years]
        ys = [vals2[y] for y in years]
        corr = _corr(xs, ys)
        if corr is not None:
            municipality_correlations.append({
                "municipality_id": m_id,
                "correlation": corr,
                "years_used": years,
                "n_years": len(years),
            })
            big_x.extend(xs)
            big_y.extend(ys)

    if not municipality_correlations:
        return {**result, "error": "No municipality had 2+ overlapping data points to compute correlation."}

    municipality_correlations.sort(key=lambda e: e["correlation"])
    n = len(municipality_correlations)
    sl = min(10, n)
    median_start = max(0, (n - 1) // 2 - (sl // 2))
    median_end = min(median_start + sl, n)

    result.update({
        "municipality_correlations": municipality_correlations,
        "top_correlation_municipalities": list(reversed(municipality_correlations[-sl:])),
        "bottom_correlation_municipalities": municipality_correlations[:sl],
        "median_correlation_municipalities": municipality_correlations[median_start:median_end],
        "overall_correlation": _corr(big_x, big_y),
    })
    return result


async def list_municipalities(
    ctx: Context,  # type: ignore[Context]
    municipality_type: str = "K",
) -> list[dict[str, str]]:
    lifespan_ctx: LifespanContext | None = _safe_ctx(ctx)
    if not lifespan_ctx:
        return []
    municipality_map = lifespan_ctx.get("municipality_map", {})
    result: list[dict[str, str]] = []
    for m_id, muni in municipality_map.items():
        if municipality_type and muni.get("type") != municipality_type:
            continue
        result.append({"id": m_id, "name": muni.get("title", f"Municipality {m_id}")})
    result.sort(key=lambda x: x["id"])
    return result


async def filter_municipalities_by_kpi(
    ctx: Context,  # type: ignore[Context]
    kpi_id: str,
    cutoff: float,
    operator: str = "above",
    year: str | None = None,
    municipality_type: str = "K",
    gender: str = "T",
) -> list[dict[str, Any]]:
    lifespan_ctx: LifespanContext | None = _safe_ctx(ctx)
    if not lifespan_ctx:
        return []
    municipality_map = lifespan_ctx.get("municipality_map", {})
    filtered_ids = [
        m_id for m_id, muni in municipality_map.items()
        if not municipality_type or muni.get("type") == municipality_type
    ]
    if not filtered_ids:
        return []
    muni_ids_str = ",".join(filtered_ids)
    data_response = await fetch_kolada_data(kpi_id, muni_ids_str, ctx, year)
    if "error" in data_response:
        return [data_response]
    values_list = data_response.get("values", [])
    latest_by_muni: dict[str, dict[str, Any]] = {}
    if year:
        for rec in values_list:
            if str(rec.get("period")) == year:
                latest_by_muni[rec.get("municipality")] = rec
    else:
        for rec in values_list:
            m_id = rec.get("municipality")
            if m_id is None:
                continue
            period = rec.get("period")
            if period is None:
                continue
            cur = latest_by_muni.get(m_id)
            if cur is None or rec.get("period") > cur.get("period"):
                latest_by_muni[m_id] = rec
    results: list[dict[str, Any]] = []
    for m_id, rec in latest_by_muni.items():
        val = None
        for d in rec.get("values", []):
            if d.get("gender") == gender:
                val = d.get("value")
                break
        if val is None:
            continue
        try:
            val_float = float(val)
        except (ValueError, TypeError):
            continue
        include = (operator == "above" and val_float > cutoff) or (operator == "below" and val_float < cutoff)
        if include:
            results.append({
                "municipality_id": m_id,
                "period": rec.get("period"),
                "value": val_float,
                "cutoff": cutoff,
                "difference": val_float - cutoff,
                "municipality_name": municipality_map.get(m_id, {}).get("title", f"Municipality {m_id}"),
            })
    results.sort(key=lambda x: x["municipality_id"])
    return results
