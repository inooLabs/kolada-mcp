# Kolada MCP Server

An MCP (Model Context Protocol) server that provides access to the [Kolada API](https://www.kolada.se/), Sweden's official database for Key Performance Indicators (KPIs) about municipalities and regions.

## What it does

This server exposes tools for querying Swedish municipal statistics, including:

- Demographics and population data
- Education and school performance
- Healthcare and social services
- Economy and labor market
- Environment and infrastructure

## Running locally

```bash
pip install -r requirements.txt
python server.py
```

The server runs on port 8001 using streamable HTTP transport.

## Docker

```bash
docker build -t kolada-mcp .
docker run -p 8001:8001 kolada-mcp
```

Or with docker-compose:

```bash
docker-compose up
```

## Available tools

| Tool | Description |
|------|-------------|
| `list_operating_areas` | List all available KPI categories |
| `get_kpis_by_operating_area` | Get all KPIs within a specific category |
| `search_kpis` | Search for KPIs by keyword |
| `get_kpi_metadata` | Get detailed info about a specific KPI |
| `fetch_kolada_data` | Fetch actual data values for a KPI and municipality |
| `analyze_kpi_across_municipalities` | Compare municipalities with rankings and statistics |
| `compare_kpis` | Correlate two KPIs across municipalities |
| `list_municipalities` | List all Swedish municipalities |
| `filter_municipalities_by_kpi` | Filter municipalities by KPI value thresholds |

## Data source

All data comes from the public Kolada API at `https://api.kolada.se/v2`. No authentication required.

## Acknowledgments

This project is inspired by and built upon the work of [Hugi (@aerugo)](https://github.com/aerugo) and his original [kolada-mcp](https://github.com/aerugo/kolada-mcp) implementation. Thanks for the inspiration and building blocks! 🙏
