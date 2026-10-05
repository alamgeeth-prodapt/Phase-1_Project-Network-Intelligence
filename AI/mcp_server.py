"""
Milano Telecom Network Operations Center (NOC) - Model Context Protocol (MCP) Server.

Exposes Milano network analytics tools, live resources, and diagnostic prompt templates
to any standard MCP client (e.g. Antigravity IDE, Claude Desktop, autonomous agents).
"""

import os
import sys
import json
from pathlib import Path
from typing import Optional, Dict, Any

# Ensure backend directory is in python search path
BASE_DIR = Path(__file__).resolve().parent.parent
BACKEND_DIR = BASE_DIR / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv

# Load database environment if not already set
load_dotenv(BACKEND_DIR / ".env.sql")

from mcp.server.mcpserver import MCPServer
from database import SessionLocal
from ai.plugins.registry import default_registry

# Initialize the MCP Server instance
server = MCPServer(
    name="milano-noc-mcp-server",
    title="Milano Telecom NOC Assistant Server",
    description="Standard MCP Server providing tools and resources for Milano network monitoring and anomaly triage.",
    version="1.0.0",
)


def _serialize_result(result: Any) -> str:
    """Helper to convert Pydantic models or dicts to JSON string."""
    if hasattr(result, "model_dump"):
        result = result.model_dump()
    elif hasattr(result, "dict"):
        result = result.dict()
    return json.dumps(result, default=str)


# ==============================================================================
# 1. MCP TOOLS (Standard Tool Calls)
# ==============================================================================

@server.tool(
    name="get_network_summary",
    description="Retrieve citywide network activity summary, peak hours, and top active grid for a specific date or latest.",
)
def get_network_summary(summary_date: Optional[str] = None) -> str:
    with SessionLocal() as db:
        res = default_registry.execute_tool(
            "get_network_summary",
            {"summary_date": summary_date} if summary_date else {},
            db=db,
        )
        return _serialize_result(res)


@server.tool(
    name="get_top_grids",
    description="Retrieve the top active spatial grids ranked by total network volume for a given date.",
)
def get_top_grids(date: Optional[str] = None, limit: int = 10) -> str:
    with SessionLocal() as db:
        res = default_registry.execute_tool(
            "get_top_grids",
            {"date": date, "limit": limit},
            db=db,
        )
        return _serialize_result(res)


@server.tool(
    name="get_grid_activity",
    description="Get activity history timeseries for a specific grid (1 to 10000) with optional date or hour filters.",
)
def get_grid_activity(
    grid_id: int,
    date: Optional[str] = None,
    hour: Optional[int] = None,
    as_of: Optional[str] = None,
) -> str:
    with SessionLocal() as db:
        tool_input = {"grid_id": grid_id}
        if date:
            tool_input["date"] = date
        if hour is not None:
            tool_input["hour"] = hour
        if as_of:
            tool_input["as_of"] = as_of
        res = default_registry.execute_tool("get_grid_activity", tool_input, db=db)
        return _serialize_result(res)


@server.tool(
    name="get_grid_features",
    description="Retrieve ML features and data quality assessment for a specific grid.",
)
def get_grid_features(grid_id: int, as_of: Optional[str] = None) -> str:
    with SessionLocal() as db:
        tool_input = {"grid_id": grid_id}
        if as_of:
            tool_input["as_of"] = as_of
        res = default_registry.execute_tool("get_grid_features", tool_input, db=db)
        return _serialize_result(res)


@server.tool(
    name="predict_grid_anomaly",
    description="Run the trained XGBoost model to predict drop anomaly probability for a specific grid.",
)
def predict_grid_anomaly(grid_id: int, as_of: Optional[str] = None) -> str:
    with SessionLocal() as db:
        tool_input = {"grid_id": grid_id}
        if as_of:
            tool_input["as_of"] = as_of
        res = default_registry.execute_tool("get_anomaly_prediction", tool_input, db=db)
        return _serialize_result(res)


@server.tool(
    name="get_rule_alerts",
    description="Fetch recent rule-based network anomaly alerts with baseline comparison.",
)
def get_rule_alerts(limit: int = 20, as_of: Optional[str] = None) -> str:
    with SessionLocal() as db:
        tool_input = {"limit": limit}
        if as_of:
            tool_input["as_of"] = as_of
        res = default_registry.execute_tool("get_rule_alerts", tool_input, db=db)
        return _serialize_result(res)


@server.tool(
    name="get_rule_hotspots",
    description="Fetch grids producing the highest activity hotspot alerts exceeding baseline boundaries.",
)
def get_rule_hotspots(limit: int = 20, as_of: Optional[str] = None) -> str:
    with SessionLocal() as db:
        tool_input = {"limit": limit}
        if as_of:
            tool_input["as_of"] = as_of
        res = default_registry.execute_tool("get_rule_hotspots", tool_input, db=db)
        return _serialize_result(res)


@server.tool(
    name="get_pipeline_health",
    description="Check data pipeline and feature computation synchronization to detect ETL lags.",
)
def get_pipeline_health() -> str:
    with SessionLocal() as db:
        res = default_registry.execute_tool("get_pipeline_health", {}, db=db)
        return _serialize_result(res)


@server.tool(
    name="suggest_remediation_sop",
    description="Retrieve standardized NOC remediation SOP steps for a diagnosed incident type and severity.",
)
def suggest_remediation_sop(issue_type: str, severity: str) -> str:
    with SessionLocal() as db:
        res = default_registry.execute_tool(
            "suggest_remediation_sop",
            {"issue_type": issue_type, "severity": severity},
            db=db,
        )
        return _serialize_result(res)


# ==============================================================================
# 2. MCP RESOURCES (Live Read-Only State URIs)
# ==============================================================================

@server.resource(
    uri="network://summary/latest",
    name="Latest Network Summary",
    description="Live JSON snapshot of Milano network status and top metrics.",
    mime_type="application/json",
)
def resource_network_summary() -> str:
    with SessionLocal() as db:
        res = default_registry.execute_tool("get_network_summary", {}, db=db)
        return _serialize_result(res)


@server.resource(
    uri="network://alerts/recent",
    name="Recent Rule Alerts",
    description="Live snapshot of the 20 most recent rule-based anomaly alerts.",
    mime_type="application/json",
)
def resource_recent_alerts() -> str:
    with SessionLocal() as db:
        res = default_registry.execute_tool("get_rule_alerts", {"limit": 20}, db=db)
        return _serialize_result(res)


@server.resource(
    uri="network://pipeline/status",
    name="Data Pipeline Status",
    description="Ingestion timestamps and freshness health across warehouse tables.",
    mime_type="application/json",
)
def resource_pipeline_status() -> str:
    with SessionLocal() as db:
        res = default_registry.execute_tool("get_pipeline_health", {}, db=db)
        return _serialize_result(res)


# ==============================================================================
# 3. MCP PROMPTS (Pre-configured Triage Templates)
# ==============================================================================

@server.prompt(
    name="triage-grid",
    description="Standard prompt template for investigating an anomalous telecom grid cell.",
)
def prompt_triage_grid(grid_id: int) -> str:
    return (
        f"You are the Milano AI NOC Assistant. Conduct a structured investigation for Grid {grid_id}.\n"
        f"1. Query recent grid activity and rolling ML features using the tools provided.\n"
        f"2. Check the ML anomaly drop prediction and any matching rule alerts.\n"
        f"3. Verify pipeline freshness to rule out data ingestion lag.\n"
        f"4. Output your findings strictly structured as:\n\n"
        f"SEVERITY\nEVIDENCE\nINTERPRETATION\nNEXT CHECKS"
    )


@server.prompt(
    name="daily-network-review",
    description="Template for generating a shift-handover operational report.",
)
def prompt_daily_review() -> str:
    return (
        "You are the Milano AI NOC Assistant. Generate a comprehensive network review for the current shift.\n"
        "1. Check overall citywide network summary and top active grids.\n"
        "2. Review recent high-severity rule alerts and hotspots.\n"
        "3. Provide actionable recommendations for the incoming NOC operations team."
    )


# ==============================================================================
# 4. SERVER ENTRYPOINT
# ==============================================================================

if __name__ == "__main__":
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    print(f"Starting Milano NOC MCP Server on transport={transport}...", file=sys.stderr)
    server.run(transport=transport)
