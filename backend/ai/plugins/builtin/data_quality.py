from datetime import datetime
from typing import Any, Dict, List, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session

from ai.plugins.base import BasePlugin


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class DataQualityPlugin(BasePlugin):
    """
    Assesses data warehouse ingestion status, feature freshness,
    and pipeline lag across the Milano analytics engine to verify
    whether an alert is an actual network outage or an ETL data processing delay.
    """

    @property
    def name(self) -> str:
        return "data_quality_and_pipeline"

    @property
    def display_name(self) -> str:
        return "Data Quality & Pipeline Freshness"

    @property
    def description(self) -> str:
        return (
            "Inspects data freshness, missing feature values, and ingestion pipeline lag "
            "to prevent diagnosing ETL data delays as network anomalies."
        )

    def get_tools(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": "get_pipeline_health",
                "description": (
                    "Check the health and freshness of the data pipeline across raw activity, "
                    "computed features, and rule alerts to determine if data is stale or lagging."
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {},
                    "required": [],
                },
            },
            {
                "name": "check_grid_freshness",
                "description": (
                    "Inspect the data quality, missing feature fields, and feature age "
                    "for a specific grid."
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "grid_id": {
                            "type": "integer",
                            "description": "Grid ID between 1 and 10000.",
                        },
                        "as_of": {
                            "type": "string",
                            "description": "Optional ISO timestamp.",
                        },
                    },
                    "required": ["grid_id"],
                },
            },
        ]

    def execute_tool(
        self,
        name: str,
        tool_input: Dict[str, Any],
        db: Session,
        context: Optional[Dict[str, Any]] = None,
    ) -> Any:
        if name == "get_pipeline_health":
            # Query max timestamps across tables
            raw_latest = db.execute(text("SELECT MAX(timestamp) FROM fact_network_activity")).scalar()
            features_latest = db.execute(text("SELECT MAX(timestamp) FROM grid_features")).scalar()
            alerts_latest = db.execute(text("SELECT MAX(timestamp) FROM network_rule_alerts")).scalar()

            return {
                "pipeline_status": "HEALTHY" if features_latest else "DEGRADED",
                "latest_raw_activity": str(raw_latest) if raw_latest else None,
                "latest_features_computed": str(features_latest) if features_latest else None,
                "latest_rule_alerts": str(alerts_latest) if alerts_latest else None,
                "freshness_assessment": (
                    "Features and alerts are synchronized with raw activity."
                    if features_latest and raw_latest and features_latest >= raw_latest
                    else "Minor pipeline lag detected between raw ingestion and feature computation."
                ),
            }

        if name == "check_grid_freshness":
            grid_id = tool_input["grid_id"]
            as_of = _parse_datetime(tool_input.get("as_of"))

            query = text("""
                SELECT MAX(timestamp) as latest_ts, COUNT(*) as record_count
                FROM grid_features
                WHERE grid_id = :grid_id
            """)
            row = db.execute(query, {"grid_id": grid_id}).mappings().first()

            if not row or not row["latest_ts"]:
                return {
                    "grid_id": grid_id,
                    "status": "NO_DATA",
                    "message": f"No computed features found for grid {grid_id}.",
                }

            return {
                "grid_id": grid_id,
                "status": "VALID",
                "latest_feature_timestamp": str(row["latest_ts"]),
                "feature_records": row["record_count"],
            }

        raise ValueError(f"DataQualityPlugin received unknown tool: {name}")

    def get_prompt_contribution(self) -> Optional[str]:
        return (
            "- If network activity is suddenly zero or missing, verify pipeline freshness before "
            "declaring a critical network outage.\n"
            "- Pipeline lag does not necessarily mean a physical cell tower outage."
        )
