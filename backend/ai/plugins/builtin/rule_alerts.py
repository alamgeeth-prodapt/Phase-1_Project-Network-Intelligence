from datetime import datetime
from typing import Any, Dict, List, Optional
from sqlalchemy.orm import Session

from ai.plugins.base import BasePlugin
from routes.rule import get_rule_alerts, get_rule_hotspots


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class RuleAlertsPlugin(BasePlugin):
    """
    Provides access to deterministic, rule-based threshold anomaly alerts,
    traffic surges, and activity hotspot violations across the Milano grid network.
    """

    @property
    def name(self) -> str:
        return "rule_alerts"

    @property
    def display_name(self) -> str:
        return "Rule Alerts & Hotspots"

    @property
    def description(self) -> str:
        return (
            "Monitors deterministic rule-based anomaly alerts, threshold breaches, "
            "and traffic hotspots across all spatial grids."
        )

    def get_tools(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": "get_rule_alerts",
                "description": (
                    "Get recent rule-based network anomaly alerts (e.g. drop alerts, spike alerts) "
                    "with baseline comparison and severity reasons."
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of alerts to retrieve (1 to 50).",
                            "minimum": 1,
                            "maximum": 50,
                        },
                        "as_of": {
                            "type": "string",
                            "description": "Optional ISO timestamp limit.",
                        },
                    },
                    "required": [],
                },
            },
            {
                "name": "get_rule_hotspots",
                "description": (
                    "Get grids producing the highest activity hotspot alerts where traffic "
                    "abnormally exceeds baseline boundaries."
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of hotspots to retrieve (1 to 50).",
                            "minimum": 1,
                            "maximum": 50,
                        },
                        "as_of": {
                            "type": "string",
                            "description": "Optional ISO timestamp limit.",
                        },
                    },
                    "required": [],
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
        current_user = context.get("current_user", "ai-agent") if context else "ai-agent"

        if name == "get_rule_alerts":
            return get_rule_alerts(
                limit=tool_input.get("limit", 20),
                as_of=_parse_datetime(tool_input.get("as_of")),
                db=db,
                current_user=current_user,
            )

        if name == "get_rule_hotspots":
            return get_rule_hotspots(
                limit=tool_input.get("limit", 20),
                as_of=_parse_datetime(tool_input.get("as_of")),
                db=db,
                current_user=current_user,
            )

        raise ValueError(f"RuleAlertsPlugin received unknown tool: {name}")

    def get_prompt_contribution(self) -> Optional[str]:
        return (
            "- Distinguish rule-based threshold breaches from ML statistical drop predictions.\n"
            "- Cross-reference hotspot alerts with grid baseline activity to confirm genuine anomalies."
        )
