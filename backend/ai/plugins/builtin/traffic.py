from datetime import datetime, date
from typing import Any, Dict, List, Optional
from sqlalchemy.orm import Session

from ai.plugins.base import BasePlugin
from routes.network import (
    get_network_summary,
    get_top_grids,
    get_grid_activity,
)


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class TrafficPlugin(BasePlugin):
    """
    Handles city-wide network traffic summaries, peak hour analysis,
    top active spatial grids, and temporal grid activity timeseries.
    """

    @property
    def name(self) -> str:
        return "traffic_analytics"

    @property
    def display_name(self) -> str:
        return "Traffic Analytics"

    @property
    def description(self) -> str:
        return (
            "Provides citywide traffic summaries, top active grid rankings, "
            "and grid-level activity timeseries across SMS, voice, and internet."
        )

    def get_tools(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": "get_network_summary",
                "description": (
                    "Get the overall Milano network activity summary including active grids, "
                    "peak hour, and top grid. Use this when the user asks how the network is "
                    "performing overall."
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "summary_date": {
                            "type": "string",
                            "description": "Optional date in YYYY-MM-DD format.",
                        }
                    },
                    "required": [],
                },
            },
            {
                "name": "get_top_grids",
                "description": (
                    "Get the grids with the highest network activity for a specific date."
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "date": {
                            "type": "string",
                            "description": "Optional date YYYY-MM-DD.",
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Number of grids to return (1 to 20).",
                            "minimum": 1,
                            "maximum": 20,
                        },
                    },
                    "required": [],
                },
            },
            {
                "name": "get_grid_activity",
                "description": (
                    "Get activity history for a specific grid across internet, voice, and SMS. "
                    "Can optionally filter by date, hour, or as_of timestamp."
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "grid_id": {
                            "type": "integer",
                            "description": "Grid ID between 1 and 10000.",
                        },
                        "date": {
                            "type": "string",
                            "description": "Optional date YYYY-MM-DD.",
                        },
                        "hour": {
                            "type": "integer",
                            "description": "Optional hour from 0 to 23.",
                            "minimum": 0,
                            "maximum": 23,
                        },
                        "as_of": {
                            "type": "string",
                            "description": "Optional ISO timestamp defining end of requested activity window.",
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
        current_user = context.get("current_user", "ai-agent") if context else "ai-agent"

        if name == "get_network_summary":
            summary_date = tool_input.get("summary_date")
            parsed_date = date.fromisoformat(summary_date) if summary_date else None
            return get_network_summary(
                summary_date=parsed_date,
                db=db,
                current_user=current_user,
            )

        if name == "get_top_grids":
            grid_date = tool_input.get("date")
            parsed_date = date.fromisoformat(grid_date) if grid_date else None
            return get_top_grids(
                date=parsed_date,
                limit=tool_input.get("limit", 10),
                db=db,
                current_user=current_user,
            )

        if name == "get_grid_activity":
            return get_grid_activity(
                grid_id=tool_input["grid_id"],
                date=tool_input.get("date"),
                hour=tool_input.get("hour"),
                as_of=_parse_datetime(tool_input.get("as_of")),
                db=db,
                current_user=current_user,
            )

        raise ValueError(f"TrafficPlugin received unknown tool: {name}")

    def get_prompt_contribution(self) -> Optional[str]:
        return (
            "- Traffic metrics represent activity counts, not automatically megabytes or bandwidth.\n"
            "- Always verify date ranges when interpreting citywide network summaries."
        )
