from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from routes.network import (
    get_network_summary,
    get_top_grids,
    get_grid_activity,
    get_grid_features,
)

from routes.rule import (
    get_rule_alerts,
    get_rule_hotspots,
)

from routes.ml import (
    predict_grid_anomaly,
)


TOOLS = [
    {
        "name": "get_network_summary",
        "description": (
            "Get the overall Milano network activity summary. "
            "Use this when the user asks how the network is "
            "performing overall."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "summary_date": {
                    "type": "string",
                    "description": (
                        "Optional date in YYYY-MM-DD format."
                    ),
                }
            },
            "required": [],
        },
    },

    {
        "name": "get_top_grids",
        "description": (
            "Get the grids with the highest network activity "
            "for a specific date."
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
                    "description": "Number of grids to return.",
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
            "Get activity history for a specific grid. "
            "Can optionally filter by date and hour."
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
                    "description": (
                        "Optional timestamp defining the end "
                        "of the requested activity window."
                    ),
                },
            },
            "required": ["grid_id"],
        },
    },

    {
        "name": "get_grid_features",
        "description": (
            "Get ML features and data-quality information "
            "for a specific grid."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "grid_id": {
                    "type": "integer",
                    "description": "Grid ID.",
                },
                "as_of": {
                    "type": "string",
                    "description": "Optional feature timestamp.",
                },
            },
            "required": ["grid_id"],
        },
    },

    {
        "name": "get_rule_alerts",
        "description": (
            "Get recent rule-based network anomaly alerts."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 20,
                },
                "as_of": {
                    "type": "string",
                },
            },
            "required": [],
        },
    },

    {
        "name": "get_rule_hotspots",
        "description": (
            "Get grids producing the highest activity "
            "hotspot alerts."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 20,
                },
                "as_of": {
                    "type": "string",
                },
            },
            "required": [],
        },
    },

    {
        "name": "get_anomaly_prediction",
        "description": (
            "Run the trained ML anomaly detector for a grid "
            "and return the anomaly probability."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "grid_id": {
                    "type": "integer",
                    "description": "Grid ID.",
                },
                "as_of": {
                    "type": "string",
                    "description": "Optional feature timestamp.",
                },
            },
            "required": ["grid_id"],
        },
    },
]



def parse_datetime(value: Optional[str]):
    if not value:
        return None

    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )



def execute_tool(
    name: str,
    tool_input: dict,
    db: Session,
):
    """
    Execute one of the AI tools.

    These functions reuse the existing Milano backend
    route logic instead of duplicating the analytics.
    """

    if name == "get_network_summary":

        summary_date = tool_input.get("summary_date")

        if summary_date:
            from datetime import date

            summary_date = date.fromisoformat(
                summary_date
            )

        result = get_network_summary(
            summary_date=summary_date,
            db=db,
            current_user="ai-agent",
        )

        return result


    if name == "get_top_grids":

        from datetime import date

        grid_date = tool_input.get("date")

        if grid_date:
            grid_date = date.fromisoformat(grid_date)

        return get_top_grids(
            date=grid_date,
            limit=tool_input.get("limit", 10),
            db=db,
            current_user="ai-agent",
        )


    if name == "get_grid_activity":

        return get_grid_activity(
            grid_id=tool_input["grid_id"],
            date=tool_input.get("date"),
            hour=tool_input.get("hour"),
            as_of=parse_datetime(
                tool_input.get("as_of")
            ),
            db=db,
            current_user="ai-agent",
        )


    if name == "get_grid_features":

        return get_grid_features(
            grid_id=tool_input["grid_id"],
            as_of=parse_datetime(
                tool_input.get("as_of")
            ),
            db=db,
            current_user="ai-agent",
        )


    if name == "get_rule_alerts":

        return get_rule_alerts(
            limit=tool_input.get("limit", 20),
            as_of=parse_datetime(
                tool_input.get("as_of")
            ),
            db=db,
            current_user="ai-agent",
        )


    if name == "get_rule_hotspots":

        return get_rule_hotspots(
            limit=tool_input.get("limit", 20),
            as_of=parse_datetime(
                tool_input.get("as_of")
            ),
            db=db,
            current_user="ai-agent",
        )


    if name == "get_anomaly_prediction":

        return predict_grid_anomaly(
            grid_id=tool_input["grid_id"],
            as_of=parse_datetime(
                tool_input.get("as_of")
            ),
            db=db,
            current_user="ai-agent",
        )


    raise ValueError(
        f"Unknown AI tool: {name}"
    )