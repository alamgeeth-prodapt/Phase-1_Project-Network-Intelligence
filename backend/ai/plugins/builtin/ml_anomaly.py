from datetime import datetime
from typing import Any, Dict, List, Optional
from sqlalchemy.orm import Session

from ai.plugins.base import BasePlugin
from routes.ml import predict_grid_anomaly
from routes.network import get_grid_features


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class MLAnomalyPlugin(BasePlugin):
    """
    Integrates trained Machine Learning models (XGBoost drop detector)
    and feature store queries to evaluate network anomaly probabilities.
    """

    @property
    def name(self) -> str:
        return "ml_anomaly_detection"

    @property
    def display_name(self) -> str:
        return "ML Anomaly Detector"

    @property
    def description(self) -> str:
        return (
            "Provides ML-based activity drop anomaly predictions using trained XGBoost "
            "models and retrieves rolling feature distributions for specific grids."
        )

    def get_tools(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": "get_grid_features",
                "description": (
                    "Get ML features and data-quality information for a specific grid "
                    "(e.g., avg_activity, activity_growth, peak_ratio, variability, internet_share)."
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
                            "description": "Optional ISO timestamp for feature evaluation point.",
                        },
                    },
                    "required": ["grid_id"],
                },
            },
            {
                "name": "get_anomaly_prediction",
                "description": (
                    "Run the trained XGBoost ML anomaly detector for a grid and return "
                    "the anomaly drop probability, optimal threshold, and binary prediction."
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
                            "description": "Optional ISO timestamp for prediction evaluation point.",
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

        if name == "get_grid_features":
            return get_grid_features(
                grid_id=tool_input["grid_id"],
                as_of=_parse_datetime(tool_input.get("as_of")),
                db=db,
                current_user=current_user,
            )

        if name == "get_anomaly_prediction":
            return predict_grid_anomaly(
                grid_id=tool_input["grid_id"],
                as_of=_parse_datetime(tool_input.get("as_of")),
                db=db,
                current_user=current_user,
            )

        raise ValueError(f"MLAnomalyPlugin received unknown tool: {name}")

    def get_prompt_contribution(self) -> Optional[str]:
        return (
            "- Anomaly predictions score the likelihood of an abnormal drop in network activity.\n"
            "- Check drop_probability against optimal_threshold to state high/medium/low severity.\n"
            "- When investigating a specific grid, combine ML features with recent grid activity."
        )
