from typing import Any, Dict, Optional, Tuple
from ai.hooks.base import BaseHook


class GuardrailsHook(BaseHook):
    """
    Validates query safety, enforces boundary constraints on grid IDs (1 to 10000),
    and prevents malformed requests before executing backend queries.
    """

    @property
    def name(self) -> str:
        return "guardrails"

    @property
    def priority(self) -> int:
        return 20

    def pre_run(
        self,
        message: str,
        context: Dict[str, Any],
    ) -> Tuple[str, Dict[str, Any]]:
        # Strip null bytes and excessive whitespace
        cleaned_message = message.replace("\x00", "").strip()

        # Maximum message length constraint to prevent memory exhaustion
        if len(cleaned_message) > 4000:
            cleaned_message = cleaned_message[:4000]

        return cleaned_message, context

    def pre_tool_call(
        self,
        tool_name: str,
        tool_input: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        # Validate grid_id boundaries if present in tool arguments
        if "grid_id" in tool_input:
            try:
                grid_id = int(tool_input["grid_id"])
                if grid_id < 1 or grid_id > 10000:
                    return (
                        False,
                        f"Grid ID {grid_id} is out of Milano municipal bounds (1 to 10000).",
                        tool_input,
                    )
                tool_input["grid_id"] = grid_id
            except (ValueError, TypeError):
                return False, "Invalid grid_id: must be an integer.", tool_input

        # Validate limit parameters
        if "limit" in tool_input:
            try:
                limit = int(tool_input["limit"])
                tool_input["limit"] = max(1, min(limit, 50))
            except (ValueError, TypeError):
                tool_input["limit"] = 10

        return True, None, tool_input
