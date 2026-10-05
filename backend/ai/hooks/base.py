from abc import ABC
from typing import Any, Dict, Optional, Tuple


class BaseHook(ABC):
    """
    Abstract base class for all NOC Agent lifecycle hooks.
    Subclasses can intercept execution at pre_run, pre_tool_call,
    post_tool_call, post_run, and on_error.
    """

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def priority(self) -> int:
        """Lower numbers run earlier. Default is 100."""
        return 100

    def pre_run(
        self,
        message: str,
        context: Dict[str, Any],
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Runs before the LLM is invoked with user message.
        Can inspect, validate, sanitize message, or inject context.
        """
        return message, context

    def pre_tool_call(
        self,
        tool_name: str,
        tool_input: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        """
        Runs immediately before a tool is executed.
        Returns:
            (is_allowed: bool, rejection_reason: Optional[str], modified_input: dict)
        """
        return True, None, tool_input

    def post_tool_call(
        self,
        tool_name: str,
        tool_input: Dict[str, Any],
        result: Any,
        duration_ms: float,
        context: Dict[str, Any],
    ) -> Any:
        """
        Runs immediately after a tool finishes execution.
        Can sanitize results, record metrics, or assert data bounds.
        """
        return result

    def post_run(
        self,
        response_text: str,
        context: Dict[str, Any],
    ) -> str:
        """
        Runs after LLM produces final response.
        Can validate format, check compliance, or append audit metadata.
        """
        return response_text

    def on_error(
        self,
        error: Exception,
        stage: str,
        context: Dict[str, Any],
    ) -> Optional[str]:
        """
        Runs if an error occurs at any stage.
        Can return an alternative user-friendly response or None to propagate.
        """
        return None
