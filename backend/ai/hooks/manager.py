import time
from typing import Any, Dict, List, Optional, Tuple

from ai.hooks.base import BaseHook
from ai.hooks.builtin import (
    TelemetryAuditHook,
    GuardrailsHook,
    NocFormatComplianceHook,
)


class HookManager:
    """
    Coordinates execution of lifecycle hooks in prioritized order.
    """

    def __init__(self):
        self._hooks: List[BaseHook] = []

    def register(self, hook: BaseHook) -> None:
        """Register a hook and sort by priority."""
        self._hooks.append(hook)
        self._hooks.sort(key=lambda h: h.priority)

    def unregister(self, hook_name: str) -> None:
        """Unregister a hook by name."""
        self._hooks = [h for h in self._hooks if h.name != hook_name]

    def run_pre_run(
        self,
        message: str,
        context: Dict[str, Any],
    ) -> Tuple[str, Dict[str, Any]]:
        """Run pre_run on all hooks in priority order."""
        current_msg = message
        for hook in self._hooks:
            current_msg, context = hook.pre_run(current_msg, context)
        return current_msg, context

    def run_pre_tool_call(
        self,
        tool_name: str,
        tool_input: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        """
        Run pre_tool_call across all hooks.
        Short-circuits if any hook denies the call.
        """
        current_input = tool_input
        for hook in self._hooks:
            allowed, reason, current_input = hook.pre_tool_call(
                tool_name, current_input, context
            )
            if not allowed:
                return False, reason, current_input
        return True, None, current_input

    def run_post_tool_call(
        self,
        tool_name: str,
        tool_input: Dict[str, Any],
        result: Any,
        duration_ms: float,
        context: Dict[str, Any],
    ) -> Any:
        """Run post_tool_call across all hooks."""
        current_result = result
        for hook in self._hooks:
            current_result = hook.post_tool_call(
                tool_name, tool_input, current_result, duration_ms, context
            )
        return current_result

    def run_post_run(
        self,
        response_text: str,
        context: Dict[str, Any],
    ) -> str:
        """Run post_run across all hooks."""
        current_resp = response_text
        for hook in self._hooks:
            current_resp = hook.post_run(current_resp, context)
        return current_resp

    def run_on_error(
        self,
        error: Exception,
        stage: str,
        context: Dict[str, Any],
    ) -> Optional[str]:
        """
        Run on_error on hooks. Returns first non-None fallback response or None.
        """
        for hook in self._hooks:
            fallback = hook.on_error(error, stage, context)
            if fallback is not None:
                return fallback
        return None


def create_default_hook_manager() -> HookManager:
    """Factory creating a hook manager configured with default enterprise hooks."""
    mgr = HookManager()
    mgr.register(TelemetryAuditHook())
    mgr.register(GuardrailsHook())
    mgr.register(NocFormatComplianceHook())
    return mgr


# Global default instance
default_hook_manager = create_default_hook_manager()
