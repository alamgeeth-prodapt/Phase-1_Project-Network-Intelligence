import time
from typing import Any, Dict, Optional, Tuple
from ai.hooks.base import BaseHook


class TelemetryAuditHook(BaseHook):
    """
    Measures tool execution latency, maintains a structured execution trace,
    and logs timestamps for auditability and frontend display.
    """

    @property
    def name(self) -> str:
        return "telemetry_audit"

    @property
    def priority(self) -> int:
        return 10  # Run early to measure accurate timing

    def pre_run(
        self,
        message: str,
        context: Dict[str, Any],
    ) -> Tuple[str, Dict[str, Any]]:
        context["run_start_time"] = time.perf_counter()
        if "traces" not in context:
            context["traces"] = []
        return message, context

    def pre_tool_call(
        self,
        tool_name: str,
        tool_input: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        context[f"_tool_start_{tool_name}"] = time.perf_counter()
        return True, None, tool_input

    def post_tool_call(
        self,
        tool_name: str,
        tool_input: Dict[str, Any],
        result: Any,
        duration_ms: float,
        context: Dict[str, Any],
    ) -> Any:
        traces = context.setdefault("traces", [])
        traces.append({
            "tool_name": tool_name,
            "duration_ms": round(duration_ms, 2),
            "status": "success",
            "input_summary": {k: v for k, v in tool_input.items() if k != "password"},
        })
        return result

    def post_run(
        self,
        response_text: str,
        context: Dict[str, Any],
    ) -> str:
        start_time = context.get("run_start_time")
        if start_time:
            context["total_duration_ms"] = round((time.perf_counter() - start_time) * 1000, 2)
        return response_text

    def on_error(
        self,
        error: Exception,
        stage: str,
        context: Dict[str, Any],
    ) -> Optional[str]:
        traces = context.setdefault("traces", [])
        traces.append({
            "stage": stage,
            "status": "error",
            "error_type": type(error).__name__,
            "error_message": str(error),
        })
        return None
