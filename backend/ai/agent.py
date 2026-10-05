import json
import time
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session

from .claude import client, MODEL
from .plugins.registry import default_registry, PluginRegistry
from .hooks.manager import default_hook_manager, HookManager


BASE_SYSTEM_PROMPT = """
You are the Milano AI NOC Assistant.

You help network operators monitor, analyze, and troubleshoot the Milano
telecom network analytics platform across 10,000 spatial grids.

You have access to specialized modular plugins and tools that retrieve real
network activity, ML drop anomaly predictions, rule-based alerts, and pipeline health.

IMPORTANT OPERATIONAL RULES:

1. Never invent network measurements or grid numbers.
2. Use tools whenever the answer depends on Milano network data.
3. Base conclusions strictly on retrieved evidence.
4. Clearly distinguish evidence from interpretation.
5. If data is missing or out of range, explicitly state that it is unavailable.
6. Do not claim that a network is congested unless actual network capacity information is available.
7. Activity metrics represent relative activity counts/measurements, not automatically MB or bandwidth.
8. When investigating a specific grid, follow this triage flow:
   - grid activity & timeseries
   - grid ML features & data freshness
   - rule-based anomaly alerts & hotspots
   - ML anomaly prediction (XGBoost drop probability)
9. Keep answers concise, actionable, and useful to a 24/7 NOC operator.

For network investigations or anomaly diagnoses, structure your response as:

SEVERITY: [CRITICAL | HIGH | MEDIUM | LOW | NORMAL]
EVIDENCE:
- [Point-by-point evidence retrieved via tools]
INTERPRETATION:
- [Operational diagnosis of what the evidence means]
NEXT CHECKS:
- [Clear, actionable next steps or runbook recommendations]
"""


def compose_system_prompt(registry: PluginRegistry) -> str:
    """Combine base NOC rules with active plugin domain contributions."""
    prompt = BASE_SYSTEM_PROMPT.strip()
    plugin_guidelines = registry.get_prompt_contributions()
    if plugin_guidelines:
        prompt += f"\n\nACTIVE CAPABILITY GUIDELINES:\n{plugin_guidelines}"
    return prompt


def chat(
    user_message: str,
    db: Session,
    history: Optional[List[dict]] = None,
    enabled_plugins: Optional[List[str]] = None,
    context: Optional[Dict[str, Any]] = None,
    registry: Optional[PluginRegistry] = None,
    hook_mgr: Optional[HookManager] = None,
) -> Dict[str, Any]:
    """
    Execute a chat interaction through the enhanced NOC Agent pipeline:
    1. Pre-run hooks (input sanitization & guardrails)
    2. Dynamic system prompt generation based on enabled plugins
    3. LLM tool-calling loop governed by pre/post tool hooks
    4. Post-run hooks (NOC format verification & telemetry compilation)

    Returns a dictionary with:
    - 'response': The assistant's text response.
    - 'traces': List of tool execution traces with latency.
    - 'duration_ms': Total processing time in milliseconds.
    - 'active_plugins': List of active plugin names.
    - 'compliance_passed': Boolean indicating adherence to NOC schema.
    """
    if registry is None:
        registry = default_registry
    if hook_mgr is None:
        hook_mgr = default_hook_manager
    if context is None:
        context = {}

    # Adjust enabled plugins if specified
    if enabled_plugins is not None:
        for p in registry.list_plugins():
            registry.set_plugin_enabled(p["name"], p["name"] in enabled_plugins)

    context["active_plugins"] = [
        p["name"] for p in registry.list_plugins() if p["is_enabled"]
    ]

    # 1. Pre-run lifecycle hook
    user_message, context = hook_mgr.run_pre_run(user_message, context)

    # 2. Prepare message history
    messages = []
    if history:
        sanitized = []
        for msg in history:
            role = msg.get("role")
            content = msg.get("content")
            if role in ("user", "assistant") and content and isinstance(content, str):
                if sanitized and sanitized[-1]["role"] == role:
                    sanitized[-1]["content"] += f"\n\n{content}"
                else:
                    sanitized.append({"role": role, "content": content})

        while sanitized and sanitized[0]["role"] != "user":
            sanitized.pop(0)

        if len(sanitized) > 10:
            sanitized = sanitized[-10:]
            while sanitized and sanitized[0]["role"] != "user":
                sanitized.pop(0)

        messages.extend(sanitized)

    if messages and messages[-1]["role"] == "user":
        messages[-1]["content"] += f"\n\n{user_message}"
    else:
        messages.append({"role": "user", "content": user_message})

    # 3. Dynamic prompt and tool catalog from active plugins
    system_prompt = compose_system_prompt(registry)
    active_tools = registry.get_all_tools()

    final_response_text = ""

    try:
        # 4. Agent tool execution loop (up to 6 rounds)
        for _ in range(6):
            response = client.messages.create(
                model=MODEL,
                max_tokens=2000,
                system=system_prompt,
                tools=active_tools if active_tools else None,
                messages=messages,
            )

            messages.append({"role": "assistant", "content": response.content})

            # No tool use requested - LLM has final answer
            if response.stop_reason != "tool_use":
                text_parts = []
                for block in response.content:
                    if block.type == "text":
                        text_parts.append(block.text)
                final_response_text = "\n".join(text_parts)
                break

            # Execute requested tools with pre/post hooks
            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue

                tool_name = block.name
                tool_input = block.input or {}

                # Pre-tool lifecycle hook
                allowed, reject_reason, modified_input = hook_mgr.run_pre_tool_call(
                    tool_name=tool_name,
                    tool_input=tool_input,
                    context=context,
                )

                if not allowed:
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps({
                            "error": "Tool call blocked by policy guardrail",
                            "reason": reject_reason,
                        }),
                        "is_error": True,
                    })
                    continue

                start_t = time.perf_counter()
                try:
                    raw_result = registry.execute_tool(
                        name=tool_name,
                        tool_input=modified_input,
                        db=db,
                        context=context,
                    )
                    duration_ms = (time.perf_counter() - start_t) * 1000

                    # Post-tool lifecycle hook
                    processed_result = hook_mgr.run_post_tool_call(
                        tool_name=tool_name,
                        tool_input=modified_input,
                        result=raw_result,
                        duration_ms=duration_ms,
                        context=context,
                    )

                    # Serialize result
                    if hasattr(processed_result, "model_dump"):
                        processed_result = processed_result.model_dump()
                    elif hasattr(processed_result, "dict"):
                        processed_result = processed_result.dict()

                    result_json = json.dumps(processed_result, default=str)

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result_json,
                    })

                except Exception as exc:
                    duration_ms = (time.perf_counter() - start_t) * 1000
                    hook_mgr.run_on_error(exc, stage=f"tool:{tool_name}", context=context)

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": f"Tool execution failed: {str(exc)}",
                        "is_error": True,
                    })

            messages.append({"role": "user", "content": tool_results})

        else:
            final_response_text = (
                "The investigation required too many tool calls. "
                "Please try a more specific question."
            )

    except Exception as exc:
        fallback = hook_mgr.run_on_error(exc, stage="agent_execution", context=context)
        final_response_text = (
            fallback
            if fallback
            else f"An operational error occurred during the investigation: {str(exc)}"
        )

    # 5. Post-run lifecycle hook
    final_response_text = hook_mgr.run_post_run(final_response_text, context)

    return {
        "response": final_response_text,
        "traces": context.get("traces", []),
        "duration_ms": context.get("total_duration_ms", 0.0),
        "active_plugins": context.get("active_plugins", []),
        "compliance_passed": context.get("compliance_passed", True),
    }