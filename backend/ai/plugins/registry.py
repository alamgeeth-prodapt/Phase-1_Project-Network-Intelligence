from typing import Any, Dict, List, Optional
from sqlalchemy.orm import Session

from ai.plugins.base import BasePlugin
from ai.plugins.builtin import (
    TrafficPlugin,
    MLAnomalyPlugin,
    RuleAlertsPlugin,
    DataQualityPlugin,
    RemediationPlugin,
)


class PluginRegistry:
    """
    Central registry for managing NOC Assistant plugins.
    Handles dynamic registration, tool lookup routing, and prompt synthesis.
    """

    def __init__(self):
        self._plugins: Dict[str, BasePlugin] = {}
        self._tool_to_plugin_map: Dict[str, str] = {}
        self._enabled_plugins: set[str] = set()

    def register(self, plugin: BasePlugin, enabled: bool = True) -> None:
        """Register a new plugin instance."""
        self._plugins[plugin.name] = plugin
        if enabled:
            self._enabled_plugins.add(plugin.name)

        # Map each tool to this plugin
        for tool in plugin.get_tools():
            tool_name = tool["name"]
            self._tool_to_plugin_map[tool_name] = plugin.name

    def unregister(self, plugin_name: str) -> None:
        """Unregister a plugin by name."""
        if plugin_name in self._plugins:
            plugin = self._plugins.pop(plugin_name)
            self._enabled_plugins.discard(plugin_name)
            # Remove tool mappings
            for tool in plugin.get_tools():
                self._tool_to_plugin_map.pop(tool["name"], None)

    def set_plugin_enabled(self, plugin_name: str, enabled: bool) -> None:
        """Enable or disable a registered plugin."""
        if plugin_name not in self._plugins:
            raise KeyError(f"Plugin '{plugin_name}' is not registered.")
        if enabled:
            self._enabled_plugins.add(plugin_name)
        else:
            self._enabled_plugins.discard(plugin_name)

    def get_plugin_for_tool(self, tool_name: str) -> Optional[BasePlugin]:
        """Find the plugin responsible for a tool."""
        plugin_name = self._tool_to_plugin_map.get(tool_name)
        if plugin_name and plugin_name in self._enabled_plugins:
            return self._plugins.get(plugin_name)
        return None

    def get_all_tools(self) -> List[Dict[str, Any]]:
        """
        Aggregate all tools from currently enabled plugins in Anthropic format.
        """
        tools: List[Dict[str, Any]] = []
        for name in self._enabled_plugins:
            plugin = self._plugins.get(name)
            if plugin:
                tools.extend(plugin.get_tools())
        return tools

    def execute_tool(
        self,
        name: str,
        tool_input: Dict[str, Any],
        db: Session,
        context: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Route tool execution to the appropriate enabled plugin.
        """
        plugin = self.get_plugin_for_tool(name)
        if not plugin:
            raise ValueError(
                f"Tool '{name}' is not available or its plugin is disabled."
            )

        return plugin.execute_tool(
            name=name,
            tool_input=tool_input,
            db=db,
            context=context,
        )

    def get_prompt_contributions(self) -> str:
        """
        Collect and format system prompt additions from all active plugins.
        """
        lines = []
        for name in sorted(self._enabled_plugins):
            plugin = self._plugins.get(name)
            if plugin:
                contrib = plugin.get_prompt_contribution()
                if contrib:
                    lines.append(f"### {plugin.display_name} Guidelines:\n{contrib}")
        return "\n\n".join(lines)

    def list_plugins(self) -> List[Dict[str, Any]]:
        """
        List all registered plugins with their status and descriptions.
        """
        result = []
        for name, plugin in self._plugins.items():
            result.append({
                "name": plugin.name,
                "display_name": plugin.display_name,
                "description": plugin.description,
                "version": plugin.version,
                "is_enabled": name in self._enabled_plugins,
                "tools_count": len(plugin.get_tools()),
            })
        return result


def create_default_registry() -> PluginRegistry:
    """Factory creating a registry loaded with all standard Milano NOC plugins."""
    registry = PluginRegistry()
    registry.register(TrafficPlugin())
    registry.register(MLAnomalyPlugin())
    registry.register(RuleAlertsPlugin())
    registry.register(DataQualityPlugin())
    registry.register(RemediationPlugin())
    return registry


# Global default instance
default_registry = create_default_registry()
