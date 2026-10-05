from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from sqlalchemy.orm import Session


class BasePlugin(ABC):
    """
    Abstract base class for all Milano NOC AI Assistant plugins.
    Each plugin encapsulates domain-specific tools, schemas, prompt
    contributions, and execution logic.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique identifier for the plugin (e.g., 'traffic_analytics')."""
        pass

    @property
    @abstractmethod
    def display_name(self) -> str:
        """Human-readable display name (e.g., 'Traffic Analytics')."""
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        """Detailed description of the domain capabilities this plugin provides."""
        pass

    @property
    def version(self) -> str:
        """Plugin version."""
        return "1.0.0"

    @property
    def is_enabled(self) -> bool:
        """Flag indicating if this plugin is enabled by default."""
        return True

    @abstractmethod
    def get_tools(self) -> List[Dict[str, Any]]:
        """
        Return the list of Anthropic/Claude-compatible tool schema definitions
        provided by this plugin.
        """
        pass

    @abstractmethod
    def execute_tool(
        self,
        name: str,
        tool_input: Dict[str, Any],
        db: Session,
        context: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Execute a tool by name with the given inputs.
        Raises ValueError if tool name is not recognized.
        """
        pass

    def get_prompt_contribution(self) -> Optional[str]:
        """
        Optional domain-specific guidelines or rules to append to the
        LLM system prompt when this plugin is active.
        """
        return None
