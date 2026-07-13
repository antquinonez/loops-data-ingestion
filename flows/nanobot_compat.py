"""
Compatibility module for nanobot ToolResult.
ToolResult was added in newer versions of nanobot-ai.
This provides a fallback implementation for older versions.
"""

try:
    from nanobot.agent.tools.base import ToolResult
    _TOOL_RESULT_AVAILABLE = True
except ImportError:
    _TOOL_RESULT_AVAILABLE = False
    
    class ToolResult(str):
        """String-compatible tool output with structured status."""
        
        is_error: bool
        
        def __new__(cls, content: str, *, is_error: bool = False) -> "ToolResult":
            obj = str.__new__(cls, content)
            obj.is_error = is_error
            return obj
        
        @classmethod
        def error(cls, content: str) -> "ToolResult":
            return cls(content, is_error=True)
