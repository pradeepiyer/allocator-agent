"""HTTP interface setup for Allocator Agent."""

from agent_kit.api.http import AgentRegistry, create_server
from agent_kit.api.mcp import run_mcp_stdio
from agent_kit.config import get_config

from .agent import AllocatorAgent
from .models import AllocatorRequest, AllocatorResponse


def create_allocator_registry() -> AgentRegistry:
    """Register allocator agent for REST and MCP access."""
    registry = AgentRegistry()

    registry.register(
        name="allocator",
        agent_class=AllocatorAgent,
        description="AI-powered capital allocation agent for fundamental stock analysis. Ask 'Analyze AAPL', 'Find stocks like MSFT', or 'Screen for high ROIC companies'",
        request_model=AllocatorRequest,
        response_model=AllocatorResponse,
    )

    return registry


def create_allocator_server():
    """Create HTTP server with REST and MCP support."""
    config = get_config()
    registry = create_allocator_registry()
    return create_server(registry, config.interfaces.http, config.interfaces.session_ttl)


def run_allocator_stdio() -> None:
    """Run MCP stdio mode for Claude Desktop integration."""
    registry = create_allocator_registry()
    run_mcp_stdio(registry)
