"""Main entry point for Allocator Agent."""

import asyncio
import sys

from agent_kit.api.console import run_console
from agent_kit.config import setup_configuration
from agent_kit.utils import set_app_name

from .console import AllocatorCommands


async def main_async():
    """Run allocator with interface selection based on config."""
    # Set custom user directory
    set_app_name("allocator-agent")

    # Load configuration to determine enabled interfaces
    config = await setup_configuration()

    # Start appropriate interface
    if config.interfaces.http.enabled:
        # HTTP mode (REST and/or MCP over HTTP)
        import uvicorn

        from .http import create_allocator_server

        app = create_allocator_server()
        uvicorn.run(app, host=config.interfaces.http.host, port=config.interfaces.http.port, log_level="info")
    elif config.interfaces.console.enabled:
        # Console mode
        await run_console(AllocatorCommands)
    elif config.interfaces.mcp_stdio.enabled:
        # MCP stdio mode for Claude Desktop
        from .http import run_allocator_stdio

        run_allocator_stdio()
    else:
        print("Error: No interfaces enabled. Check your configuration.")
        sys.exit(1)


def main():
    """Entry point wrapper."""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\nGoodbye!")
        sys.exit(0)


if __name__ == "__main__":
    main()
