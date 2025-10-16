"""Main entry point for Allocator Agent."""

import asyncio
import logging
import sys

from agent_kit.api.console.server import run_console
from agents.allocator.console import AllocatorCommands


def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Set specific loggers to appropriate levels
    logging.getLogger("agent_kit").setLevel(logging.INFO)
    logging.getLogger("allocator").setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)


def main():
    """Main entry point."""
    setup_logging()

    # Run console with Allocator commands
    asyncio.run(run_console(AllocatorCommands))


if __name__ == "__main__":
    main()
