"""Allocator Agent - Capital allocation and stock analysis agent."""

import logging
from pathlib import Path

from agent_kit.agents.base_agent import BaseAgent
from agent_kit.clients.openai_client import OpenAIClient
from agent_kit.config.config import get_config
from agent_kit.prompts.loader import PromptLoader

from allocator.models import SimilarStocksResult, StockAnalysis
from allocator.tools import execute_tool, get_tool_definitions

logger = logging.getLogger(__name__)


class AllocatorAgent(BaseAgent):
    """AI agent for capital allocation analysis and stock selection."""

    def __init__(self, openai_client: OpenAIClient):
        """Initialize Allocator Agent."""
        super().__init__(openai_client)

        # Register custom prompt directory for allocator-agent
        prompts_root = Path(__file__).parent.parent
        self.prompt_loader = PromptLoader(search_paths=[prompts_root])

        logger.info("AllocatorAgent initialized")

    async def analyze_stock(self, symbol: str, continue_conversation: bool = False) -> StockAnalysis:
        """Perform comprehensive analysis of a stock.

        Uses the orchestrator prompt with full investment principles to provide
        deep analysis covering management, capital allocation, financials,
        competitive position, valuation, and technical factors.

        Args:
            symbol: Stock ticker symbol to analyze
            continue_conversation: If True, continues previous conversation using last_response_id

        Returns:
            Structured analysis with investment recommendation
        """
        logger.info(f"Analyzing stock {symbol} (continue={continue_conversation})")

        # Render analyzer prompt with full investment principles
        prompts = self.render_prompt("allocator", "analyzer")

        # Get max iterations from agent config
        max_iterations = self.get_agent_config("max_iterations", get_config().agents.max_iterations)

        # Execute analysis with tools
        response = await self.execute_tool_conversation(
            instructions=prompts["instructions"],
            initial_input=[{"role": "user", "content": f"Analyze {symbol} as a potential investment. Provide a comprehensive analysis covering all investment principles including management quality, capital allocation, financial metrics, competitive position, valuation, and technical setup. End with a clear investment recommendation."}],
            tools=get_tool_definitions(),
            tool_executor=execute_tool,
            max_iterations=max_iterations,
            previous_response_id=self.last_response_id if continue_conversation else None,
            response_format=StockAnalysis,
        )

        return response

    async def find_similar_stocks(self, symbol: str, continue_conversation: bool = False) -> SimilarStocksResult:
        """Find stocks with similar characteristics to the given symbol.

        Uses similarity prompt to identify comparable companies based on
        business model, financial profile, management quality, and other dimensions.

        Args:
            symbol: Reference stock ticker symbol
            continue_conversation: If True, continues previous conversation

        Returns:
            Structured result with ranked list of similar stocks
        """
        logger.info(f"Finding similar stocks to {symbol} (continue={continue_conversation})")

        # Render similarity prompt
        prompts = self.render_prompt("allocator", "similarity")

        # Get max iterations
        max_iterations = self.get_agent_config("max_iterations", get_config().agents.max_iterations)

        # Execute similarity search
        response = await self.execute_tool_conversation(
            instructions=prompts["instructions"],
            initial_input=[{"role": "user", "content": f"Find stocks that are similar to {symbol}. IMPORTANT: First use the 'find_similar_companies' tool to programmatically discover candidate companies in the same sector/industry with similar characteristics. Then analyze the top candidates across multiple dimensions including business model, financial profile, management quality, and competitive position. Rank the results by similarity and explain what makes each one similar or different."}],
            tools=get_tool_definitions(),
            tool_executor=execute_tool,
            max_iterations=max_iterations,
            previous_response_id=self.last_response_id if continue_conversation else None,
            response_format=SimilarStocksResult,
        )

        return response

    async def process(self, query: str, continue_conversation: bool = False) -> str:
        """Process a general query with investment analysis context.

        This is the general-purpose method that can handle any investment-related
        question using the analyzer prompt.

        Args:
            query: User query or request
            continue_conversation: If True, continues previous conversation

        Returns:
            Response to the query
        """
        logger.info(f"Processing query (continue={continue_conversation}): {query[:100]}")

        # Render analyzer prompt
        prompts = self.render_prompt("allocator", "analyzer")

        # Get max iterations
        max_iterations = self.get_agent_config("max_iterations", get_config().agents.max_iterations)

        # Execute conversation
        response = await self.execute_tool_conversation(
            instructions=prompts["instructions"],
            initial_input=[{"role": "user", "content": query}],
            tools=get_tool_definitions(),
            tool_executor=execute_tool,
            max_iterations=max_iterations,
            previous_response_id=self.last_response_id if continue_conversation else None,
            response_format=None,
        )

        # Extract text from response
        if hasattr(response, "output_text") and response.output_text:
            logger.info(f"Successfully extracted output_text: {response.output_text[:100]}")
            return response.output_text
        elif (
            hasattr(response, "output")
            and response.output
            and (text_items := [item for item in response.output if hasattr(item, "type") and item.type == "text"])
        ):
            if text_items and hasattr(text_items[0], "text"):
                logger.info(f"Successfully extracted text from output: {text_items[0].text[:100]}")
                return text_items[0].text

        # Fallback
        logger.warning("Could not extract text from response, using fallback")
        return "I'm having trouble processing that request. Please try again."
