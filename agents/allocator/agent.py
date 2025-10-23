"""Allocator Agent - Capital allocation and stock analysis agent."""

import logging
from pathlib import Path

from agent_kit.agents.base_agent import BaseAgent
from agent_kit.api.progress import ProgressHandler
from agent_kit.clients.openai_client import OpenAIClient
from agent_kit.config.config import get_config
from agent_kit.prompts.loader import PromptLoader

from agents.allocator.models import AllocatorReport, ScreeningResult, SimilarStocksResult, StockAnalysis
from agents.allocator.tools import execute_tool, get_tool_definitions

logger = logging.getLogger(__name__)


class AllocatorAgent(BaseAgent):
    """AI agent for capital allocation analysis and stock selection."""

    def __init__(self, openai_client: OpenAIClient, progress_handler: ProgressHandler):
        """Initialize Allocator Agent."""
        super().__init__(openai_client, progress_handler)

        # Track intent for session continuity
        self.current_intent: str | None = None

        # Register custom prompt directory for allocator agent
        # prompts_root now points to agents/ directory which contains agents/allocator/prompts/
        prompts_root = Path(__file__).parent.parent
        self.prompt_loader = PromptLoader(search_paths=[prompts_root])

        logger.info("AllocatorAgent initialized")

    async def _analyze_stock(self, symbol: str, continue_conversation: bool = False) -> StockAnalysis:
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

        # Build user query
        user_query = f"Analyze {symbol} as a potential investment. Provide a comprehensive analysis covering all investment principles including management quality, capital allocation, financial metrics, competitive position, valuation, and technical setup. End with a clear investment recommendation."

        # Render analyzer prompt with query parameter
        prompts = self.render_prompt("allocator", "analyzer", query=user_query)

        # Get max iterations from agent config
        config = get_config()
        agent_config = config.agent_configs.get(self.agent_type, {})
        max_iterations = agent_config.get("max_iterations", config.agents.max_iterations)

        # Execute analysis with tools
        response = await self.execute_tool_conversation(
            instructions=prompts["instructions"],
            initial_input=[{"role": "user", "content": prompts["user"]}],
            tools=get_tool_definitions(),
            tool_executor=execute_tool,
            max_iterations=max_iterations,
            previous_response_id=self.last_response_id if continue_conversation else None,
            response_format=StockAnalysis,
        )

        return response

    async def _find_similar_stocks(self, symbol: str, continue_conversation: bool = False) -> SimilarStocksResult:
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

        # Build user query
        user_query = f"Find stocks that are similar to {symbol}. IMPORTANT: First use the 'find_similar_companies' tool to programmatically discover candidate companies in the same sector/industry with similar characteristics. Then analyze the top candidates across multiple dimensions including business model, financial profile, management quality, and competitive position. Rank the results by similarity and explain what makes each one similar or different."

        # Render similarity prompt with query parameter
        prompts = self.render_prompt("allocator", "similarity", query=user_query)

        # Get max iterations from agent config
        config = get_config()
        agent_config = config.agent_configs.get(self.agent_type, {})
        max_iterations = agent_config.get("max_iterations", config.agents.max_iterations)

        # Execute similarity search
        response = await self.execute_tool_conversation(
            instructions=prompts["instructions"],
            initial_input=[{"role": "user", "content": prompts["user"]}],
            tools=get_tool_definitions(),
            tool_executor=execute_tool,
            max_iterations=max_iterations,
            previous_response_id=self.last_response_id if continue_conversation else None,
            response_format=SimilarStocksResult,
        )

        return response

    async def _generate_allocator_report(self, symbol: str, continue_conversation: bool = False) -> AllocatorReport:
        """Generate comprehensive allocator report including stock analysis and similar stocks.

        This is the unified analysis method that performs both comprehensive stock
        analysis and finds similar companies, combining everything into one report.

        Args:
            symbol: Stock ticker symbol to analyze
            continue_conversation: If True, continues previous conversation

        Returns:
            Unified report with analysis and similar stocks
        """
        logger.info(f"Generating allocator report for {symbol} (continue={continue_conversation})")

        # First, perform comprehensive stock analysis
        analysis = await self._analyze_stock(symbol, continue_conversation=continue_conversation)

        # Then, find similar stocks (continue the conversation)
        similar_result = await self._find_similar_stocks(symbol, continue_conversation=True)

        # Combine into unified report
        report = AllocatorReport(
            symbol=symbol,
            analysis=analysis,
            similar_stocks=similar_result.similar_stocks,
            sources=list(set(analysis.sources + similar_result.sources)),  # Deduplicate sources
        )

        logger.info(f"Generated allocator report for {symbol} with {len(report.similar_stocks)} similar stocks")
        return report

    async def _screen_opportunities(
        self, criteria: str | None = None, limit: int = 20, continue_conversation: bool = False
    ) -> ScreeningResult:
        """Screen the market database for high-quality investment opportunities.

        Uses the screener prompt to apply investment principles and identify stocks
        that meet quality criteria. Returns ranked list of opportunities with scores.

        Args:
            criteria: Optional custom screening criteria from user (e.g., "tech stocks with high ROIC")
            limit: Maximum number of results to return (default 20)
            continue_conversation: If True, continues previous conversation

        Returns:
            ScreeningResult with ranked list of investment opportunities
        """
        logger.info(f"Screening for opportunities (criteria={criteria}, limit={limit})")

        # Build user query
        if criteria:
            user_query = f"Screen the market database for investment opportunities matching these criteria: {criteria}. Return up to {limit} high-quality stocks ranked by quality score."
        else:
            user_query = f"Screen the market database for high-quality investment opportunities using the default investment principles (high ROIC, ROE, margins, low debt, insider ownership). Return up to {limit} stocks ranked by quality score."

        # Render screener prompt with query parameter
        prompts = self.render_prompt("allocator", "screener", query=user_query)

        # Get max iterations from agent config
        config = get_config()
        agent_config = config.agent_configs.get(self.agent_type, {})
        max_iterations = agent_config.get("max_iterations", config.agents.max_iterations)

        # Execute screening with tools
        response = await self.execute_tool_conversation(
            instructions=prompts["instructions"],
            initial_input=[{"role": "user", "content": prompts["user"]}],
            tools=get_tool_definitions(),
            tool_executor=execute_tool,
            max_iterations=max_iterations,
            previous_response_id=self.last_response_id if continue_conversation else None,
            response_format=ScreeningResult,
        )

        return response

    async def _detect_intent(self, query: str) -> str:
        """Classify query intent using classifier prompt.

        Returns: "analyze" | "similar" | "screen"
        """
        prompts = self.render_prompt("allocator", "classifier", query=query)

        # Use execute_tool_conversation with max_iterations=1 for simple classification
        response = await self.execute_tool_conversation(
            instructions=prompts["instructions"],
            initial_input=[{"role": "user", "content": prompts["user"]}],
            tools=None,
            tool_executor=None,
            max_iterations=1,
            previous_response_id=None,
            response_format=None,
        )

        # Extract text from response
        intent_text = ""
        if hasattr(response, "output_text") and response.output_text:
            intent_text = response.output_text.strip().lower()
        elif hasattr(response, "output") and response.output:
            text_items = [item for item in response.output if hasattr(item, "type") and item.type == "text"]
            if text_items and hasattr(text_items[0], "text"):
                intent_text = text_items[0].text.strip().lower()

        if intent_text not in ["analyze", "similar", "screen"]:
            logger.warning(f"Unknown intent '{intent_text}', defaulting to 'analyze'")
            return "analyze"

        return intent_text

    async def _parse_symbol(self, query: str) -> str:
        """Extract stock ticker from query using extraction prompt."""
        prompts = self.render_prompt("allocator", "extract_symbol", query=query)

        response = await self.execute_tool_conversation(
            instructions=prompts["instructions"],
            initial_input=[{"role": "user", "content": prompts["user"]}],
            tools=None,
            tool_executor=None,
            max_iterations=1,
            previous_response_id=None,
            response_format=None,
        )

        # Extract text from response
        symbol = ""
        if hasattr(response, "output_text") and response.output_text:
            symbol = response.output_text.strip().upper()
        elif hasattr(response, "output") and response.output:
            text_items = [item for item in response.output if hasattr(item, "type") and item.type == "text"]
            if text_items and hasattr(text_items[0], "text"):
                symbol = text_items[0].text.strip().upper()

        if symbol == "UNKNOWN" or not symbol:
            raise ValueError(f"Could not extract stock symbol from query: {query}")

        return symbol

    async def _parse_criteria(self, query: str) -> str | None:
        """Extract screening criteria from query using extraction prompt."""
        prompts = self.render_prompt("allocator", "extract_criteria", query=query)

        response = await self.execute_tool_conversation(
            instructions=prompts["instructions"],
            initial_input=[{"role": "user", "content": prompts["user"]}],
            tools=None,
            tool_executor=None,
            max_iterations=1,
            previous_response_id=None,
            response_format=None,
        )

        # Extract text from response
        criteria = ""
        if hasattr(response, "output_text") and response.output_text:
            criteria = response.output_text.strip()
        elif hasattr(response, "output") and response.output:
            text_items = [item for item in response.output if hasattr(item, "type") and item.type == "text"]
            if text_items and hasattr(text_items[0], "text"):
                criteria = text_items[0].text.strip()

        if criteria == "DEFAULT" or not criteria:
            return None

        return criteria

    async def _route_to_method(self, intent: str, query: str, continue_conv: bool):
        """Route to appropriate private method based on detected intent."""
        if intent == "analyze":
            symbol = await self._parse_symbol(query) if not continue_conv else ""
            return await self._analyze_stock(symbol, continue_conversation=continue_conv)
        elif intent == "similar":
            symbol = await self._parse_symbol(query) if not continue_conv else ""
            return await self._find_similar_stocks(symbol, continue_conversation=continue_conv)
        elif intent == "screen":
            criteria = await self._parse_criteria(query) if not continue_conv else None
            return await self._screen_opportunities(criteria, continue_conversation=continue_conv)
        else:
            raise ValueError(f"Unknown intent: {intent}")

    async def process(self, query: str, continue_conversation: bool = False) -> str:
        """Unified entry point for all interfaces (Console/REST/MCP).

        Always detects intent to determine if query is a new task or follow-up.
        New tasks reset intent; follow-ups continue with cached intent.

        Args:
            query: Natural language investment query
            continue_conversation: If True, allows continuing previous conversation

        Returns:
            Serialized response (JSON for structured data)
        """
        logger.info(f"Processing query (continue={continue_conversation}): {query[:100]}")

        # Always detect intent to determine if this is a new query or follow-up
        detected_intent = await self._detect_intent(query)
        logger.info(f"Detected intent: {detected_intent}")

        # Check if intent changed (new query) or is the same (follow-up to current analysis)
        is_new_query = detected_intent != self.current_intent or not continue_conversation

        if is_new_query:
            # New query with different intent - reset and start fresh
            self.current_intent = detected_intent
            result = await self._route_to_method(detected_intent, query, continue_conv=False)
        else:
            # Same intent and continue_conversation=True - this is a follow-up
            result = await self._route_to_method(self.current_intent, query, continue_conv=True)

        # Serialize structured results for API consumption
        if hasattr(result, "model_dump_json"):
            return result.model_dump_json(indent=2)
        else:
            return str(result)
