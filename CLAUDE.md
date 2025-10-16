# Allocator Agent - Claude Instructions

## Overview
AI-powered capital allocation agent for stock analysis using fundamental investing principles. Built on agent-kit, focuses on owner-operator mindset, capital allocation excellence, and high-quality businesses.

## Tech Stack
- Python 3.13+, uv package manager
- agent-kit framework (OpenAI Responses API)
- yfinance for financial data (free, no API key)
- pandas, numpy for data processing
- pandas-ta for technical indicators
- rich console for UI

## Architecture
- **AllocatorAgent**: Extends BaseAgent with 4 methods (analyze, similar, screen, process)
- **Tools**: 10 financial data tools in `tools.py` (fundamentals, ownership, technical, valuation)
- **Prompts**: 3 YAML files encoding investment principles (orchestrator, similarity, screener)
- **Console**: Interactive CLI with slash commands (`/analyze`, `/similar`, `/screen`)
- **Session Management**: Thread-safe sessions via agent-kit

## Investment Philosophy
Analysis framework covers 8 dimensions:
1. **Management Quality**: Insider ownership, skin in the game, tenure
2. **Capital Allocation**: Buybacks, M&A track record, share dilution
3. **Financial Quality**: ROIC, ROE, margins, FCF, balance sheet
4. **Shareholders**: Institutional holders, insider transactions
5. **Valuation**: P/E, P/B, P/S, EV/EBITDA, PEG ratio
6. **Business Quality**: Moat, competitive dynamics, industry structure
7. **Market Position**: Consensus view, hidden assets, opportunities
8. **Technical**: Trend, momentum, RSI, MACD, moving averages

## Key Features
- **Stock Analysis** (`/analyze SYMBOL`): Comprehensive analysis across all 8 dimensions
- **Similar Stocks** (`/similar SYMBOL`): Multi-dimensional comparison and ranking
- **Screening** (`/screen [criteria]`): Quantitative + qualitative filtering
- **Chat Mode**: Natural language queries with investment context
- **Web Search**: News, filings, management research with citations

## Agent Pattern
```python
# Standard pattern: render prompt → execute_tool_conversation → extract text
async def analyze_stock(self, symbol: str, continue_conversation: bool = False) -> str:
    prompts = self.render_prompt("allocator", "orchestrator")

    response = await self.execute_tool_conversation(
        instructions=prompts["instructions"],
        initial_input=[{"role": "user", "content": f"Analyze {symbol}..."}],
        tools=get_tool_definitions(),
        tool_executor=execute_tool,
        max_iterations=25,  # Complex multi-step analysis
        previous_response_id=self.last_response_id if continue_conversation else None,
        response_format=None,
    )

    return self._extract_response_text(response)
```

## Tool System
Financial data tools in `tools.py`:
- `get_stock_fundamentals()` - ROIC, ROE, margins, balance sheet
- `get_insider_ownership()` - Insider %, recent transactions
- `get_institutional_holders()` - Major shareholders
- `get_share_data()` - Share count history, buybacks
- `get_management_compensation()` - Executive pay, SBC
- `get_technical_indicators()` - RSI, MACD, trends, momentum
- `get_valuation_metrics()` - P/E, P/B, P/S, EV/EBITDA
- `get_financial_history()` - Multi-year trends
- `calculate_similarity()` - Stock comparison scores
- `screen_stocks()` - Placeholder (requires paid API)

## Development Guidelines
- Follow agent-kit patterns (async, Pydantic, Responses API)
- Brief comments, contextual docstrings
- Run commands under uv: `uv run python -m allocator.main`
- Use `make lint` before committing
- Web search citations: [1], [2] with Sources section
- YAML prompts use markdown headers for structure

## Testing
```bash
# Run linter
make lint

# Run type checker
uv run pyright

# Run tests
make test

# Local CI
make ci

# Launch console
make console
```

## Configuration
- `allocator/config.yaml`: Agent-specific settings (max_iterations: 25)
- `~/.agent-kit/config.yaml`: Global OpenAI settings
- `OPENAI_API_KEY` environment variable required

## Key Principles
- **Investment rigor**: Always support conclusions with data
- **Balanced view**: Highlight both positives and risks
- **Citation discipline**: Source all web search claims
- **Quality focus**: High ROIC, strong moats, smart capital allocation
- **Valuation discipline**: Great business at wrong price is bad investment
- **Owner-operator lens**: Management skin in the game matters
- **Long-term view**: Track records over single-quarter results
