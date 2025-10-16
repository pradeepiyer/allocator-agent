# Allocator Agent

AI-powered capital allocation agent for identifying high-quality investment opportunities and analyzing stocks using fundamental investing principles.

## Overview

Allocator Agent is built on [agent-kit](https://github.com/pradeepiyer/agent-kit) and uses OpenAI's Responses API to provide sophisticated stock analysis grounded in proven investment principles:

- **Owner-operator mindset**: Focus on management quality and skin in the game
- **Capital allocation excellence**: Track record of smart buybacks, M&A, and capital deployment
- **Financial quality**: High ROIC, ROE, margins, and strong balance sheets
- **Competitive moats**: Sustainable competitive advantages
- **Reasonable valuations**: Price discipline and margin of safety
- **Technical analysis**: Entry/exit timing and momentum

## Features

### 🎯 Comprehensive Allocator Report (`/analyse`)

Generate a complete investment analysis combining stock analysis and similar company discovery in one unified report:

**Stock Analysis**
- Deep dive covering all investment principles
- Management quality and insider ownership
- Capital allocation track record
- Financial metrics (ROIC, ROE, margins, growth)
- Competitive position and moat assessment
- Valuation analysis with multiples
- Technical indicators and trends
- Clear investment recommendation

**Similar Companies**
- Programmatic discovery using sector/industry classification
- Multi-dimensional comparison (business model, financials, management)
- Ranked results with similarity scores (sector, industry, market cap, margins, growth, ROE)
- Identifies both obvious and non-obvious comparables
- Filters by market cap range (0.3x-3x) for relevance

**PDF Export**
- Professional report with quantitative metrics tables
- Unified comparison table showing all metrics (financial, valuation, technical, ownership) for reference stock and similar stocks side-by-side
- Price charts with moving averages
- Similarity comparison visualizations
- Reports saved to `reports/` directory

### 🛠️ Data Sources

Currently powered by **yfinance** (free, no API key required):
- Financial statements and metrics
- Insider ownership and transactions
- Institutional holdings
- Share buyback data
- Technical indicators
- Historical trends

Can be extended with additional data sources (Financial Modeling Prep, Alpha Vantage, etc.)

## Installation

### Prerequisites

- Python 3.13+
- [uv](https://github.com/astral-sh/uv) package manager
- OpenAI API key

### Setup

1. **Clone the repository**
   ```bash
   cd allocator-agent
   ```

2. **Install dependencies**
   ```bash
   uv sync
   ```

3. **Set up OpenAI API key**
   ```bash
   export OPENAI_API_KEY="sk-..."
   ```

4. **Initialize agent-kit configuration** (first time only)
   ```bash
   uv run agent-kit init
   ```

5. **Run the agent**
   ```bash
   uv run python -m allocator.main
   ```

## Usage

### Interactive Console

Start the interactive console:
```bash
uv run python -m allocator.main
```

### Slash Commands

#### Generate Allocator Report
```
/analyse AAPL
```
Generates a comprehensive allocator report for Apple including:
- Full investment analysis across all principles
- Similar companies with similarity scores
- All metrics displayed in console output
- **Automatic PDF export** to `reports/allocator-AAPL-{timestamp}.pdf`

The PDF report includes:
- Executive summary with recommendation
- Unified comparison table showing all metrics for reference stock and similar stocks (financial, valuation, technical, ownership)
- Price chart with 50/200-day moving averages
- Full analysis text
- Similar stocks with comparison chart
- All sources cited

### Chat Mode

You can also interact in natural language:
```
What do you think about Tesla's capital allocation?
Compare Amazon and Walmart
Is Nvidia overvalued?
```

### Other Commands

- `/help` - Show all available commands
- `/clear` - Clear conversation history
- `/sessions` - List active sessions
- `/quit` - Exit the agent

## Investment Principles

The agent analyzes stocks using these key principles:

### 1. Management Quality & Culture
- Ownership mentality and culture
- Insider ownership percentage (skin in the game)
- Management tenure and track record
- Employee compensation alignment

### 2. Capital Allocation
- Track record of smart capital decisions
- M&A history and success rate
- Share buyback programs and discipline
- Share dilution trends
- Self-funded growth vs capital needs

### 3. Financial Quality
- ROIC (Return on Invested Capital)
- ROE (Return on Equity)
- Profit margins (high and sustainable)
- Balance sheet strength
- Free cash flow generation
- Capital intensity

### 4. Shareholders
- Major shareholders and their track record
- Recent buyer/seller activity
- Quality of institutional investors

### 5. Valuation
- Reasonable valuation vs quality
- Premium/discount justification
- Historical valuation context

### 6. Business Quality
- Competitive dynamics and moat
- Industry structure
- Resilience to economic conditions
- Key risks
- Growth trajectory

### 7. Technical Analysis
- Trend analysis (uptrend/downtrend)
- Entry/exit points
- Momentum indicators
- Sentiment

## Architecture

```
allocator-agent/
├── agents/
│   └── allocator/
│       ├── __init__.py
│       ├── agent.py           # AllocatorAgent with generate_allocator_report()
│       ├── tools.py           # 10 financial data tools (yfinance-based)
│       ├── models.py          # AllocatorReport, StockAnalysis, SimilarStock
│       ├── export.py          # PDF generation with metrics tables
│       ├── console.py         # Console with /analyse command and automatic PDF export
│       ├── main.py            # Entry point
│       ├── config.yaml        # Agent configuration
│       └── prompts/           # Investment principle prompts
│           ├── analyzer.yaml       # Stock analysis
│           └── similarity.yaml     # Similar stock finding
├── pyproject.toml         # Dependencies (agent-kit, yfinance, etc.)
└── README.md
```

### Tools Available

The agent has access to these financial data tools:

- `get_stock_fundamentals()` - Financial metrics, ROIC, ROE, margins
- `get_insider_ownership()` - Insider holdings and transactions
- `get_institutional_holders()` - Major shareholders
- `get_share_data()` - Share count history and buybacks
- `get_management_compensation()` - Executive pay structure
- `get_technical_indicators()` - Price trends, RSI, MACD, moving averages
- `get_valuation_metrics()` - P/E, P/B, P/S, EV/EBITDA, PEG
- `get_financial_history()` - Multi-year trends
- `calculate_similarity()` - Stock comparison
- `find_similar_companies()` - Programmatic discovery of similar stocks by sector/industry

Plus built-in `web_search` for news, filings, and qualitative research.

## Configuration

### Agent Configuration

The agent config is in `agents/allocator/config.yaml` and will be auto-loaded by agent-kit.

```yaml
max_iterations: 25  # Allows for complex multi-step analysis
```

### Global agent-kit Configuration

Configure OpenAI model and settings in `~/.agent-kit/config.yaml`:

```yaml
openai:
  api_key: "${OPENAI_API_KEY}"
  model: "gpt-4o"
  pool_size: 8

agents:
  max_iterations: 20
  max_parallel_tools: 5
```

## Development

### Project Structure

The agent follows agent-kit's extension pattern:

1. **Tools** (`tools.py`): Python functions that fetch/analyze data
2. **Prompts** (`prompts/*.yaml`): YAML files encoding investment principles
3. **Agent** (`agent.py`): Methods that orchestrate tool calls using prompts
4. **Console** (`console.py`): User interface with slash commands

### Adding New Tools

1. Define the async function in `tools.py`
2. Add tool definition to `get_tool_definitions()`
3. Register in `execute_tool()` dispatcher
4. The LLM can now use it automatically

### Extending with More Data Sources

To add Financial Modeling Prep or other APIs:

1. Install additional package (e.g., `uv add financialmodelingprep`)
2. Add API functions to `tools.py`
3. Add corresponding tool definitions
4. Update prompts if needed

No changes to agent logic required - tools are automatically available to LLM.

## Examples

### Example: Comprehensive Allocator Report
```
/analyse BRK.B

Result: Complete allocator report with:

Part 1 - Stock Analysis:
- Buffett/Munger's capital allocation track record
- Insurance float as competitive advantage
- Ownership of wholly-owned subsidiaries
- Key Metrics (ROIC, ROE, margins, valuation multiples)
- Valuation vs book value
- Investment recommendation: Buy (High conviction)

Part 2 - Similar Companies:
1. MKL (Markel) - Similar insurance + investment model (Similarity: 78)
2. FFH (Fairfax Financial) - Comparable structure (Similarity: 72)
3. Y (Alleghany) - Insurance conglomerate (Similarity: 65)
Each with detailed similarity analysis
```

### Example: Workflow
```
/analyse AAPL

▶ Generating comprehensive report for AAPL...

... analysis output displayed in console ...

▶ Generating PDF report...
✓ PDF report saved to reports/allocator-AAPL-20251016-143052.pdf

# Now you have both:
# 1. Console output for immediate reading
# 2. Professional PDF report for review/sharing
```

## Tips for Best Results

1. **Use `/analyse` for comprehensive reports**: Gets you analysis, similar companies, and PDF report automatically
2. **Review the PDF**: Contains unified comparison table and charts for deeper analysis
3. **Use chat for specific questions**: "What's AAPL's ROIC?" or "Compare GOOGL and META's capital allocation"
4. **Check comparison table in PDF**: Shows all metrics for reference stock and similar stocks side-by-side
5. **Explore similar companies**: Often reveals non-obvious investment alternatives
6. **Verify sources**: Agent cites all sources - always verify important claims

## Limitations

- **yfinance data**: Free but may have delays or gaps
- **No real-time data**: 15-minute delayed quotes
- **Historical data**: Limited to what yfinance provides
- **Not financial advice**: For educational/research purposes only

## Future Enhancements

- [ ] Integration with Financial Modeling Prep for additional data sources
- [ ] SEC Edgar filing analysis
- [ ] Portfolio tracking and rebalancing suggestions
- [ ] Backtesting capabilities
- [ ] Custom watchlists
- [ ] Email/Slack alerts for opportunities
- [ ] DeMark Sequential indicator implementation
- [ ] Sentiment analysis from news and social media

## License

MIT

## Disclaimer

This tool is for educational and research purposes only. It does not constitute financial advice. Always do your own research and consult with qualified financial advisors before making investment decisions.

## Contributing

Contributions welcome! Please open an issue or PR.

---

Built with [agent-kit](https://github.com/pradeepiyer/agent-kit) | Powered by OpenAI
