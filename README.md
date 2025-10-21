# Allocator Agent

AI-powered capital allocation agent for identifying high-quality investment opportunities and analyzing stocks using fundamental investing principles.

## Overview

Allocator Agent is built on [agent-kit](https://github.com/pradeepiyer/agent-kit) and uses OpenAI's Responses API to provide sophisticated stock analysis grounded in proven investment principles:

- **Owner-operator mindset**: Focus on management quality and skin in the game
- **Capital allocation excellence**: Track record of smart buybacks, M&A, and capital deployment
- **Financial quality**: High ROIC, ROE, margins, and strong balance sheets with low to no debt
- **Competitive moats**: Sustainable competitive advantages
- **Reasonable valuations**: Price discipline and margin of safety
- **Incentive alignment**: Management incentives aligned with long-term business growth
- **Capital efficiency**: Prefer businesses that are capital light with high profit margins and sustainable cash flow

## Features

### 🔍 Investment Screener (`/screen`)

Discover new high-quality investment opportunities using a two-stage LLM-powered screening process:

**Stage 1: Broad Initial Screen**
- Queries market database with quantitative filters (ROIC ≥15%, ROE ≥15%, margins ≥10%)
- Returns 50-100 candidates with key metrics
- LLM evaluates and identifies top 25-30 finalists

**Stage 2: Detailed Analysis**
- Fetches comprehensive data for finalists only
- Applies full investment principles
- Scores and ranks top 20 opportunities

**Features:**
- Screens from database of 2000+ stocks with cached financial data
- Evaluates across 8 investment dimensions
- Quality scoring (0-100) based on financial metrics, ownership, business model
- Automatic PDF export to `reports/screening-{timestamp}.pdf`
- Follow-up questions in chat mode ("tell me more about #3", "analyze NVDA")

### 🎯 Stock Analysis (`/analyse`)

Generate comprehensive investment analysis for a specific stock:

**Investment Analysis**
- Management quality and insider ownership
- Capital allocation track record
- Financial metrics (ROIC, ROE, margins, growth)
- Competitive position and moat assessment
- Valuation analysis with multiples
- Technical indicators and trends
- Clear investment recommendation (Strong Buy / Buy / Hold / Sell / Pass)

**Similar Companies Discovery**
- Programmatic discovery using sector/industry classification
- Multi-dimensional comparison (business model, financials, management)
- Ranked results with similarity scores
- Identifies both obvious and non-obvious comparables

**PDF Export**
- Professional report with quantitative metrics tables
- Unified comparison table showing all metrics side-by-side
- Price charts with moving averages
- Reports saved to `reports/allocator-{symbol}-{timestamp}.pdf`

### 💬 Chat Mode

Interactive analysis with follow-up questions:
- "What do you think about Tesla's capital allocation?"
- "Compare Amazon and Walmart"
- "Is Nvidia overvalued?"
- After `/screen`: "Tell me more about #3", "Analyze AAPL and export PDF"

### 🗄️ Data Infrastructure

**Market Database**
- 2000+ stocks with comprehensive financial data
- Annual fundamentals (ROIC, ROE, margins, cash flow, balance sheet)
- Quarterly share counts and buyback data
- Insider ownership and transactions
- Institutional holdings
- Stock-based compensation
- Price history and technical indicators
- Read-through cache with yfinance fallback

**Data Sources**
- Primary: yfinance (free, no API key required)
- Cached locally for fast access
- Refreshable with market data scripts

## Installation

### Prerequisites

- Python 3.13+
- [uv](https://github.com/astral-sh/uv) package manager
- OpenAI API key

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/pradeepiyer/allocator-agent.git
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

### Quick Start

Start the interactive console:
```bash
uv run python -m allocator.main
```

### Commands

#### Find Investment Opportunities
```
/screen
```
Discovers high-quality stocks matching investment criteria:
- Screens 2000+ stocks from database
- Two-stage filtering for efficiency
- Returns top 20 ranked by quality score
- Automatic PDF export to `reports/`
- Enables chat follow-ups

Examples:
```
/screen
/screen tech stocks with high margins
/screen healthcare under $10B market cap
```

#### Analyze Specific Stock
```
/analyse AAPL
```
Generates comprehensive allocator report including:
- Full investment analysis
- Similar companies with similarity scores
- Automatic PDF export to `reports/`

#### Chat Mode
```
What's NVDA's ROIC?
Compare META and GOOGL capital allocation
Tell me more about #3
Analyze MSFT and export PDF
```

#### Utility Commands
- `/help` - Show all available commands
- `/clear` - Clear conversation history
- `/sessions` - List active sessions
- `/quit` - Exit the agent

## Investment Principles

The agent analyzes stocks using these 8 key dimensions:

### 1. Management Quality & Culture
- Ownership mentality (does management think like owners?)
- Insider ownership percentage (skin in the game)
- Management tenure and execution track record
- Employee compensation alignment with shareholders

### 2. Capital Allocation Excellence
- Track record of smart capital decisions
- M&A history and success rate (accretive or destructive?)
- Share buyback programs and price discipline
- Share dilution trends
- Self-funded growth vs capital needs

### 3. Financial Quality
- **ROIC ≥ 15%**: High return on invested capital
- **ROE ≥ 15%**: Strong return on equity
- **Profit Margin ≥ 10%**: Sustainable margins showing pricing power
- Balance sheet strength (low debt preferred)
- Free cash flow generation
- Capital intensity (capital-light preferred)

### 4. Shareholders & Ownership
- Major shareholders and their track record
- Recent buyer/seller activity
- Quality of institutional investors
- Insider buying/selling patterns

### 5. Valuation Analysis
- Reasonable valuation relative to quality
- Premium/discount justification vs peers
- Forward P/E, P/B, P/S, EV/EBITDA, PEG ratio
- Historical valuation context

### 6. Business Quality & Resilience
- Competitive dynamics and economic moat
- Industry structure and competitive intensity
- Resilience to economic conditions (inflation, recession)
- Key risks and vulnerabilities
- Growth trajectory and drivers

### 7. Market Position
- Consensus view (are you contrarian or aligned?)
- Hidden assets or underpriced optionality
- Idiosyncratic opportunity or commodity business?

### 8. Technical & Timing
- Trend analysis (uptrend/downtrend, breakout/breakdown)
- Entry/exit points and support/resistance
- Momentum indicators (RSI, MACD)
- Sentiment analysis

## Architecture

```
allocator-agent/
├── agents/
│   └── allocator/
│       ├── agent.py              # AllocatorAgent with screen/analyze methods
│       ├── tools.py              # 13 financial data tools
│       ├── models.py             # Pydantic models for structured outputs
│       ├── export.py             # PDF generation (analysis + screening)
│       ├── console.py            # Interactive CLI with /screen, /analyse
│       ├── db.py                 # Database cache layer
│       ├── main.py               # Entry point
│       ├── config.yaml           # Agent configuration
│       ├── data/
│       │   └── market.db         # SQLite cache (705MB, 5000+ stocks)
│       └── prompts/
│           ├── analyzer.yaml     # Stock analysis principles
│           ├── similarity.yaml   # Similar company discovery
│           └── screener.yaml     # Two-stage screening workflow
├── scripts/
│   ├── download_market_data.py   # Populate database cache
│   ├── download_symbols.py       # Fetch stock symbols
│   └── refresh_market_data.py    # Update cached data
├── reports/                      # Auto-generated PDF reports
├── pyproject.toml                # Dependencies
└── README.md
```

### Available Tools

The agent has access to these financial data tools:

**Stock Fundamentals**
- `get_stock_fundamentals()` - ROIC, ROE, margins, balance sheet, cash flow
- `get_financial_history()` - Multi-year trend analysis
- `get_valuation_metrics()` - P/E, P/B, P/S, EV/EBITDA, PEG ratio

**Ownership & Management**
- `get_insider_ownership()` - Insider holdings and transactions
- `get_institutional_holders()` - Major shareholders
- `get_share_data()` - Share count history and buybacks
- `get_management_compensation()` - Executive pay and stock-based compensation

**Technical Analysis**
- `get_technical_indicators()` - Price trends, RSI, MACD, moving averages

**Stock Comparison**
- `calculate_similarity()` - Quantitative similarity scoring
- `find_similar_companies()` - Sector/industry-based discovery

**Investment Screening** (Two-Stage)
- `screen_database_initial()` - Stage 1: Broad pool with key metrics (50-100 stocks)
- `get_detailed_metrics()` - Stage 2: Comprehensive data for finalists
- `screen_database()` - Legacy single-stage (deprecated)

**Web Research**
- `web_search` - News, filings, management research with citations

## Configuration

### Agent Configuration

Set in `agents/allocator/config.yaml`:

```yaml
max_iterations: 25  # Complex multi-step analysis and screening
```

### Global agent-kit Configuration

Configure in `~/.agent-kit/config.yaml`:

```yaml
openai:
  api_key: "${OPENAI_API_KEY}"
  model: "gpt-4o"
  pool_size: 8

agents:
  max_iterations: 20
  max_parallel_tools: 5
```

## Database Management

### Refresh Market Data

Update the cached database with latest financial data:

```bash
# Refresh all data
uv run python scripts/refresh_market_data.py

# Refresh specific symbols
uv run python scripts/refresh_market_data.py AAPL MSFT GOOGL

# Download initial dataset
uv run python scripts/download_market_data.py
```

The database cache significantly speeds up screening and analysis by avoiding repeated API calls.

## Development

### Project Structure

The agent follows agent-kit's extension pattern:

1. **Tools** (`tools.py`): Async functions that fetch/analyze data
2. **Prompts** (`prompts/*.yaml`): YAML files encoding investment principles
3. **Models** (`models.py`): Pydantic models for structured LLM outputs
4. **Agent** (`agent.py`): Methods orchestrating tool calls using prompts
5. **Console** (`console.py`): User interface with slash commands

### Adding New Tools

1. Define async function in `tools.py`
2. Add tool definition to `get_tool_definitions()`
3. Register in `execute_tool()` dispatcher
4. LLM can now use it automatically

### Extending Prompts

Edit YAML files in `prompts/` to:
- Modify investment criteria
- Adjust scoring methodology
- Add new analysis dimensions
- Change output format

### Testing

```bash
# Run linter
make lint

# Run type checker
uv run pyright

# Run tests
make test

# Full CI check
make ci
```

## Examples

### Example: Finding Investment Opportunities

```
> /screen

▶ Screening for high-quality investment opportunities...

# Investment Opportunities Screener

**Screening Criteria:** High ROIC (≥15%), ROE (≥15%), profit margins (≥10%), low debt
**Total Analyzed:** 50
**High-Quality Matches:** 20

## 1. NVDA - NVIDIA Corporation
**Sector:** Technology | **Quality Score:** 95/100

### Key Strengths
- Exceptional ROIC of 68.8% demonstrates outstanding capital efficiency
- Industry-leading profit margins of 53.4% show dominant market position
- Minimal debt with debt-to-equity of 0.21
- Strong insider ownership at 4.2%
- Capital-light business model with massive free cash flow

### Key Metrics
- ROIC: 68.8%, ROE: 91.9%, Profit Margin: 53.4%
- Debt/Equity: 0.21, Insider Ownership: 4.2%
- Forward P/E: 28.5, Market Cap: $2.1T

## 2. CALM - Cal-Maine Foods, Inc.
**Sector:** Consumer Staples | **Quality Score:** 88/100
...

▶ Generating PDF report...
✓ PDF report saved to reports/screening-20251020-153045.pdf

💡 Tip: You can ask follow-up questions like 'tell me more about #3' or 'analyze AAPL and export PDF'
```

### Example: Stock Analysis

```
> /analyse BRK.B

▶ Generating comprehensive report for BRK.B...

# BRK.B - Berkshire Hathaway Inc.

**Recommendation:** Buy (Conviction: High)

## Investment Thesis
Berkshire Hathaway represents the gold standard of capital allocation...

## Similar Stocks

1. MKL - Markel Corporation (Similarity: 78/100)
   - Similar insurance + investment company structure
   - Excellent underwriting discipline
   - Strong capital allocation track record

▶ Generating PDF report...
✓ PDF report saved to reports/allocator-BRK.B-20251020-154312.pdf
```

### Example: Chat Follow-ups

```
> /screen

[... screening results ...]

> Tell me more about #3

Analyzing TPL (Texas Pacific Land Corporation)...

TPL is a unique land management company with exceptional economics:
- ROIC: 35.9% driven by royalty-based revenue model
- Zero debt capital structure provides ultimate financial flexibility
- Owns 880,000 acres in Permian Basin with oil/gas royalties
- Trading at 18x forward earnings despite pristine balance sheet
...

> Analyze TPL and export PDF

▶ Generating comprehensive report for TPL...
✓ PDF report saved to reports/allocator-TPL-20251020-155523.pdf
```

## Tips for Best Results

1. **Start with `/screen`**: Discover opportunities systematically
2. **Use `/analyse` for deep dives**: Comprehensive analysis of specific stocks
3. **Review PDFs**: Professional reports for detailed review and sharing
4. **Ask follow-ups**: Drill into specific aspects via chat
5. **Check comparison tables**: Side-by-side metrics reveal insights
6. **Explore similar companies**: Often reveals non-obvious alternatives
7. **Verify sources**: Agent cites all sources - always verify claims
8. **Refresh database**: Run refresh scripts periodically for latest data

## Limitations

- **Data quality**: Dependent on yfinance data quality and coverage
- **No real-time data**: Market data may have 15-minute delays
- **Historical focus**: Limited predictive capability, based on historical metrics
- **Not financial advice**: For educational and research purposes only
- **Coverage**: Best results for US-listed stocks with complete financial data

## License

MIT

## Disclaimer

This tool is for educational and research purposes only. It does not constitute financial advice. Always do your own research and consult with qualified financial advisors before making investment decisions. Past performance does not guarantee future results.

## Contributing

Contributions welcome! Please open an issue or PR.

Key areas for contribution:
- Additional data sources integration
- New screening criteria and filters
- Enhanced PDF visualizations
- Performance optimizations
- Test coverage improvements

---

Built with [agent-kit](https://github.com/pradeepiyer/agent-kit) | Powered by OpenAI
