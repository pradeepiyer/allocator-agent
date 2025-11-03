# Allocator Agent

AI-powered stock analysis agent using fundamental investing principles. Built on [agent-kit](https://github.com/pradeepiyer/agent-kit).

## What It Does

Analyzes stocks through the lens of capital allocation excellence:
- **High-quality businesses**: ROIC ≥15%, ROE ≥15%, margins ≥10%
- **Owner-operator mindset**: Management quality and insider ownership
- **Smart capital allocation**: Buybacks, M&A track record, low share dilution
- **Sustainable moats**: Competitive advantages and pricing power
- **Valuation discipline**: Price matters, even for great businesses

## Quick Start

```bash
# Install
git clone https://github.com/pradeepiyer/allocator-agent.git
cd allocator-agent
uv sync

# Configure
export OPENAI_API_KEY="sk-..."
uv run agent-kit init

# Run
uv run python -m allocator.main
```

## Usage

**Screen for opportunities** (scans 3000+ stocks from Russell 3000 database):
```
/screen
/screen tech stocks with high margins
```

**Analyze a stock** (generates comprehensive report + similar companies):
```
/analyse AAPL
```

**Chat mode** (natural language queries):
```
What's NVDA's capital allocation track record?
Compare META and GOOGL
```

All analysis generates PDF reports in `reports/`.

## Features

### Investment Screening
- Two-stage LLM-powered screening (broad filter → detailed analysis)
- Cached database of 3000+ stocks with fundamentals, ownership, technical data
- Quality scoring (0-100) across 8 dimensions
- Automatic PDF export with ranked results

### Stock Analysis
- Comprehensive investment analysis (management, financials, moat, valuation)
- Programmatic similar company discovery
- Side-by-side comparison tables
- Clear recommendation (Strong Buy / Buy / Hold / Sell / Pass)

### Data Infrastructure
- SQLite cache with Russell 3000 data
- Annual fundamentals (ROIC, ROE, margins, cash flow, balance sheet)
- Quarterly share counts, insider ownership, institutional holdings
- Technical indicators (RSI, MACD, trends)
- yfinance API (free, no key required)

## Analysis Framework

The agent evaluates stocks across 8 dimensions:

1. **Management Quality**: Insider ownership, skin in the game, track record
2. **Capital Allocation**: Buybacks, M&A, share dilution, capital efficiency
3. **Financial Quality**: ROIC, ROE, margins, cash flow, balance sheet
4. **Shareholders**: Institutional holders, insider transactions
5. **Valuation**: P/E, P/B, P/S, EV/EBITDA, PEG relative to quality
6. **Business Quality**: Moat, competitive dynamics, resilience
7. **Market Position**: Consensus view, hidden assets, opportunity type
8. **Technical**: Trend, momentum, entry/exit points

## Architecture

```
agents/allocator/
├── agent.py              # AllocatorAgent (analyze, screen, similar stocks)
├── tools.py              # 13 financial data tools
├── models.py             # Pydantic models for structured outputs
├── export.py             # PDF generation
├── console.py            # Interactive CLI
├── db.py                 # Database cache layer
├── main.py               # Entry point
├── data/market.db        # SQLite cache (~3000 stocks)
└── prompts/
    ├── analyzer.yaml     # Stock analysis principles (6 prompts total)
    ├── similarity.yaml
    └── screener.yaml
```

**Tools** (`tools.py`):
- `get_stock_fundamentals()` - ROIC, ROE, margins, balance sheet
- `get_insider_ownership()` - Insider %, transactions
- `get_institutional_holders()` - Major shareholders
- `get_share_data()` - Share count history, buybacks
- `get_management_compensation()` - Executive pay, SBC
- `get_technical_indicators()` - RSI, MACD, trends
- `get_valuation_metrics()` - P/E, P/B, P/S, EV/EBITDA
- `get_financial_history()` - Multi-year trends
- `find_similar_companies()` - Sector/industry discovery
- `screen_database_initial()` - Stage 1 screening
- `get_detailed_metrics()` - Stage 2 screening
- `web_search` - News, filings, research

## Configuration

`agents/allocator/config.yaml`:
```yaml
max_iterations: 25  # Complex multi-step analysis
```

`~/.agent-kit/config.yaml`:
```yaml
openai:
  api_key: "${OPENAI_API_KEY}"
  model: "gpt-4o"
```

## Database Management

```bash
# Refresh all cached data
uv run python scripts/refresh_market_data.py

# Refresh specific symbols
uv run python scripts/refresh_market_data.py AAPL MSFT

# Download initial dataset
uv run python scripts/download_market_data.py
```

## Development

```bash
make lint      # Run linter
make test      # Run tests
make ci        # Full CI check
```

**Extension pattern** (following agent-kit):
1. Add async tool function in `tools.py`
2. Register in `get_tool_definitions()` and `execute_tool()`
3. Update YAML prompts in `prompts/` to guide tool usage
4. Define Pydantic models in `models.py` for structured outputs

## Limitations

- Dependent on yfinance data quality
- No real-time data (15-min delays)
- Historical metrics only (not predictive)
- For educational/research purposes only (not financial advice)

## License

MIT

## Disclaimer

Educational and research purposes only. Not financial advice. Do your own research and consult qualified advisors before investing.

---

Built with [agent-kit](https://github.com/pradeepiyer/agent-kit) | Powered by OpenAI
