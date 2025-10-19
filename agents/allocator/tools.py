"""Financial data tools for Allocator Agent using yfinance."""

import logging
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


def _safe_date_str(value: Any) -> str | None:
    """Convert datetime/Timestamp to string safely."""
    if pd.isna(value) or value is None:
        return None
    if hasattr(value, "date"):
        return str(value.date())
    return str(value)


async def get_stock_fundamentals(symbol: str) -> dict[str, Any]:
    """Get fundamental financial metrics for a stock.

    Covers: ROIC, ROE, margins, balance sheet strength, capital allocation metrics.

    Args:
        symbol: Stock ticker symbol

    Returns:
        Dictionary with fundamental metrics
    """
    try:
        stock = yf.Ticker(symbol)
        info = stock.info

        # Get financial statements for ROIC calculation and cash flow metrics
        roic = None
        free_cash_flow = None
        operating_cash_flow = None

        try:
            balance_sheet = stock.balance_sheet
            financials = stock.financials
            cash_flow = stock.cashflow

            # Calculate ROIC if data available
            if not balance_sheet.empty and not financials.empty:
                # ROIC = NOPAT / Invested Capital
                # Simplified: Operating Income * (1 - tax rate) / (Total Assets - Current Liabilities)
                try:
                    operating_income = (
                        financials.loc["Operating Income"].iloc[0] if "Operating Income" in financials.index else None
                    )
                    total_assets = (
                        balance_sheet.loc["Total Assets"].iloc[0] if "Total Assets" in balance_sheet.index else None
                    )
                    current_liabilities = (
                        balance_sheet.loc["Current Liabilities"].iloc[0]
                        if "Current Liabilities" in balance_sheet.index
                        else None
                    )

                    if operating_income and total_assets and current_liabilities:
                        tax_rate = info.get("effectiveTaxRate", 0.21)
                        nopat = operating_income * (1 - tax_rate)
                        invested_capital = total_assets - current_liabilities
                        roic = (nopat / invested_capital) if invested_capital > 0 else None
                except Exception as e:
                    logger.debug(f"Could not calculate ROIC for {symbol}: {e}")

            # Get cash flow metrics from annual statement (more stable than TTM)
            if not cash_flow.empty:
                try:
                    if "Free Cash Flow" in cash_flow.index:
                        fcf_value = cash_flow.loc["Free Cash Flow"].iloc[0]
                        if pd.notna(fcf_value):
                            free_cash_flow = float(fcf_value)
                except Exception as e:
                    logger.debug(f"Could not fetch annual FCF for {symbol}: {e}")

                try:
                    if "Operating Cash Flow" in cash_flow.index:
                        ocf_value = cash_flow.loc["Operating Cash Flow"].iloc[0]
                        if pd.notna(ocf_value):
                            operating_cash_flow = float(ocf_value)
                except Exception as e:
                    logger.debug(f"Could not fetch annual OCF for {symbol}: {e}")

        except Exception as e:
            logger.debug(f"Could not fetch financial statements for {symbol}: {e}")

        return {
            "symbol": symbol,
            "company_name": info.get("longName", "N/A"),
            "sector": info.get("sector", "N/A"),
            "industry": info.get("industry", "N/A"),
            "market_cap": info.get("marketCap"),
            "enterprise_value": info.get("enterpriseValue"),
            # Profitability & Returns
            "roic": roic,
            "roe": info.get("returnOnEquity"),
            "roa": info.get("returnOnAssets"),
            "profit_margin": info.get("profitMargins"),
            "operating_margin": info.get("operatingMargins"),
            "gross_margin": info.get("grossMargins"),
            # Balance Sheet
            "debt_to_equity": info.get("debtToEquity"),
            "current_ratio": info.get("currentRatio"),
            "quick_ratio": info.get("quickRatio"),
            "total_cash": info.get("totalCash"),
            "total_debt": info.get("totalDebt"),
            # Cash Flow (prefer annual from statement, fallback to TTM from info)
            "free_cash_flow": free_cash_flow if free_cash_flow is not None else info.get("freeCashflow"),
            "operating_cash_flow": operating_cash_flow
            if operating_cash_flow is not None
            else info.get("operatingCashflow"),
            # Growth
            "revenue_growth": info.get("revenueGrowth"),
            "earnings_growth": info.get("earningsGrowth"),
            # Additional metrics
            "beta": info.get("beta"),
            "52_week_high": info.get("fiftyTwoWeekHigh"),
            "52_week_low": info.get("fiftyTwoWeekLow"),
            "current_price": info.get("currentPrice"),
        }
    except Exception as e:
        logger.error(f"Error fetching fundamentals for {symbol}: {e}")
        return {"error": str(e), "symbol": symbol}


async def get_insider_ownership(symbol: str) -> dict[str, Any]:
    """Get insider ownership and recent insider transactions.

    Covers: Skin in the game, insider buying/selling activity.

    Args:
        symbol: Stock ticker symbol

    Returns:
        Dictionary with insider ownership data
    """
    try:
        stock = yf.Ticker(symbol)
        info = stock.info

        # Get insider transactions
        insider_transactions = []
        try:
            insider_txns = stock.insider_transactions
            if not insider_txns.empty:
                # Get recent transactions (last 6 months)
                recent_txns = insider_txns.head(20)  # Latest 20 transactions
                for _, txn in recent_txns.iterrows():
                    insider_transactions.append(
                        {
                            "date": _safe_date_str(txn.get("Start Date")),
                            "insider": txn.get("Insider Trading"),
                            "shares": txn.get("Shares"),
                            "value": txn.get("Value"),
                            "transaction": txn.get("Transaction"),
                        }
                    )
        except Exception as e:
            logger.debug(f"Could not fetch insider transactions for {symbol}: {e}")

        return {
            "symbol": symbol,
            "insider_ownership_pct": info.get("heldPercentInsiders"),
            "institutional_ownership_pct": info.get("heldPercentInstitutions"),
            "recent_transactions": insider_transactions,
            "shares_outstanding": info.get("sharesOutstanding"),
            "float_shares": info.get("floatShares"),
            "shares_short": info.get("sharesShort"),
        }
    except Exception as e:
        logger.error(f"Error fetching insider ownership for {symbol}: {e}")
        return {"error": str(e), "symbol": symbol}


async def get_institutional_holders(symbol: str) -> dict[str, Any]:
    """Get institutional holders and recent activity.

    Covers: Who are major shareholders, recent buyers/sellers.

    Args:
        symbol: Stock ticker symbol

    Returns:
        Dictionary with institutional holder data
    """
    try:
        stock = yf.Ticker(symbol)

        # Get institutional holders
        institutional_holders = []
        try:
            holders = stock.institutional_holders
            if not holders.empty:
                for _, holder in holders.iterrows():
                    institutional_holders.append(
                        {
                            "holder": holder.get("Holder"),
                            "shares": holder.get("Shares"),
                            "date_reported": _safe_date_str(holder.get("Date Reported")),
                            "pct_out": holder.get("% Out"),
                            "value": holder.get("Value"),
                        }
                    )
        except Exception as e:
            logger.debug(f"Could not fetch institutional holders for {symbol}: {e}")

        # Get major holders summary
        major_holders_summary = {}
        try:
            major_holders = stock.major_holders
            if not major_holders.empty:
                major_holders_summary = major_holders.to_dict()
        except Exception as e:
            logger.debug(f"Could not fetch major holders for {symbol}: {e}")

        return {
            "symbol": symbol,
            "institutional_holders": institutional_holders[:10],  # Top 10
            "major_holders_summary": major_holders_summary,
        }
    except Exception as e:
        logger.error(f"Error fetching institutional holders for {symbol}: {e}")
        return {"error": str(e), "symbol": symbol}


async def get_share_data(symbol: str) -> dict[str, Any]:
    """Get share count history and buyback activity.

    Covers: Share dilution/reduction, corporate buybacks.

    Args:
        symbol: Stock ticker symbol

    Returns:
        Dictionary with share count and buyback data
    """
    try:
        stock = yf.Ticker(symbol)
        info = stock.info

        # Get historical share count
        shares_history = []
        try:
            quarterly = stock.quarterly_balance_sheet
            if not quarterly.empty and "Ordinary Shares Number" in quarterly.index:
                shares_series = quarterly.loc["Ordinary Shares Number"]
                for date, shares in shares_series.items():
                    shares_history.append(
                        {
                            "date": str(date.date()) if hasattr(date, "date") else str(date),
                            "shares": float(shares) if pd.notna(shares) else None,
                        }
                    )
        except Exception as e:
            logger.debug(f"Could not fetch share count history for {symbol}: {e}")

        # Get buyback data from cash flow
        buyback_history = []
        try:
            cash_flow = stock.cashflow
            if not cash_flow.empty:
                if "Repurchase Of Capital Stock" in cash_flow.index:
                    buybacks = cash_flow.loc["Repurchase Of Capital Stock"]
                    for date, amount in buybacks.items():
                        if pd.notna(amount) and amount != 0:
                            buyback_history.append(
                                {
                                    "date": str(date.date()) if hasattr(date, "date") else str(date),
                                    "amount": float(amount),
                                }
                            )
        except Exception as e:
            logger.debug(f"Could not fetch buyback history for {symbol}: {e}")

        return {
            "symbol": symbol,
            "shares_outstanding": info.get("sharesOutstanding"),
            "float_shares": info.get("floatShares"),
            "shares_history": shares_history,
            "buyback_history": buyback_history,
            "implied_shares_outstanding": info.get("impliedSharesOutstanding"),
        }
    except Exception as e:
        logger.error(f"Error fetching share data for {symbol}: {e}")
        return {"error": str(e), "symbol": symbol}


async def get_management_compensation(symbol: str) -> dict[str, Any]:
    """Get management compensation structure.

    Covers: How employees are compensated, stock-based comp.

    Args:
        symbol: Stock ticker symbol

    Returns:
        Dictionary with compensation data
    """
    try:
        stock = yf.Ticker(symbol)

        # Get key executives
        executives = []
        try:
            officers = stock.info.get("companyOfficers", [])
            for officer in officers[:5]:  # Top 5 executives
                executives.append(
                    {
                        "name": officer.get("name"),
                        "title": officer.get("title"),
                        "total_pay": officer.get("totalPay"),
                        "exercised_value": officer.get("exercisedValue"),
                        "unexercised_value": officer.get("unexercisedValue"),
                    }
                )
        except Exception as e:
            logger.debug(f"Could not fetch executive data for {symbol}: {e}")

        # Get stock-based compensation from income statement
        stock_based_comp = []
        try:
            cash_flow = stock.cashflow
            if not cash_flow.empty and "Stock Based Compensation" in cash_flow.index:
                sbc = cash_flow.loc["Stock Based Compensation"]
                for date, amount in sbc.items():
                    if pd.notna(amount):
                        stock_based_comp.append(
                            {"date": str(date.date()) if hasattr(date, "date") else str(date), "amount": float(amount)}
                        )
        except Exception as e:
            logger.debug(f"Could not fetch stock-based comp for {symbol}: {e}")

        return {"symbol": symbol, "key_executives": executives, "stock_based_compensation_history": stock_based_comp}
    except Exception as e:
        logger.error(f"Error fetching management compensation for {symbol}: {e}")
        return {"error": str(e), "symbol": symbol}


async def get_technical_indicators(symbol: str, period: str = "1y") -> dict[str, Any]:
    """Get technical indicators and price trends.

    Covers: Technical analysis, momentum, trends.

    Args:
        symbol: Stock ticker symbol
        period: Historical period (1mo, 3mo, 6mo, 1y, 2y, 5y)

    Returns:
        Dictionary with technical indicators
    """
    try:
        stock = yf.Ticker(symbol)
        hist = stock.history(period=period)

        if hist.empty:
            return {"error": "No price data available", "symbol": symbol}

        # Calculate technical indicators using pandas-ta
        try:
            import pandas_ta  # noqa: F401

            # Add technical indicators
            hist.ta.rsi(length=14, append=True)
            hist.ta.macd(append=True)
            hist.ta.bbands(append=True)
            hist.ta.sma(length=50, append=True)
            hist.ta.sma(length=200, append=True)

            latest = hist.iloc[-1]

            # Determine trend
            sma_50 = latest.get("SMA_50", None)
            sma_200 = latest.get("SMA_200", None)
            current_price = latest["Close"]

            trend = "neutral"
            if sma_50 and sma_200 and current_price:
                if current_price > sma_50 > sma_200:
                    trend = "strong_uptrend"
                elif current_price > sma_50:
                    trend = "uptrend"
                elif current_price < sma_50 < sma_200:
                    trend = "strong_downtrend"
                elif current_price < sma_50:
                    trend = "downtrend"

            # Calculate momentum
            returns_1m = ((hist["Close"].iloc[-1] / hist["Close"].iloc[-20]) - 1) * 100 if len(hist) >= 20 else None
            returns_3m = ((hist["Close"].iloc[-1] / hist["Close"].iloc[-60]) - 1) * 100 if len(hist) >= 60 else None
            returns_1y = ((hist["Close"].iloc[-1] / hist["Close"].iloc[0]) - 1) * 100 if len(hist) >= 200 else None

            return {
                "symbol": symbol,
                "current_price": float(latest["Close"]),
                "rsi": float(latest.get("RSI_14", 0)),
                "macd": float(latest.get("MACD_12_26_9", 0)),
                "macd_signal": float(latest.get("MACDs_12_26_9", 0)),
                "sma_50": float(sma_50) if sma_50 else None,
                "sma_200": float(sma_200) if sma_200 else None,
                "bb_upper": float(latest.get("BBU_5_2.0", 0)),
                "bb_lower": float(latest.get("BBL_5_2.0", 0)),
                "trend": trend,
                "returns_1m_pct": float(returns_1m) if returns_1m else None,
                "returns_3m_pct": float(returns_3m) if returns_3m else None,
                "returns_1y_pct": float(returns_1y) if returns_1y else None,
                "52_week_high": float(hist["High"].max()),
                "52_week_low": float(hist["Low"].min()),
                "avg_volume": float(hist["Volume"].mean()),
            }
        except ImportError:
            # Fallback if pandas-ta not available
            logger.warning("pandas-ta not available, using basic technical analysis")

            latest = hist.iloc[-1]
            current_price = latest["Close"]

            # Simple moving averages
            sma_50 = hist["Close"].rolling(50).mean().iloc[-1] if len(hist) >= 50 else None
            sma_200 = hist["Close"].rolling(200).mean().iloc[-1] if len(hist) >= 200 else None

            # Basic RSI calculation
            delta = hist["Close"].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))

            return {
                "symbol": symbol,
                "current_price": float(current_price),
                "sma_50": float(sma_50) if sma_50 and not pd.isna(sma_50) else None,
                "sma_200": float(sma_200) if sma_200 and not pd.isna(sma_200) else None,
                "rsi": float(rsi.iloc[-1]) if not rsi.empty and not pd.isna(rsi.iloc[-1]) else None,
                "52_week_high": float(hist["High"].max()),
                "52_week_low": float(hist["Low"].min()),
                "avg_volume": float(hist["Volume"].mean()),
            }

    except Exception as e:
        logger.error(f"Error fetching technical indicators for {symbol}: {e}")
        return {"error": str(e), "symbol": symbol}


async def get_valuation_metrics(symbol: str) -> dict[str, Any]:
    """Get valuation metrics and historical comparison.

    Covers: Reasonable valuation, premium/discount justification.

    Args:
        symbol: Stock ticker symbol

    Returns:
        Dictionary with valuation metrics
    """
    try:
        stock = yf.Ticker(symbol)
        info = stock.info

        # Historical P/E calculation would require matching quarterly earnings with prices
        # Skipping for now as it requires complex date alignment

        return {
            "symbol": symbol,
            "current_price": info.get("currentPrice"),
            "market_cap": info.get("marketCap"),
            # Valuation multiples
            "trailing_pe": info.get("trailingPE"),
            "forward_pe": info.get("forwardPE"),
            "peg_ratio": info.get("pegRatio"),
            "price_to_book": info.get("priceToBook"),
            "price_to_sales": info.get("priceToSalesTrailing12Months"),
            "enterprise_to_revenue": info.get("enterpriseToRevenue"),
            "enterprise_to_ebitda": info.get("enterpriseToEbitda"),
            # Dividend metrics
            "dividend_yield": info.get("dividendYield"),
            "dividend_rate": info.get("dividendRate"),
            "payout_ratio": info.get("payoutRatio"),
            # Additional context
            "trailing_eps": info.get("trailingEps"),
            "forward_eps": info.get("forwardEps"),
            "book_value": info.get("bookValue"),
        }
    except Exception as e:
        logger.error(f"Error fetching valuation metrics for {symbol}: {e}")
        return {"error": str(e), "symbol": symbol}


async def get_financial_history(symbol: str, years: int = 5) -> dict[str, Any]:
    """Get multi-year financial history for trend analysis.

    Covers: Capital allocation track record, historical performance.

    Args:
        symbol: Stock ticker symbol
        years: Number of years of history to retrieve

    Returns:
        Dictionary with historical financial data
    """
    try:
        stock = yf.Ticker(symbol)

        # Get annual financials
        financials_history = []
        try:
            financials = stock.financials
            if not financials.empty:
                for date_col in financials.columns[:years]:
                    year_data = {"date": str(date_col.date()) if hasattr(date_col, "date") else str(date_col)}
                    for idx in financials.index:
                        value = financials.loc[idx, date_col]
                        if pd.notna(value):
                            year_data[str(idx)] = float(value)
                    financials_history.append(year_data)
        except Exception as e:
            logger.debug(f"Could not fetch financial history for {symbol}: {e}")

        return {"symbol": symbol, "annual_financials": financials_history}
    except Exception as e:
        logger.error(f"Error fetching financial history for {symbol}: {e}")
        return {"error": str(e), "symbol": symbol}


async def calculate_similarity(symbol1: str, symbol2: str) -> dict[str, Any]:
    """Calculate similarity score between two stocks based on multiple dimensions.

    Args:
        symbol1: First stock ticker
        symbol2: Second stock ticker

    Returns:
        Dictionary with similarity scores across different dimensions
    """
    try:
        # Get fundamentals for both stocks
        fund1 = await get_stock_fundamentals(symbol1)
        fund2 = await get_stock_fundamentals(symbol2)

        if "error" in fund1 or "error" in fund2:
            return {"error": "Could not fetch data for comparison", "symbol1": symbol1, "symbol2": symbol2}

        # Calculate similarity scores (0-100) for different dimensions
        scores = {}

        # Sector/Industry match
        scores["sector_match"] = 100 if fund1.get("sector") == fund2.get("sector") else 0
        scores["industry_match"] = 100 if fund1.get("industry") == fund2.get("industry") else 0

        # Financial metrics similarity (using percentage difference)
        def calc_similarity(val1, val2):
            if val1 is None or val2 is None or val1 == 0 or val2 == 0:
                return None
            pct_diff = abs((val1 - val2) / ((val1 + val2) / 2)) * 100
            return max(0, 100 - pct_diff)

        scores["roe_similarity"] = calc_similarity(fund1.get("roe"), fund2.get("roe"))
        scores["margin_similarity"] = calc_similarity(fund1.get("profit_margin"), fund2.get("profit_margin"))
        scores["growth_similarity"] = calc_similarity(fund1.get("revenue_growth"), fund2.get("revenue_growth"))

        # Calculate overall similarity (weighted average of available scores)
        valid_scores = [s for s in scores.values() if s is not None]
        overall_similarity = sum(valid_scores) / len(valid_scores) if valid_scores else 0

        return {
            "symbol1": symbol1,
            "symbol2": symbol2,
            "overall_similarity": round(overall_similarity, 2),
            "dimension_scores": scores,
        }
    except Exception as e:
        logger.error(f"Error calculating similarity: {e}")
        return {"error": str(e), "symbol1": symbol1, "symbol2": symbol2}


async def find_similar_companies(symbol: str, limit: int = 10) -> dict[str, Any]:
    """Find similar companies programmatically using sector/industry and financial metrics.

    Uses yfinance Sector/Industry classes to discover companies in same sector/industry,
    then filters and ranks by market cap and financial similarity.

    Args:
        symbol: Reference stock ticker symbol
        limit: Maximum number of similar companies to return (default 10)

    Returns:
        Dictionary with reference info and ranked list of similar companies
    """
    try:
        # Get reference stock fundamentals
        ref_fund = await get_stock_fundamentals(symbol)
        if "error" in ref_fund:
            return {"error": f"Could not fetch data for {symbol}", "symbol": symbol}

        ref_sector = ref_fund.get("sector", "N/A")
        ref_industry = ref_fund.get("industry", "N/A")
        ref_market_cap = ref_fund.get("market_cap")

        if ref_sector == "N/A" or not ref_market_cap:
            return {"error": "Reference stock missing sector or market cap data", "symbol": symbol}

        # Get candidate companies from sector/industry
        candidates = []

        try:
            # Try to get industry companies first (more specific)
            stock = yf.Ticker(symbol)
            industry_key = stock.info.get("industryKey")

            if industry_key:
                try:
                    industry = yf.Industry(industry_key)
                    top_companies = industry.top_companies
                    if not top_companies.empty:
                        # Symbols are in the DataFrame index, not a column
                        candidates.extend(top_companies.index.tolist())
                        logger.info(f"Found {len(candidates)} companies from industry {industry_key}")
                except Exception as e:
                    logger.debug(f"Could not fetch industry companies: {e}")

            # Also get sector companies for broader coverage
            sector_key = stock.info.get("sectorKey")
            if sector_key:
                try:
                    sector = yf.Sector(sector_key)
                    top_companies = sector.top_companies
                    if not top_companies.empty:
                        # Symbols are in the DataFrame index, not a column
                        sector_symbols = top_companies.index.tolist()
                        # Add sector companies not already in candidates
                        candidates.extend([s for s in sector_symbols if s not in candidates])
                        logger.info(f"Total {len(candidates)} companies after adding sector {sector_key}")
                except Exception as e:
                    logger.debug(f"Could not fetch sector companies: {e}")

        except Exception as e:
            logger.warning(f"Error accessing Sector/Industry classes: {e}")

        # Remove reference symbol from candidates
        candidates = [c for c in candidates if c.upper() != symbol.upper()]

        if not candidates:
            return {
                "error": "No candidate companies found",
                "symbol": symbol,
                "reference_sector": ref_sector,
                "reference_industry": ref_industry,
            }

        # Calculate similarity scores for each candidate
        similar_companies = []
        for candidate_symbol in candidates[:50]:  # Limit to top 50 to avoid too many API calls
            try:
                # Get candidate fundamentals
                cand_fund = await get_stock_fundamentals(candidate_symbol)
                if "error" in cand_fund:
                    continue

                cand_market_cap = cand_fund.get("market_cap")
                if not cand_market_cap:
                    continue

                # Filter by market cap range (0.1x - 2x reference)
                market_cap_ratio = cand_market_cap / ref_market_cap
                if market_cap_ratio < 0.1 or market_cap_ratio > 2.0:
                    continue

                # Calculate multi-dimensional similarity score
                score = 0.0
                weights = {}

                # Sector match required (skip if different sector)
                if cand_fund.get("sector") != ref_sector:
                    continue
                score += 50
                weights["sector_match"] = True

                # Industry match (20 points bonus, not required)
                if cand_fund.get("industry") == ref_industry:
                    score += 20
                    weights["industry_match"] = True
                else:
                    weights["industry_match"] = False

                # Market cap similarity (20 points)
                # Score decreases as ratio diverges from 1.0
                mc_similarity = 20 * (1 - abs(np.log10(market_cap_ratio)) / np.log10(2.0))
                score += max(0, mc_similarity)
                weights["market_cap_similarity"] = round(mc_similarity, 2)

                # Financial metrics similarity
                def calc_metric_similarity(ref_val, cand_val, max_points):
                    if ref_val is None or cand_val is None or ref_val == 0 or cand_val == 0:
                        return 0
                    pct_diff = abs((ref_val - cand_val) / ((ref_val + cand_val) / 2))
                    return max(0, max_points * (1 - pct_diff))

                # Margin similarity (15 points)
                margin_sim = calc_metric_similarity(ref_fund.get("profit_margin"), cand_fund.get("profit_margin"), 15)
                score += margin_sim
                weights["margin_similarity"] = round(margin_sim, 2)

                # Growth similarity (10 points)
                growth_sim = calc_metric_similarity(ref_fund.get("revenue_growth"), cand_fund.get("revenue_growth"), 10)
                score += growth_sim
                weights["growth_similarity"] = round(growth_sim, 2)

                # ROE similarity (5 points)
                roe_sim = calc_metric_similarity(ref_fund.get("roe"), cand_fund.get("roe"), 5)
                score += roe_sim
                weights["roe_similarity"] = round(roe_sim, 2)

                similar_companies.append(
                    {
                        "symbol": candidate_symbol,
                        "name": cand_fund.get("company_name", "N/A"),
                        "similarity_score": round(score, 2),
                        "market_cap": cand_market_cap,
                        "sector": cand_fund.get("sector", "N/A"),
                        "industry": cand_fund.get("industry", "N/A"),
                        "weights": weights,
                    }
                )

            except Exception as e:
                logger.debug(f"Error processing candidate {candidate_symbol}: {e}")
                continue

        # Sort by similarity score descending
        similar_companies.sort(key=lambda x: x["similarity_score"], reverse=True)

        return {
            "reference_symbol": symbol,
            "reference_sector": ref_sector,
            "reference_industry": ref_industry,
            "reference_market_cap": ref_market_cap,
            "similar_companies": similar_companies[:limit],
            "total_candidates_analyzed": len(candidates),
            "total_matches_found": len(similar_companies),
        }

    except Exception as e:
        logger.error(f"Error finding similar companies for {symbol}: {e}")
        return {"error": str(e), "symbol": symbol}


def get_tool_definitions() -> list[dict[str, Any]]:
    """Get tool definitions for OpenAI API.

    Returns:
        List of tool definitions in OpenAI format
    """
    return [
        {"type": "web_search", "search_context_size": "high"},
        {
            "type": "function",
            "name": "get_stock_fundamentals",
            "description": "Get fundamental financial metrics including ROIC, ROE, margins, balance sheet, cash flow. Use this to understand profitability, capital efficiency, and financial health.",
            "parameters": {
                "type": "object",
                "properties": {"symbol": {"type": "string", "description": "Stock ticker symbol (e.g., AAPL, MSFT)"}},
                "required": ["symbol"],
            },
        },
        {
            "type": "function",
            "name": "get_insider_ownership",
            "description": "Get insider ownership percentage and recent insider buying/selling transactions. Use this to understand skin in the game and insider confidence.",
            "parameters": {
                "type": "object",
                "properties": {"symbol": {"type": "string", "description": "Stock ticker symbol"}},
                "required": ["symbol"],
            },
        },
        {
            "type": "function",
            "name": "get_institutional_holders",
            "description": "Get major institutional shareholders and recent changes in holdings. Use this to identify smart money and track their moves.",
            "parameters": {
                "type": "object",
                "properties": {"symbol": {"type": "string", "description": "Stock ticker symbol"}},
                "required": ["symbol"],
            },
        },
        {
            "type": "function",
            "name": "get_share_data",
            "description": "Get share count history and corporate buyback activity. Use this to understand dilution/accretion and capital allocation to shareholders.",
            "parameters": {
                "type": "object",
                "properties": {"symbol": {"type": "string", "description": "Stock ticker symbol"}},
                "required": ["symbol"],
            },
        },
        {
            "type": "function",
            "name": "get_management_compensation",
            "description": "Get executive compensation structure and stock-based comp. Use this to understand alignment and employee turnover incentives.",
            "parameters": {
                "type": "object",
                "properties": {"symbol": {"type": "string", "description": "Stock ticker symbol"}},
                "required": ["symbol"],
            },
        },
        {
            "type": "function",
            "name": "get_technical_indicators",
            "description": "Get technical indicators including RSI, MACD, moving averages, trends, momentum. Use this for entry/exit timing and trend analysis.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Stock ticker symbol"},
                    "period": {
                        "type": "string",
                        "description": "Historical period: 1mo, 3mo, 6mo, 1y, 2y, 5y",
                        "default": "1y",
                    },
                },
                "required": ["symbol"],
            },
        },
        {
            "type": "function",
            "name": "get_valuation_metrics",
            "description": "Get valuation multiples including P/E, P/B, P/S, EV/EBITDA, PEG ratio. Use this to assess if stock is fairly valued.",
            "parameters": {
                "type": "object",
                "properties": {"symbol": {"type": "string", "description": "Stock ticker symbol"}},
                "required": ["symbol"],
            },
        },
        {
            "type": "function",
            "name": "get_financial_history",
            "description": "Get multi-year financial statement history. Use this to analyze trends and track record over time.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Stock ticker symbol"},
                    "years": {"type": "integer", "description": "Number of years of history", "default": 5},
                },
                "required": ["symbol"],
            },
        },
        {
            "type": "function",
            "name": "calculate_similarity",
            "description": "Calculate multi-dimensional similarity score between two stocks based on sector, financials, growth, margins.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol1": {"type": "string", "description": "First stock ticker symbol"},
                    "symbol2": {"type": "string", "description": "Second stock ticker symbol"},
                },
                "required": ["symbol1", "symbol2"],
            },
        },
        {
            "type": "function",
            "name": "find_similar_companies",
            "description": "Find companies similar to reference stock using sector, industry, market cap, and financial metrics. Returns ranked list of most similar companies with similarity scores. Use this to discover comparable companies programmatically.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Reference stock ticker symbol (e.g., AAPL, MSFT)"},
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of similar companies to return (default 10)",
                        "default": 10,
                    },
                },
                "required": ["symbol"],
            },
        },
    ]


async def execute_tool(tool_name: str, tool_args: dict[str, Any]) -> dict[str, Any]:
    """Execute a tool by name.

    Args:
        tool_name: Name of the tool to execute
        tool_args: Arguments for the tool

    Returns:
        Tool execution result

    Raises:
        ValueError: If tool name is unknown
    """
    tool_map = {
        "get_stock_fundamentals": get_stock_fundamentals,
        "get_insider_ownership": get_insider_ownership,
        "get_institutional_holders": get_institutional_holders,
        "get_share_data": get_share_data,
        "get_management_compensation": get_management_compensation,
        "get_technical_indicators": get_technical_indicators,
        "get_valuation_metrics": get_valuation_metrics,
        "get_financial_history": get_financial_history,
        "calculate_similarity": calculate_similarity,
        "find_similar_companies": find_similar_companies,
    }

    if tool_name not in tool_map:
        raise ValueError(f"Unknown tool: {tool_name}")

    return await tool_map[tool_name](**tool_args)
