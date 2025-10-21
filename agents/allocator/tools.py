"""Financial data tools for Allocator Agent using yfinance."""

import logging
from typing import Any, Literal

import numpy as np
import pandas as pd
import yfinance as yf

from . import db

logger = logging.getLogger(__name__)

# Valid sector names from the database
ValidSector = Literal[
    "Technology",
    "Healthcare",
    "Financial Services",
    "Energy",
    "Consumer Cyclical",
    "Consumer Defensive",
    "Industrials",
    "Basic Materials",
    "Utilities",
    "Real Estate",
    "Communication Services",
]


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
    Uses database cache with yfinance fallback.

    Args:
        symbol: Stock ticker symbol

    Returns:
        Dictionary with fundamental metrics
    """
    try:
        # Try database first
        stock_info = db.get_stock_info(symbol)
        fundamentals = db.get_latest_fundamentals_annual(symbol)

        # Cache hit - use DB data with fresh price from yfinance
        if stock_info and fundamentals:
            logger.debug(f"Cache hit for {symbol}")

            # Get current price from yfinance for freshness
            current_price = None
            week_52_high = None
            week_52_low = None
            try:
                stock = yf.Ticker(symbol)
                info = stock.info
                current_price = info.get("currentPrice")
                week_52_high = info.get("fiftyTwoWeekHigh")
                week_52_low = info.get("fiftyTwoWeekLow")
            except Exception as e:
                logger.debug(f"Could not fetch current price for {symbol}: {e}")

            return {
                "symbol": symbol,
                "company_name": stock_info.get("name", "N/A"),
                "sector": stock_info.get("sector", "N/A"),
                "industry": stock_info.get("industry", "N/A"),
                "market_cap": stock_info.get("market_cap"),
                "enterprise_value": stock_info.get("enterprise_value"),
                # Profitability & Returns
                "roic": fundamentals.get("roic"),
                "roe": fundamentals.get("roe"),
                "roa": fundamentals.get("roa"),
                "profit_margin": fundamentals.get("profit_margin"),
                "operating_margin": fundamentals.get("operating_margin"),
                "gross_margin": fundamentals.get("gross_margin"),
                # Balance Sheet
                "debt_to_equity": fundamentals.get("debt_to_equity"),
                "current_ratio": fundamentals.get("current_ratio"),
                "quick_ratio": stock_info.get("quick_ratio"),
                "total_cash": stock_info.get("total_cash"),
                "total_debt": stock_info.get("total_debt"),
                # Cash Flow
                "free_cash_flow": fundamentals.get("free_cash_flow"),
                "operating_cash_flow": fundamentals.get("operating_cash_flow"),
                # Growth (not in DB - would need multi-year comparison)
                "revenue_growth": None,
                "earnings_growth": None,
                # Additional metrics
                "beta": stock_info.get("beta"),
                "52_week_high": week_52_high,
                "52_week_low": week_52_low,
                "current_price": current_price,
            }

        # Cache miss - fetch from yfinance and populate DB
        logger.debug(f"Cache miss for {symbol} - fetching from yfinance")

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

            # Get cash flow metrics from annual statement
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

        # Write to database cache
        try:
            # Write stock info
            db.write_stock_info(
                symbol,
                {
                    "name": info.get("longName"),
                    "sector": info.get("sector"),
                    "industry": info.get("industry"),
                    "market_cap": info.get("marketCap"),
                    "description": info.get("longBusinessSummary"),
                    "beta": info.get("beta"),
                    "enterprise_value": info.get("enterpriseValue"),
                    "quick_ratio": info.get("quickRatio"),
                    "total_cash": info.get("totalCash"),
                    "total_debt": info.get("totalDebt"),
                    "shares_short": info.get("sharesShort"),
                    "implied_shares_outstanding": info.get("impliedSharesOutstanding"),
                    "dividend_yield": info.get("dividendYield"),
                    "dividend_rate": info.get("dividendRate"),
                    "payout_ratio": info.get("payoutRatio"),
                    "forward_pe": info.get("forwardPE"),
                    "forward_eps": info.get("forwardEps"),
                    "peg_ratio": info.get("pegRatio"),
                },
            )

            # Write fundamentals if we have financial data
            if not financials.empty:
                latest_year = financials.columns[0].year
                # Extract key metrics for latest year
                db.write_fundamentals_annual(
                    symbol,
                    latest_year,
                    {
                        "revenue": None,  # Would need to extract from financials
                        "operating_income": operating_income,
                        "net_income": None,
                        "total_assets": total_assets,
                        "total_liabilities": None,
                        "shareholders_equity": None,
                        "operating_cash_flow": operating_cash_flow,
                        "free_cash_flow": free_cash_flow,
                        "shares_outstanding": None,
                        "roic": roic,
                        "roe": info.get("returnOnEquity"),
                        "roa": info.get("returnOnAssets"),
                        "ebitda": None,
                        "profit_margin": info.get("profitMargins"),
                        "operating_margin": info.get("operatingMargins"),
                        "gross_margin": info.get("grossMargins"),
                        "debt_to_equity": info.get("debtToEquity"),
                        "current_ratio": info.get("currentRatio"),
                    },
                )
        except Exception as e:
            logger.warning(f"Could not write {symbol} to database: {e}")

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
            # Cash Flow
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
    Uses database cache with yfinance fallback.

    Args:
        symbol: Stock ticker symbol

    Returns:
        Dictionary with insider ownership data
    """
    try:
        # Try database first
        ownership = db.get_ownership(symbol)
        transactions = db.get_insider_transactions(symbol, 20)

        # Cache hit - return DB data
        if ownership:
            logger.debug(f"Cache hit for {symbol}")

            # Format transactions to match current structure
            insider_transactions = []
            for txn in transactions:
                insider_transactions.append(
                    {
                        "date": txn.get("transaction_date"),
                        "insider": txn.get("insider_name"),
                        "shares": txn.get("shares"),
                        "value": txn.get("value"),
                        "transaction": txn.get("transaction_type"),
                    }
                )

            return {
                "symbol": symbol,
                "insider_ownership_pct": ownership.get("insider_ownership_pct"),
                "institutional_ownership_pct": ownership.get("institutional_ownership_pct"),
                "recent_transactions": insider_transactions,
                "shares_outstanding": ownership.get("shares_outstanding"),
                "float_shares": ownership.get("float_shares"),
                "shares_short": None,  # Not in ownership table, would need stock_info
            }

        # Cache miss - fetch from yfinance
        logger.debug(f"Cache miss for {symbol} - fetching from yfinance")

        stock = yf.Ticker(symbol)
        info = stock.info

        # Get insider transactions
        insider_transactions = []
        insider_txns_for_db = []
        try:
            insider_txns = stock.insider_transactions
            if not insider_txns.empty:
                recent_txns = insider_txns.head(20)
                for _, txn in recent_txns.iterrows():
                    transaction_date = txn.get("Start Date")
                    shares = txn.get("Shares")
                    value = txn.get("Value")

                    insider_transactions.append(
                        {
                            "date": _safe_date_str(transaction_date),
                            "insider": txn.get("Insider Trading"),
                            "shares": shares,
                            "value": value,
                            "transaction": txn.get("Transaction"),
                        }
                    )

                    # Prepare for DB write
                    price_per_share = None
                    if shares and value and shares != 0:
                        price_per_share = float(value / shares)

                    insider_txns_for_db.append(
                        {
                            "transaction_date": transaction_date.date()
                            if hasattr(transaction_date, "date")
                            else transaction_date,
                            "insider_name": txn.get("Insider Trading"),
                            "insider_title": None,
                            "transaction_type": txn.get("Transaction"),
                            "shares": int(shares) if shares else None,
                            "value": float(value) if value else None,
                            "price_per_share": price_per_share,
                        }
                    )
        except Exception as e:
            logger.debug(f"Could not fetch insider transactions for {symbol}: {e}")

        # Write to DB
        try:
            db.write_ownership(
                symbol,
                {
                    "insider_ownership_pct": info.get("heldPercentInsiders"),
                    "institutional_ownership_pct": info.get("heldPercentInstitutions"),
                    "shares_outstanding": info.get("sharesOutstanding"),
                    "float_shares": info.get("floatShares"),
                },
            )
            if insider_txns_for_db:
                db.write_insider_transactions(symbol, insider_txns_for_db)
        except Exception as e:
            logger.warning(f"Could not write ownership/insider data for {symbol}: {e}")

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
    Uses database cache with yfinance fallback.

    Args:
        symbol: Stock ticker symbol

    Returns:
        Dictionary with institutional holder data
    """
    try:
        # Try database first
        holders = db.get_institutional_holders(symbol, 10)
        major = db.get_major_holders(symbol)

        # Cache hit - return DB data
        if holders or major:
            logger.debug(f"Cache hit for {symbol}")

            institutional_holders = []
            for holder in holders:
                institutional_holders.append(
                    {
                        "holder": holder.get("holder_name"),
                        "shares": holder.get("shares"),
                        "date_reported": holder.get("date_reported"),
                        "pct_out": holder.get("pct_out"),
                        "value": holder.get("value"),
                    }
                )

            major_holders_summary = {}
            if major:
                # Format major holders to match yfinance structure
                major_holders_summary = {
                    "insiders_percent": major.get("insiders_percent"),
                    "institutions_percent": major.get("institutions_percent"),
                    "institutions_float_percent": major.get("institutions_float_percent"),
                    "institutions_count": major.get("institutions_count"),
                }

            return {
                "symbol": symbol,
                "institutional_holders": institutional_holders,
                "major_holders_summary": major_holders_summary,
            }

        # Cache miss - fetch from yfinance
        logger.debug(f"Cache miss for {symbol} - fetching from yfinance")

        stock = yf.Ticker(symbol)

        # Get institutional holders
        institutional_holders = []
        holders_for_db = []
        try:
            holders_df = stock.institutional_holders
            if not holders_df.empty:
                for _, holder in holders_df.iterrows():
                    date_reported = holder.get("Date Reported")
                    institutional_holders.append(
                        {
                            "holder": holder.get("Holder"),
                            "shares": holder.get("Shares"),
                            "date_reported": _safe_date_str(date_reported),
                            "pct_out": holder.get("% Out"),
                            "value": holder.get("Value"),
                        }
                    )
                    holders_for_db.append(
                        {
                            "holder_name": holder.get("Holder"),
                            "shares": holder.get("Shares"),
                            "date_reported": date_reported.date() if hasattr(date_reported, "date") else date_reported,
                            "pct_out": holder.get("% Out"),
                            "value": holder.get("Value"),
                        }
                    )
        except Exception as e:
            logger.debug(f"Could not fetch institutional holders for {symbol}: {e}")

        # Get major holders summary
        major_holders_summary = {}
        major_holders_for_db = {}
        try:
            major_holders = stock.major_holders
            if not major_holders.empty:
                major_holders_summary = major_holders.to_dict()
                # Parse major holders data for DB (simplified)
                major_holders_for_db = {
                    "insiders_percent": None,
                    "institutions_percent": None,
                    "institutions_float_percent": None,
                    "institutions_count": None,
                }
        except Exception as e:
            logger.debug(f"Could not fetch major holders for {symbol}: {e}")

        # Write to DB
        try:
            if holders_for_db:
                db.write_institutional_holders(symbol, holders_for_db)
            if major_holders_for_db:
                db.write_major_holders(symbol, major_holders_for_db)
        except Exception as e:
            logger.warning(f"Could not write institutional holders for {symbol}: {e}")

        return {
            "symbol": symbol,
            "institutional_holders": institutional_holders[:10],
            "major_holders_summary": major_holders_summary,
        }
    except Exception as e:
        logger.error(f"Error fetching institutional holders for {symbol}: {e}")
        return {"error": str(e), "symbol": symbol}


async def get_share_data(symbol: str) -> dict[str, Any]:
    """Get share count history and buyback activity.

    Covers: Share dilution/reduction, corporate buybacks.
    Uses database cache with yfinance fallback.

    Args:
        symbol: Stock ticker symbol

    Returns:
        Dictionary with share count and buyback data
    """
    try:
        # Try database first
        quarterly_shares = db.get_quarterly_shares(symbol, 20)
        buybacks = db.get_buybacks(symbol)
        stock_info = db.get_stock_info(symbol)

        # Cache hit - return DB data
        if quarterly_shares or buybacks:
            logger.debug(f"Cache hit for {symbol}")

            shares_history = []
            for qtr in quarterly_shares:
                shares_history.append(
                    {
                        "date": f"{qtr.get('fiscal_year')}-Q{qtr.get('fiscal_quarter')}",
                        "shares": qtr.get("shares_outstanding"),
                    }
                )

            buyback_history = []
            for buyback in buybacks:
                buyback_history.append(
                    {
                        "date": f"{buyback.get('fiscal_year')}-Q{buyback.get('fiscal_quarter')}",
                        "amount": buyback.get("buyback_amount"),
                    }
                )

            return {
                "symbol": symbol,
                "shares_outstanding": stock_info.get("implied_shares_outstanding") if stock_info else None,
                "float_shares": None,  # Not in quarterly_shares table
                "shares_history": shares_history,
                "buyback_history": buyback_history,
                "implied_shares_outstanding": stock_info.get("implied_shares_outstanding") if stock_info else None,
            }

        # Cache miss - fetch from yfinance
        logger.debug(f"Cache miss for {symbol} - fetching from yfinance")

        stock = yf.Ticker(symbol)
        info = stock.info

        # Get historical share count
        shares_history = []
        shares_for_db = []
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
                    if pd.notna(shares):
                        shares_for_db.append(
                            {
                                "fiscal_year": date.year,
                                "fiscal_quarter": (date.month - 1) // 3 + 1,
                                "shares_outstanding": float(shares),
                            }
                        )
        except Exception as e:
            logger.debug(f"Could not fetch share count history for {symbol}: {e}")

        # Get buyback data from cash flow
        buyback_history = []
        buybacks_for_db = []
        try:
            cash_flow = stock.cashflow
            if not cash_flow.empty:
                if "Repurchase Of Capital Stock" in cash_flow.index:
                    buybacks_series = cash_flow.loc["Repurchase Of Capital Stock"]
                    for date, amount in buybacks_series.items():
                        if pd.notna(amount) and amount != 0:
                            buyback_history.append(
                                {
                                    "date": str(date.date()) if hasattr(date, "date") else str(date),
                                    "amount": float(amount),
                                }
                            )
                            buybacks_for_db.append(
                                {
                                    "fiscal_year": date.year,
                                    "fiscal_quarter": (date.month - 1) // 3 + 1,
                                    "shares_repurchased": None,
                                    "buyback_amount": float(abs(amount)),
                                }
                            )
        except Exception as e:
            logger.debug(f"Could not fetch buyback history for {symbol}: {e}")

        # Write to DB
        try:
            if shares_for_db:
                db.write_quarterly_shares(symbol, shares_for_db)
            if buybacks_for_db:
                db.write_buybacks(symbol, buybacks_for_db)
        except Exception as e:
            logger.warning(f"Could not write share/buyback data for {symbol}: {e}")

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
    Uses database cache with yfinance fallback.

    Args:
        symbol: Stock ticker symbol

    Returns:
        Dictionary with compensation data
    """
    try:
        # Try database first
        executives_db = db.get_executives(symbol, 5)
        sbc_db = db.get_stock_based_compensation(symbol)

        # Cache hit - return DB data
        if executives_db or sbc_db:
            logger.debug(f"Cache hit for {symbol}")

            executives = []
            for exec_data in executives_db:
                executives.append(
                    {
                        "name": exec_data.get("name"),
                        "title": exec_data.get("title"),
                        "total_pay": exec_data.get("total_pay"),
                        "exercised_value": exec_data.get("exercised_value"),
                        "unexercised_value": exec_data.get("unexercised_value"),
                    }
                )

            stock_based_comp = []
            for sbc in sbc_db:
                stock_based_comp.append({"date": str(sbc.get("fiscal_year")), "amount": sbc.get("sbc_amount")})

            return {
                "symbol": symbol,
                "key_executives": executives,
                "stock_based_compensation_history": stock_based_comp,
            }

        # Cache miss - fetch from yfinance
        logger.debug(f"Cache miss for {symbol} - fetching from yfinance")

        stock = yf.Ticker(symbol)

        # Get key executives
        executives = []
        executives_for_db = []
        try:
            officers = stock.info.get("companyOfficers", [])
            for officer in officers[:5]:
                executives.append(
                    {
                        "name": officer.get("name"),
                        "title": officer.get("title"),
                        "total_pay": officer.get("totalPay"),
                        "exercised_value": officer.get("exercisedValue"),
                        "unexercised_value": officer.get("unexercisedValue"),
                    }
                )
                executives_for_db.append(
                    {
                        "name": officer.get("name"),
                        "title": officer.get("title"),
                        "total_pay": officer.get("totalPay"),
                        "exercised_value": officer.get("exercisedValue"),
                        "unexercised_value": officer.get("unexercisedValue"),
                        "year_born": officer.get("yearBorn"),
                        "fiscal_year": None,  # Not available in info
                    }
                )
        except Exception as e:
            logger.debug(f"Could not fetch executive data for {symbol}: {e}")

        # Get stock-based compensation
        stock_based_comp = []
        sbc_for_db = []
        try:
            cash_flow = stock.cashflow
            if not cash_flow.empty and "Stock Based Compensation" in cash_flow.index:
                sbc = cash_flow.loc["Stock Based Compensation"]
                for date, amount in sbc.items():
                    if pd.notna(amount):
                        stock_based_comp.append(
                            {"date": str(date.date()) if hasattr(date, "date") else str(date), "amount": float(amount)}
                        )
                        sbc_for_db.append({"fiscal_year": date.year, "sbc_amount": float(amount)})
        except Exception as e:
            logger.debug(f"Could not fetch stock-based comp for {symbol}: {e}")

        # Write to DB
        try:
            if executives_for_db:
                db.write_executives(symbol, executives_for_db)
            if sbc_for_db:
                db.write_stock_based_compensation(symbol, sbc_for_db)
        except Exception as e:
            logger.warning(f"Could not write compensation data for {symbol}: {e}")

        return {"symbol": symbol, "key_executives": executives, "stock_based_compensation_history": stock_based_comp}
    except Exception as e:
        logger.error(f"Error fetching management compensation for {symbol}: {e}")
        return {"error": str(e), "symbol": symbol}


async def get_technical_indicators(symbol: str, period: str = "1y") -> dict[str, Any]:
    """Get technical indicators and price trends.

    Covers: Technical analysis, momentum, trends.
    Uses database cache with yfinance for latest prices.

    Args:
        symbol: Stock ticker symbol
        period: Historical period (1mo, 3mo, 6mo, 1y, 2y, 5y)

    Returns:
        Dictionary with technical indicators
    """
    try:
        # Try database first - get historical data
        from datetime import datetime, timedelta

        # Map period to days
        period_days_map = {"1mo": 30, "3mo": 90, "6mo": 180, "1y": 365, "2y": 730, "5y": 1825}
        days = period_days_map.get(period, 365)

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        hist = db.get_price_history(symbol, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))

        # Cache hit but need to extend with latest data from yfinance
        if not hist.empty:
            logger.debug(f"Cache hit for {symbol} - extending with latest prices")

            # Get latest prices from yfinance to extend DB data
            try:
                stock = yf.Ticker(symbol)
                latest_hist = stock.history(period="5d")  # Get last few days to fill any gap

                if not latest_hist.empty:
                    # Merge latest data with DB data (avoid duplicates by date)
                    hist = pd.concat([hist, latest_hist]).loc[
                        ~pd.concat([hist, latest_hist]).index.duplicated(keep="last")
                    ]
                    hist = hist.sort_index()

                    # Write new prices to DB
                    try:
                        db.write_price_history(symbol, latest_hist)
                    except Exception as e:
                        logger.debug(f"Could not write latest prices for {symbol}: {e}")
            except Exception as e:
                logger.debug(f"Could not fetch latest prices for {symbol}: {e}")
        else:
            # Cache miss - fetch all from yfinance
            logger.debug(f"Cache miss for {symbol} - fetching from yfinance")

            stock = yf.Ticker(symbol)
            hist = stock.history(period=period)

            # Write to DB
            if not hist.empty:
                try:
                    db.write_price_history(symbol, hist)
                except Exception as e:
                    logger.warning(f"Could not write price history for {symbol}: {e}")

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
    Uses database cache with yfinance fallback.

    Args:
        symbol: Stock ticker symbol

    Returns:
        Dictionary with valuation metrics
    """
    try:
        # Try database first
        stock_info = db.get_stock_info(symbol)

        # Cache hit - use DB data with fresh price from yfinance
        if stock_info:
            logger.debug(f"Cache hit for {symbol}")

            # Get current price and some dynamic metrics from yfinance
            current_price = None
            trailing_pe = None
            price_to_book = None
            price_to_sales = None
            enterprise_to_revenue = None
            enterprise_to_ebitda = None
            trailing_eps = None
            book_value = None

            try:
                stock = yf.Ticker(symbol)
                info = stock.info
                current_price = info.get("currentPrice")
                trailing_pe = info.get("trailingPE")
                price_to_book = info.get("priceToBook")
                price_to_sales = info.get("priceToSalesTrailing12Months")
                enterprise_to_revenue = info.get("enterpriseToRevenue")
                enterprise_to_ebitda = info.get("enterpriseToEbitda")
                trailing_eps = info.get("trailingEps")
                book_value = info.get("bookValue")
            except Exception as e:
                logger.debug(f"Could not fetch current valuation metrics for {symbol}: {e}")

            return {
                "symbol": symbol,
                "current_price": current_price,
                "market_cap": stock_info.get("market_cap"),
                # Valuation multiples (mix of DB and live)
                "trailing_pe": trailing_pe,
                "forward_pe": stock_info.get("forward_pe"),
                "peg_ratio": stock_info.get("peg_ratio"),
                "price_to_book": price_to_book,
                "price_to_sales": price_to_sales,
                "enterprise_to_revenue": enterprise_to_revenue,
                "enterprise_to_ebitda": enterprise_to_ebitda,
                # Dividend metrics (from DB)
                "dividend_yield": stock_info.get("dividend_yield"),
                "dividend_rate": stock_info.get("dividend_rate"),
                "payout_ratio": stock_info.get("payout_ratio"),
                # Additional context (mix of DB and live)
                "trailing_eps": trailing_eps,
                "forward_eps": stock_info.get("forward_eps"),
                "book_value": book_value,
            }

        # Cache miss - fetch from yfinance
        logger.debug(f"Cache miss for {symbol} - fetching from yfinance")

        stock = yf.Ticker(symbol)
        info = stock.info

        # Write stock info to cache (reuse logic from get_stock_fundamentals)
        try:
            db.write_stock_info(
                symbol,
                {
                    "name": info.get("longName"),
                    "sector": info.get("sector"),
                    "industry": info.get("industry"),
                    "market_cap": info.get("marketCap"),
                    "description": info.get("longBusinessSummary"),
                    "beta": info.get("beta"),
                    "enterprise_value": info.get("enterpriseValue"),
                    "quick_ratio": info.get("quickRatio"),
                    "total_cash": info.get("totalCash"),
                    "total_debt": info.get("totalDebt"),
                    "shares_short": info.get("sharesShort"),
                    "implied_shares_outstanding": info.get("impliedSharesOutstanding"),
                    "dividend_yield": info.get("dividendYield"),
                    "dividend_rate": info.get("dividendRate"),
                    "payout_ratio": info.get("payoutRatio"),
                    "forward_pe": info.get("forwardPE"),
                    "forward_eps": info.get("forwardEps"),
                    "peg_ratio": info.get("pegRatio"),
                },
            )
        except Exception as e:
            logger.warning(f"Could not write {symbol} to database: {e}")

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
    Uses database cache with yfinance fallback.

    Args:
        symbol: Stock ticker symbol
        years: Number of years of history to retrieve

    Returns:
        Dictionary with historical financial data
    """
    try:
        # Try database first
        fundamentals_history = db.get_fundamentals_annual_history(symbol, years)

        # Cache hit - return DB data
        if fundamentals_history:
            logger.debug(f"Cache hit for {symbol}")

            # Convert to format matching current implementation
            annual_financials = []
            for row in fundamentals_history:
                year_data = {"date": str(row.get("fiscal_year"))}
                # Add all available metrics
                for key, value in row.items():
                    if key not in ["id", "symbol", "fiscal_year"] and value is not None:
                        year_data[key] = value
                annual_financials.append(year_data)

            return {"symbol": symbol, "annual_financials": annual_financials}

        # Cache miss - fetch from yfinance
        logger.debug(f"Cache miss for {symbol} - fetching from yfinance")

        stock = yf.Ticker(symbol)

        # Get annual financials
        financials_history_list = []
        try:
            financials = stock.financials
            if not financials.empty:
                for date_col in financials.columns[:years]:
                    year_data = {"date": str(date_col.date()) if hasattr(date_col, "date") else str(date_col)}
                    for idx in financials.index:
                        value = financials.loc[idx, date_col]
                        if pd.notna(value):
                            year_data[str(idx)] = float(value)
                    financials_history_list.append(year_data)
        except Exception as e:
            logger.debug(f"Could not fetch financial history for {symbol}: {e}")

        # Note: Writing full financial history to DB would require parsing all rows
        # For now, we rely on get_stock_fundamentals to populate the latest year
        # A full implementation would extract and write all years here

        return {"symbol": symbol, "annual_financials": financials_history_list}
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


async def screen_database_initial(
    min_roic: float | None = None,
    min_roe: float | None = None,
    min_profit_margin: float | None = None,
    max_debt_to_equity: float | None = None,
    min_market_cap: float | None = None,
    max_market_cap: float | None = None,
    sectors: list[ValidSector] | None = None,
    min_revenue_growth: float | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    """Initial screening with minimal data for fast candidate identification.

    Queries database with filters but returns only essential fields to minimize
    token usage. Use this for Stage 1 screening to identify promising candidates.

    NOTE: ROIC, ROE, and profit margin filters use 5-year historical averages to
    ensure consistent long-term performance, not just a single good year.

    Args:
        min_roic: Minimum average ROIC over last 5 years (as decimal, e.g., 0.15 for 15%)
        min_roe: Minimum average ROE over last 5 years (as decimal, e.g., 0.15 for 15%)
        min_profit_margin: Minimum average profit margin over last 5 years (as decimal, e.g., 0.10 for 10%)
        max_debt_to_equity: Maximum debt-to-equity ratio (latest year)
        min_market_cap: Minimum market cap in dollars
        max_market_cap: Maximum market cap in dollars
        sectors: List of sectors to include (e.g., ["Technology", "Healthcare"])
        min_revenue_growth: Minimum revenue CAGR over last 5 years (as decimal, e.g., 0.10 for 10% growth)
        limit: Maximum number of results to return

    Returns:
        Dictionary with list of stocks (minimal fields) and filters applied
    """
    try:
        with db.get_db_connection() as conn:
            # Screening query with 5-year historical averages for quality filters
            # This ensures we only surface companies with proven long-term track records
            query = """
                WITH historical_metrics AS (
                    SELECT
                        symbol,
                        AVG(roic) as avg_roic_5yr,
                        AVG(roe) as avg_roe_5yr,
                        AVG(profit_margin) as avg_profit_margin_5yr,
                        -- Revenue CAGR: (latest / oldest)^(1/(years-1)) - 1
                        CASE
                            WHEN MIN(revenue) > 0 AND MAX(revenue) > 0 AND COUNT(*) > 1
                            THEN POWER(MAX(revenue) * 1.0 / MIN(revenue), 1.0 / (COUNT(*) - 1)) - 1
                            ELSE NULL
                        END as revenue_cagr_5yr,
                        COUNT(*) as years_of_data
                    FROM fundamentals_annual
                    WHERE fiscal_year >= 2020
                    GROUP BY symbol
                    HAVING COUNT(*) >= 3  -- Require at least 3 years of historical data
                ),
                latest_metrics AS (
                    SELECT symbol, debt_to_equity, free_cash_flow, operating_cash_flow
                    FROM fundamentals_annual f1
                    WHERE fiscal_year = (
                        SELECT MAX(fiscal_year)
                        FROM fundamentals_annual f2
                        WHERE f2.symbol = f1.symbol
                    )
                )
                SELECT DISTINCT
                    s.symbol,
                    s.name,
                    s.sector,
                    s.market_cap,
                    h.avg_roic_5yr,
                    h.avg_roe_5yr,
                    h.avg_profit_margin_5yr,
                    h.revenue_cagr_5yr,
                    l.debt_to_equity,
                    l.free_cash_flow,
                    l.operating_cash_flow,
                    o.insider_ownership_pct,
                    o.institutional_ownership_pct
                FROM stocks s
                INNER JOIN historical_metrics h ON s.symbol = h.symbol
                LEFT JOIN latest_metrics l ON s.symbol = l.symbol
                LEFT JOIN (
                    SELECT symbol, insider_ownership_pct, institutional_ownership_pct
                    FROM ownership o1
                    WHERE as_of_date = (
                        SELECT MAX(as_of_date)
                        FROM ownership o2
                        WHERE o2.symbol = o1.symbol
                    )
                ) o ON s.symbol = o.symbol
                WHERE 1=1
            """

            params = []

            # Add filters - use 5-year averages for quality metrics
            if min_roic is not None:
                query += " AND h.avg_roic_5yr >= ?"
                params.append(min_roic)

            if min_roe is not None:
                query += " AND h.avg_roe_5yr >= ?"
                params.append(min_roe)

            if min_profit_margin is not None:
                query += " AND h.avg_profit_margin_5yr >= ?"
                params.append(min_profit_margin)

            if max_debt_to_equity is not None:
                query += " AND (l.debt_to_equity <= ? OR l.debt_to_equity IS NULL)"
                params.append(max_debt_to_equity)

            if min_market_cap is not None:
                query += " AND s.market_cap >= ?"
                params.append(min_market_cap)

            if max_market_cap is not None:
                query += " AND s.market_cap <= ?"
                params.append(max_market_cap)

            if sectors:
                placeholders = ",".join("?" * len(sectors))
                query += f" AND s.sector IN ({placeholders})"
                params.extend(sectors)

            if min_revenue_growth is not None:
                query += " AND h.revenue_cagr_5yr >= ?"
                params.append(min_revenue_growth)

            # Order by historical averages (proven track record)
            query += " ORDER BY h.avg_roic_5yr DESC, h.avg_roe_5yr DESC LIMIT ?"
            params.append(limit)

            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()

            # Convert to dictionaries with Stage 1 fields (historical averages)
            stocks = []
            for row in rows:
                stocks.append(
                    {
                        "symbol": row["symbol"],
                        "name": row["name"],
                        "sector": row["sector"],
                        "market_cap": row["market_cap"],
                        "roic": row["avg_roic_5yr"],  # 5-year average
                        "roe": row["avg_roe_5yr"],  # 5-year average
                        "profit_margin": row["avg_profit_margin_5yr"],  # 5-year average
                        "revenue_cagr": row["revenue_cagr_5yr"],  # 5-year growth
                        "debt_to_equity": row["debt_to_equity"],  # Latest year
                        "free_cash_flow": row["free_cash_flow"],  # Latest year
                        "operating_cash_flow": row["operating_cash_flow"],  # Latest year
                        "insider_ownership_pct": row["insider_ownership_pct"],
                        "institutional_ownership_pct": row["institutional_ownership_pct"],
                    }
                )

            logger.info(f"Initial screening returned {len(stocks)} candidates with 5-year track records")

            return {
                "stocks": stocks,
                "total_found": len(stocks),
                "filters_applied": {
                    "min_roic": min_roic,
                    "min_roe": min_roe,
                    "min_profit_margin": min_profit_margin,
                    "max_debt_to_equity": max_debt_to_equity,
                    "min_market_cap": min_market_cap,
                    "max_market_cap": max_market_cap,
                    "sectors": sectors,
                    "min_revenue_growth": min_revenue_growth,
                },
            }

    except Exception as e:
        logger.error(f"Error in initial screening: {e}")
        return {"error": str(e), "stocks": []}


async def get_detailed_metrics(symbols: list[str]) -> dict[str, Any]:
    """Get detailed metrics for specific stock symbols.

    Fetches comprehensive financial data for finalist stocks identified in
    Stage 1 screening. Use this for Stage 2 detailed analysis.

    Args:
        symbols: List of stock ticker symbols to get details for

    Returns:
        Dictionary with detailed stock information for each symbol
    """
    try:
        if not symbols:
            return {"stocks": [], "total_found": 0}

        with db.get_db_connection() as conn:
            # Detailed query with all fields
            placeholders = ",".join("?" * len(symbols))
            query = f"""
                SELECT DISTINCT
                    s.symbol,
                    s.name,
                    s.sector,
                    s.industry,
                    s.market_cap,
                    f.roic,
                    f.roe,
                    f.profit_margin,
                    f.debt_to_equity,
                    f.current_ratio,
                    f.free_cash_flow,
                    f.operating_cash_flow,
                    o.insider_ownership_pct,
                    o.institutional_ownership_pct,
                    s.forward_pe,
                    s.peg_ratio,
                    s.beta
                FROM stocks s
                LEFT JOIN (
                    SELECT symbol, roic, roe, profit_margin, debt_to_equity,
                           current_ratio, free_cash_flow, operating_cash_flow
                    FROM fundamentals_annual f1
                    WHERE fiscal_year = (
                        SELECT MAX(fiscal_year)
                        FROM fundamentals_annual f2
                        WHERE f2.symbol = f1.symbol
                    )
                ) f ON s.symbol = f.symbol
                LEFT JOIN (
                    SELECT symbol, insider_ownership_pct, institutional_ownership_pct
                    FROM ownership o1
                    WHERE as_of_date = (
                        SELECT MAX(as_of_date)
                        FROM ownership o2
                        WHERE o2.symbol = o1.symbol
                    )
                ) o ON s.symbol = o.symbol
                WHERE s.symbol IN ({placeholders})
            """

            cursor = conn.cursor()
            cursor.execute(query, symbols)
            rows = cursor.fetchall()

            # Convert to detailed dictionaries
            stocks = []
            for row in rows:
                stocks.append(
                    {
                        "symbol": row["symbol"],
                        "name": row["name"],
                        "sector": row["sector"],
                        "industry": row["industry"],
                        "market_cap": row["market_cap"],
                        "roic": row["roic"],
                        "roe": row["roe"],
                        "profit_margin": row["profit_margin"],
                        "debt_to_equity": row["debt_to_equity"],
                        "current_ratio": row["current_ratio"],
                        "free_cash_flow": row["free_cash_flow"],
                        "operating_cash_flow": row["operating_cash_flow"],
                        "insider_ownership_pct": row["insider_ownership_pct"],
                        "institutional_ownership_pct": row["institutional_ownership_pct"],
                        "forward_pe": row["forward_pe"],
                        "peg_ratio": row["peg_ratio"],
                        "beta": row["beta"],
                    }
                )

            logger.info(f"Fetched detailed metrics for {len(stocks)} stocks")

            return {"stocks": stocks, "total_found": len(stocks)}

    except Exception as e:
        logger.error(f"Error fetching detailed metrics: {e}")
        return {"error": str(e), "stocks": []}


async def screen_database(
    min_roic: float | None = None,
    min_roe: float | None = None,
    min_profit_margin: float | None = None,
    max_debt_to_equity: float | None = None,
    min_market_cap: float | None = None,
    max_market_cap: float | None = None,
    sectors: list[str] | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    """Screen database for investment opportunities using quantitative filters.

    Queries fundamentals_annual, stocks, and ownership tables to find companies
    matching the specified criteria.

    Args:
        min_roic: Minimum ROIC (as decimal, e.g., 0.15 for 15%)
        min_roe: Minimum ROE (as decimal, e.g., 0.15 for 15%)
        min_profit_margin: Minimum profit margin (as decimal, e.g., 0.10 for 10%)
        max_debt_to_equity: Maximum debt-to-equity ratio
        min_market_cap: Minimum market cap in dollars
        max_market_cap: Maximum market cap in dollars
        sectors: List of sectors to include (e.g., ["Technology", "Healthcare"])
        limit: Maximum number of results to return

    Returns:
        Dictionary with list of stocks matching criteria and their key metrics
    """
    try:
        with db.get_db_connection() as conn:
            # Build query dynamically based on provided filters
            query = """
                SELECT DISTINCT
                    s.symbol,
                    s.name,
                    s.sector,
                    s.industry,
                    s.market_cap,
                    f.roic,
                    f.roe,
                    f.roa,
                    f.profit_margin,
                    f.operating_margin,
                    f.gross_margin,
                    f.debt_to_equity,
                    f.current_ratio,
                    f.free_cash_flow,
                    f.operating_cash_flow,
                    o.insider_ownership_pct,
                    o.institutional_ownership_pct,
                    s.forward_pe,
                    s.peg_ratio,
                    s.beta
                FROM stocks s
                LEFT JOIN (
                    SELECT symbol, roic, roe, roa, profit_margin, operating_margin, gross_margin,
                           debt_to_equity, current_ratio, free_cash_flow, operating_cash_flow
                    FROM fundamentals_annual f1
                    WHERE fiscal_year = (
                        SELECT MAX(fiscal_year)
                        FROM fundamentals_annual f2
                        WHERE f2.symbol = f1.symbol
                    )
                ) f ON s.symbol = f.symbol
                LEFT JOIN (
                    SELECT symbol, insider_ownership_pct, institutional_ownership_pct
                    FROM ownership o1
                    WHERE as_of_date = (
                        SELECT MAX(as_of_date)
                        FROM ownership o2
                        WHERE o2.symbol = o1.symbol
                    )
                ) o ON s.symbol = o.symbol
                WHERE 1=1
            """

            params = []

            # Add filters
            if min_roic is not None:
                query += " AND f.roic >= ?"
                params.append(min_roic)

            if min_roe is not None:
                query += " AND f.roe >= ?"
                params.append(min_roe)

            if min_profit_margin is not None:
                query += " AND f.profit_margin >= ?"
                params.append(min_profit_margin)

            if max_debt_to_equity is not None:
                query += " AND (f.debt_to_equity <= ? OR f.debt_to_equity IS NULL)"
                params.append(max_debt_to_equity)

            if min_market_cap is not None:
                query += " AND s.market_cap >= ?"
                params.append(min_market_cap)

            if max_market_cap is not None:
                query += " AND s.market_cap <= ?"
                params.append(max_market_cap)

            if sectors:
                placeholders = ",".join("?" * len(sectors))
                query += f" AND s.sector IN ({placeholders})"
                params.extend(sectors)

            # Order by ROIC descending (prioritize capital efficiency)
            query += " ORDER BY f.roic DESC, f.roe DESC LIMIT ?"
            params.append(limit)

            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()

            # Convert to list of dictionaries
            stocks = []
            for row in rows:
                stocks.append(
                    {
                        "symbol": row["symbol"],
                        "name": row["name"],
                        "sector": row["sector"],
                        "industry": row["industry"],
                        "market_cap": row["market_cap"],
                        "roic": row["roic"],
                        "roe": row["roe"],
                        "roa": row["roa"],
                        "profit_margin": row["profit_margin"],
                        "operating_margin": row["operating_margin"],
                        "gross_margin": row["gross_margin"],
                        "debt_to_equity": row["debt_to_equity"],
                        "current_ratio": row["current_ratio"],
                        "free_cash_flow": row["free_cash_flow"],
                        "operating_cash_flow": row["operating_cash_flow"],
                        "insider_ownership_pct": row["insider_ownership_pct"],
                        "institutional_ownership_pct": row["institutional_ownership_pct"],
                        "forward_pe": row["forward_pe"],
                        "peg_ratio": row["peg_ratio"],
                        "beta": row["beta"],
                    }
                )

            return {
                "stocks": stocks,
                "total_found": len(stocks),
                "filters_applied": {
                    "min_roic": min_roic,
                    "min_roe": min_roe,
                    "min_profit_margin": min_profit_margin,
                    "max_debt_to_equity": max_debt_to_equity,
                    "min_market_cap": min_market_cap,
                    "max_market_cap": max_market_cap,
                    "sectors": sectors,
                },
            }

    except Exception as e:
        logger.error(f"Error screening database: {e}")
        return {"error": str(e), "stocks": []}


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
        {
            "type": "function",
            "name": "screen_database_initial",
            "description": "STAGE 1: Initial screening with 5-year historical track records. Returns stocks with proven long-term performance (not just one good year). Fields: symbol, name, sector, market_cap, roic (5yr avg), roe (5yr avg), profit_margin (5yr avg), revenue_cagr (5yr), debt_to_equity, free_cash_flow, operating_cash_flow, insider_ownership_pct, institutional_ownership_pct. Requires minimum 3 years of historical data. Use this first to get a quality pool of 25-50 finalists.",
            "parameters": {
                "type": "object",
                "properties": {
                    "min_roic": {
                        "type": "number",
                        "description": "Minimum average ROIC over last 5 years as decimal (e.g., 0.15 for 15% avg). Filters for consistent capital efficiency.",
                    },
                    "min_roe": {
                        "type": "number",
                        "description": "Minimum average ROE over last 5 years as decimal (e.g., 0.15 for 15% avg). Filters for consistent profitability.",
                    },
                    "min_profit_margin": {
                        "type": "number",
                        "description": "Minimum average profit margin over last 5 years as decimal (e.g., 0.10 for 10% avg). Filters for consistent pricing power.",
                    },
                    "max_debt_to_equity": {
                        "type": "number",
                        "description": "Maximum debt-to-equity ratio (e.g., 0.5 for low debt, 1.0 for moderate debt).",
                    },
                    "min_market_cap": {
                        "type": "number",
                        "description": "Minimum market cap in dollars (e.g., 500000000 for $500M).",
                    },
                    "max_market_cap": {
                        "type": "number",
                        "description": "Maximum market cap in dollars (e.g., 500000000000 for $500B).",
                    },
                    "sectors": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": [
                                "Technology",
                                "Healthcare",
                                "Financial Services",
                                "Energy",
                                "Consumer Cyclical",
                                "Consumer Defensive",
                                "Industrials",
                                "Basic Materials",
                                "Utilities",
                                "Real Estate",
                                "Communication Services",
                            ],
                        },
                        "description": "List of sectors to filter by. Valid sectors: Technology, Healthcare, Financial Services, Energy, Consumer Cyclical, Consumer Defensive, Industrials, Basic Materials, Utilities, Real Estate, Communication Services.",
                    },
                    "min_revenue_growth": {
                        "type": "number",
                        "description": "Minimum revenue CAGR (compound annual growth rate) over last 5 years as decimal (e.g., 0.10 for 10% growth, 0.15 for 15% growth). Filters for growing companies.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results (default 50, can go up to 100 for broader pool)",
                        "default": 50,
                    },
                },
                "required": [],
            },
        },
        {
            "type": "function",
            "name": "get_detailed_metrics",
            "description": "STAGE 2: Get comprehensive metrics for specific stocks identified as finalists. Returns detailed financial data including debt, ownership, cash flow, valuation multiples. Use this after initial screening to analyze top candidates.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbols": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of stock ticker symbols to get detailed metrics for (e.g., ['NVDA', 'AAPL', 'MSFT'])",
                    }
                },
                "required": ["symbols"],
            },
        },
        {
            "type": "function",
            "name": "screen_database",
            "description": "DEPRECATED: Single-stage screening (use screen_database_initial + get_detailed_metrics instead for better performance). Returns stocks with all fields in one query.",
            "parameters": {
                "type": "object",
                "properties": {
                    "min_roic": {
                        "type": "number",
                        "description": "Minimum ROIC as decimal (e.g., 0.15 for 15%). High ROIC indicates efficient capital allocation.",
                    },
                    "min_roe": {
                        "type": "number",
                        "description": "Minimum ROE as decimal (e.g., 0.15 for 15%). High ROE shows strong returns to shareholders.",
                    },
                    "min_profit_margin": {
                        "type": "number",
                        "description": "Minimum profit margin as decimal (e.g., 0.10 for 10%). High margins indicate pricing power.",
                    },
                    "max_debt_to_equity": {
                        "type": "number",
                        "description": "Maximum debt-to-equity ratio (e.g., 1.0). Lower is better for balance sheet strength.",
                    },
                    "min_market_cap": {
                        "type": "number",
                        "description": "Minimum market cap in dollars (e.g., 1000000000 for $1B).",
                    },
                    "max_market_cap": {
                        "type": "number",
                        "description": "Maximum market cap in dollars (e.g., 500000000000 for $500B).",
                    },
                    "sectors": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of sectors to include (e.g., ['Technology', 'Healthcare']). Omit to search all sectors.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return (default 50)",
                        "default": 50,
                    },
                },
                "required": [],
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
        "screen_database_initial": screen_database_initial,
        "get_detailed_metrics": get_detailed_metrics,
        "screen_database": screen_database,
    }

    if tool_name not in tool_map:
        raise ValueError(f"Unknown tool: {tool_name}")

    return await tool_map[tool_name](**tool_args)
