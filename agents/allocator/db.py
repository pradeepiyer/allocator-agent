"""ABOUTME: Database access layer for market data cache.
ABOUTME: Provides read-through cache with yfinance fallback for financial data.
"""

import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Database path
DB_PATH = Path(__file__).parent / "data" / "market.db"


@contextmanager
def get_db_connection():
    """Get database connection with automatic cleanup.

    Yields:
        sqlite3.Connection: Database connection
    """
    conn = None
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row  # Access columns by name
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()


# ============================================================================
# READ FUNCTIONS - Query cached data
# ============================================================================


def get_stock_info(symbol: str) -> dict[str, Any] | None:
    """Get stock information from database.

    Args:
        symbol: Stock ticker symbol

    Returns:
        Dictionary with stock info or None if not found
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM stocks WHERE symbol = ?
            """,
                (symbol,),
            )
            row = cursor.fetchone()

            if row:
                return dict(row)
            return None

    except Exception as e:
        logger.debug(f"Error fetching stock info for {symbol}: {e}")
        return None


def get_latest_fundamentals_annual(symbol: str) -> dict[str, Any] | None:
    """Get latest annual fundamentals from database.

    Args:
        symbol: Stock ticker symbol

    Returns:
        Dictionary with fundamentals or None if not found
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM fundamentals_annual
                WHERE symbol = ?
                ORDER BY fiscal_year DESC
                LIMIT 1
            """,
                (symbol,),
            )
            row = cursor.fetchone()

            if row:
                return dict(row)
            return None

    except Exception as e:
        logger.debug(f"Error fetching fundamentals for {symbol}: {e}")
        return None


def get_fundamentals_annual_history(symbol: str, years: int = 10) -> list[dict[str, Any]]:
    """Get multi-year annual fundamentals from database.

    Args:
        symbol: Stock ticker symbol
        years: Number of years to fetch

    Returns:
        List of fundamentals dictionaries, newest first
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM fundamentals_annual
                WHERE symbol = ?
                ORDER BY fiscal_year DESC
                LIMIT ?
            """,
                (symbol, years),
            )
            rows = cursor.fetchall()

            return [dict(row) for row in rows]

    except Exception as e:
        logger.debug(f"Error fetching financial history for {symbol}: {e}")
        return []


def get_price_history(symbol: str, start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
    """Get price history from database.

    Args:
        symbol: Stock ticker symbol
        start_date: Start date (YYYY-MM-DD) or None for all
        end_date: End date (YYYY-MM-DD) or None for all

    Returns:
        DataFrame with price history (empty if not found)
    """
    try:
        with get_db_connection() as conn:
            query = "SELECT * FROM price_history WHERE symbol = ?"
            params = [symbol]

            if start_date:
                query += " AND date >= ?"
                params.append(start_date)

            if end_date:
                query += " AND date <= ?"
                params.append(end_date)

            query += " ORDER BY date ASC"

            df = pd.read_sql_query(query, conn, params=params)

            if not df.empty:
                df["date"] = pd.to_datetime(df["date"])
                df.set_index("date", inplace=True)

            return df

    except Exception as e:
        logger.debug(f"Error fetching price history for {symbol}: {e}")
        return pd.DataFrame()


def get_insider_transactions(symbol: str, limit: int = 20) -> list[dict[str, Any]]:
    """Get recent insider transactions from database.

    Args:
        symbol: Stock ticker symbol
        limit: Maximum number of transactions to return

    Returns:
        List of transaction dictionaries
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM insider_transactions
                WHERE symbol = ?
                ORDER BY transaction_date DESC
                LIMIT ?
            """,
                (symbol, limit),
            )
            rows = cursor.fetchall()

            return [dict(row) for row in rows]

    except Exception as e:
        logger.debug(f"Error fetching insider transactions for {symbol}: {e}")
        return []


def get_ownership(symbol: str) -> dict[str, Any] | None:
    """Get latest ownership data from database.

    Args:
        symbol: Stock ticker symbol

    Returns:
        Dictionary with ownership data or None if not found
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM ownership
                WHERE symbol = ?
                ORDER BY as_of_date DESC
                LIMIT 1
            """,
                (symbol,),
            )
            row = cursor.fetchone()

            if row:
                return dict(row)
            return None

    except Exception as e:
        logger.debug(f"Error fetching ownership for {symbol}: {e}")
        return None


def get_institutional_holders(symbol: str, limit: int = 10) -> list[dict[str, Any]]:
    """Get institutional holders from database.

    Args:
        symbol: Stock ticker symbol
        limit: Maximum number of holders to return

    Returns:
        List of holder dictionaries
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM institutional_holders
                WHERE symbol = ?
                ORDER BY shares DESC
                LIMIT ?
            """,
                (symbol, limit),
            )
            rows = cursor.fetchall()

            return [dict(row) for row in rows]

    except Exception as e:
        logger.debug(f"Error fetching institutional holders for {symbol}: {e}")
        return []


def get_major_holders(symbol: str) -> dict[str, Any] | None:
    """Get major holders summary from database.

    Args:
        symbol: Stock ticker symbol

    Returns:
        Dictionary with major holders data or None if not found
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM major_holders
                WHERE symbol = ?
                ORDER BY as_of_date DESC
                LIMIT 1
            """,
                (symbol,),
            )
            row = cursor.fetchone()

            if row:
                return dict(row)
            return None

    except Exception as e:
        logger.debug(f"Error fetching major holders for {symbol}: {e}")
        return None


def get_quarterly_shares(symbol: str, limit: int = 20) -> list[dict[str, Any]]:
    """Get quarterly share count history from database.

    Args:
        symbol: Stock ticker symbol
        limit: Maximum number of quarters to return

    Returns:
        List of quarterly share data
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM quarterly_shares
                WHERE symbol = ?
                ORDER BY fiscal_year DESC, fiscal_quarter DESC
                LIMIT ?
            """,
                (symbol, limit),
            )
            rows = cursor.fetchall()

            return [dict(row) for row in rows]

    except Exception as e:
        logger.debug(f"Error fetching quarterly shares for {symbol}: {e}")
        return []


def get_buybacks(symbol: str) -> list[dict[str, Any]]:
    """Get buyback history from database.

    Args:
        symbol: Stock ticker symbol

    Returns:
        List of buyback records
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM buybacks
                WHERE symbol = ?
                ORDER BY fiscal_year DESC, fiscal_quarter DESC
            """,
                (symbol,),
            )
            rows = cursor.fetchall()

            return [dict(row) for row in rows]

    except Exception as e:
        logger.debug(f"Error fetching buybacks for {symbol}: {e}")
        return []


def get_executives(symbol: str, limit: int = 10) -> list[dict[str, Any]]:
    """Get executive data from database.

    Args:
        symbol: Stock ticker symbol
        limit: Maximum number of executives to return

    Returns:
        List of executive records
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM executives
                WHERE symbol = ?
                ORDER BY total_pay DESC
                LIMIT ?
            """,
                (symbol, limit),
            )
            rows = cursor.fetchall()

            return [dict(row) for row in rows]

    except Exception as e:
        logger.debug(f"Error fetching executives for {symbol}: {e}")
        return []


def get_stock_based_compensation(symbol: str) -> list[dict[str, Any]]:
    """Get stock-based compensation history from database.

    Args:
        symbol: Stock ticker symbol

    Returns:
        List of SBC records
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM stock_based_compensation
                WHERE symbol = ?
                ORDER BY fiscal_year DESC
            """,
                (symbol,),
            )
            rows = cursor.fetchall()

            return [dict(row) for row in rows]

    except Exception as e:
        logger.debug(f"Error fetching SBC for {symbol}: {e}")
        return []


# ============================================================================
# WRITE FUNCTIONS - Populate cache on miss
# ============================================================================


def write_stock_info(symbol: str, data: dict[str, Any]) -> None:
    """Write stock information to database.

    Args:
        symbol: Stock ticker symbol
        data: Dictionary with stock info fields
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO stocks
                (symbol, name, sector, industry, market_cap, description, last_updated,
                 beta, enterprise_value, quick_ratio, total_cash, total_debt, shares_short,
                 implied_shares_outstanding, dividend_yield, dividend_rate, payout_ratio,
                 forward_pe, forward_eps, peg_ratio)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    symbol,
                    data.get("name"),
                    data.get("sector"),
                    data.get("industry"),
                    data.get("market_cap"),
                    data.get("description"),
                    datetime.now(),
                    data.get("beta"),
                    data.get("enterprise_value"),
                    data.get("quick_ratio"),
                    data.get("total_cash"),
                    data.get("total_debt"),
                    data.get("shares_short"),
                    data.get("implied_shares_outstanding"),
                    data.get("dividend_yield"),
                    data.get("dividend_rate"),
                    data.get("payout_ratio"),
                    data.get("forward_pe"),
                    data.get("forward_eps"),
                    data.get("peg_ratio"),
                ),
            )
            conn.commit()
            logger.debug(f"Wrote stock info for {symbol} to database")

    except Exception as e:
        logger.warning(f"Error writing stock info for {symbol}: {e}")


def write_fundamentals_annual(symbol: str, year: int, data: dict[str, Any]) -> None:
    """Write annual fundamentals to database.

    Args:
        symbol: Stock ticker symbol
        year: Fiscal year
        data: Dictionary with fundamental fields
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO fundamentals_annual
                (symbol, fiscal_year, revenue, operating_income, net_income,
                 total_assets, total_liabilities, shareholders_equity,
                 operating_cash_flow, free_cash_flow, shares_outstanding,
                 roic, roe, roa, ebitda, profit_margin, operating_margin, gross_margin,
                 debt_to_equity, current_ratio)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    symbol,
                    year,
                    data.get("revenue"),
                    data.get("operating_income"),
                    data.get("net_income"),
                    data.get("total_assets"),
                    data.get("total_liabilities"),
                    data.get("shareholders_equity"),
                    data.get("operating_cash_flow"),
                    data.get("free_cash_flow"),
                    data.get("shares_outstanding"),
                    data.get("roic"),
                    data.get("roe"),
                    data.get("roa"),
                    data.get("ebitda"),
                    data.get("profit_margin"),
                    data.get("operating_margin"),
                    data.get("gross_margin"),
                    data.get("debt_to_equity"),
                    data.get("current_ratio"),
                ),
            )
            conn.commit()
            logger.debug(f"Wrote annual fundamentals for {symbol} {year} to database")

    except Exception as e:
        logger.warning(f"Error writing fundamentals for {symbol} {year}: {e}")


def write_price_history(symbol: str, prices: pd.DataFrame) -> None:
    """Write price history to database.

    Args:
        symbol: Stock ticker symbol
        prices: DataFrame with price data (index = date, columns = open/high/low/close/volume)
    """
    try:
        if prices.empty:
            return

        with get_db_connection() as conn:
            # Prepare data for insertion
            records = []
            for date, row in prices.iterrows():
                records.append(
                    (
                        symbol,
                        date.date() if hasattr(date, "date") else date,
                        float(row["Open"]) if pd.notna(row["Open"]) else None,
                        float(row["High"]) if pd.notna(row["High"]) else None,
                        float(row["Low"]) if pd.notna(row["Low"]) else None,
                        float(row["Close"]) if pd.notna(row["Close"]) else None,
                        int(row["Volume"]) if pd.notna(row["Volume"]) else None,
                        float(row["Close"]) if pd.notna(row["Close"]) else None,  # adj_close = close
                    )
                )

            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT OR IGNORE INTO price_history
                (symbol, date, open, high, low, close, volume, adj_close)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                records,
            )
            conn.commit()
            logger.debug(f"Wrote {len(records)} price records for {symbol} to database")

    except Exception as e:
        logger.warning(f"Error writing price history for {symbol}: {e}")


def write_insider_transactions(symbol: str, transactions: list[dict[str, Any]]) -> None:
    """Write insider transactions to database.

    Args:
        symbol: Stock ticker symbol
        transactions: List of transaction dictionaries
    """
    try:
        if not transactions:
            return

        with get_db_connection() as conn:
            cursor = conn.cursor()
            for txn in transactions:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO insider_transactions
                    (symbol, transaction_date, insider_name, insider_title,
                     transaction_type, shares, value, price_per_share)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        symbol,
                        txn.get("transaction_date"),
                        txn.get("insider_name"),
                        txn.get("insider_title"),
                        txn.get("transaction_type"),
                        txn.get("shares"),
                        txn.get("value"),
                        txn.get("price_per_share"),
                    ),
                )
            conn.commit()
            logger.debug(f"Wrote {len(transactions)} insider transactions for {symbol} to database")

    except Exception as e:
        logger.warning(f"Error writing insider transactions for {symbol}: {e}")


def write_ownership(symbol: str, data: dict[str, Any]) -> None:
    """Write ownership data to database.

    Args:
        symbol: Stock ticker symbol
        data: Dictionary with ownership fields
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO ownership
                (symbol, as_of_date, insider_ownership_pct, institutional_ownership_pct,
                 shares_outstanding, float_shares)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    symbol,
                    datetime.now().date(),
                    data.get("insider_ownership_pct"),
                    data.get("institutional_ownership_pct"),
                    data.get("shares_outstanding"),
                    data.get("float_shares"),
                ),
            )
            conn.commit()
            logger.debug(f"Wrote ownership data for {symbol} to database")

    except Exception as e:
        logger.warning(f"Error writing ownership for {symbol}: {e}")


def write_institutional_holders(symbol: str, holders: list[dict[str, Any]]) -> None:
    """Write institutional holders to database.

    Args:
        symbol: Stock ticker symbol
        holders: List of holder dictionaries
    """
    try:
        if not holders:
            return

        with get_db_connection() as conn:
            cursor = conn.cursor()
            # Clear existing holders for this symbol
            cursor.execute("DELETE FROM institutional_holders WHERE symbol = ?", (symbol,))

            for holder in holders:
                cursor.execute(
                    """
                    INSERT INTO institutional_holders
                    (symbol, holder_name, shares, date_reported, pct_out, value)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        symbol,
                        holder.get("holder_name"),
                        holder.get("shares"),
                        holder.get("date_reported"),
                        holder.get("pct_out"),
                        holder.get("value"),
                    ),
                )
            conn.commit()
            logger.debug(f"Wrote {len(holders)} institutional holders for {symbol} to database")

    except Exception as e:
        logger.warning(f"Error writing institutional holders for {symbol}: {e}")


def write_major_holders(symbol: str, data: dict[str, Any]) -> None:
    """Write major holders data to database.

    Args:
        symbol: Stock ticker symbol
        data: Dictionary with major holders fields
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO major_holders
                (symbol, insiders_percent, institutions_percent, institutions_float_percent,
                 institutions_count, as_of_date)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    symbol,
                    data.get("insiders_percent"),
                    data.get("institutions_percent"),
                    data.get("institutions_float_percent"),
                    data.get("institutions_count"),
                    datetime.now().date(),
                ),
            )
            conn.commit()
            logger.debug(f"Wrote major holders for {symbol} to database")

    except Exception as e:
        logger.warning(f"Error writing major holders for {symbol}: {e}")


def write_executives(symbol: str, executives: list[dict[str, Any]]) -> None:
    """Write executive data to database.

    Args:
        symbol: Stock ticker symbol
        executives: List of executive dictionaries
    """
    try:
        if not executives:
            return

        with get_db_connection() as conn:
            cursor = conn.cursor()
            # Clear existing executives for this symbol
            cursor.execute("DELETE FROM executives WHERE symbol = ?", (symbol,))

            for exec_data in executives:
                cursor.execute(
                    """
                    INSERT INTO executives
                    (symbol, name, title, total_pay, exercised_value, unexercised_value,
                     year_born, fiscal_year)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        symbol,
                        exec_data.get("name"),
                        exec_data.get("title"),
                        exec_data.get("total_pay"),
                        exec_data.get("exercised_value"),
                        exec_data.get("unexercised_value"),
                        exec_data.get("year_born"),
                        exec_data.get("fiscal_year", datetime.now().year),
                    ),
                )
            conn.commit()
            logger.debug(f"Wrote {len(executives)} executives for {symbol} to database")

    except Exception as e:
        logger.warning(f"Error writing executives for {symbol}: {e}")


def write_stock_based_compensation(symbol: str, sbc_data: list[dict[str, Any]]) -> None:
    """Write stock-based compensation to database.

    Args:
        symbol: Stock ticker symbol
        sbc_data: List of SBC dictionaries
    """
    try:
        if not sbc_data:
            return

        with get_db_connection() as conn:
            cursor = conn.cursor()
            for sbc in sbc_data:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO stock_based_compensation
                    (symbol, fiscal_year, sbc_amount)
                    VALUES (?, ?, ?)
                """,
                    (symbol, sbc.get("fiscal_year"), sbc.get("sbc_amount")),
                )
            conn.commit()
            logger.debug(f"Wrote {len(sbc_data)} SBC records for {symbol} to database")

    except Exception as e:
        logger.warning(f"Error writing SBC for {symbol}: {e}")


def write_buybacks(symbol: str, buybacks: list[dict[str, Any]]) -> None:
    """Write buyback data to database.

    Args:
        symbol: Stock ticker symbol
        buybacks: List of buyback dictionaries
    """
    try:
        if not buybacks:
            return

        with get_db_connection() as conn:
            cursor = conn.cursor()
            for buyback in buybacks:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO buybacks
                    (symbol, fiscal_year, fiscal_quarter, shares_repurchased, buyback_amount)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (
                        symbol,
                        buyback.get("fiscal_year"),
                        buyback.get("fiscal_quarter"),
                        buyback.get("shares_repurchased"),
                        buyback.get("buyback_amount"),
                    ),
                )
            conn.commit()
            logger.debug(f"Wrote {len(buybacks)} buyback records for {symbol} to database")

    except Exception as e:
        logger.warning(f"Error writing buybacks for {symbol}: {e}")


def write_quarterly_shares(symbol: str, shares_data: list[dict[str, Any]]) -> None:
    """Write quarterly share count to database.

    Args:
        symbol: Stock ticker symbol
        shares_data: List of quarterly share dictionaries
    """
    try:
        if not shares_data:
            return

        with get_db_connection() as conn:
            cursor = conn.cursor()
            for shares in shares_data:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO quarterly_shares
                    (symbol, fiscal_year, fiscal_quarter, shares_outstanding)
                    VALUES (?, ?, ?, ?)
                """,
                    (
                        symbol,
                        shares.get("fiscal_year"),
                        shares.get("fiscal_quarter"),
                        shares.get("shares_outstanding"),
                    ),
                )
            conn.commit()
            logger.debug(f"Wrote {len(shares_data)} quarterly share records for {symbol} to database")

    except Exception as e:
        logger.warning(f"Error writing quarterly shares for {symbol}: {e}")
