"""Refresh existing market database with latest data.

Updates:
- Stock metadata (beta, enterprise value, valuation metrics, dividends, etc.)
- Price history (last 30 days)
- Insider transactions (last 6 months)
- Ownership data (current percentages)
- Institutional holders (current top holders)
- Major holders summary
- Executives (current officers and compensation)
- Quarterly shares (last 2 quarters)
- Quarterly fundamentals (last 2 quarters)

Faster than full download since it only updates recent/current data.

Usage:
    # Refresh all stocks
    uv run python scripts/refresh_market_data.py

    # Refresh specific symbols
    uv run python scripts/refresh_market_data.py --symbols AAPL MSFT GOOGL

    # Refresh S&P 500 only
    uv run python scripts/refresh_market_data.py --index sp500

Estimated time: 45-90 minutes for all stocks
"""

import argparse
import asyncio
import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf
from tqdm.asyncio import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class MarketDataRefresher:
    """Refresh existing market database with latest data."""

    def __init__(self, db_path: str):
        """Initialize refresher.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.success_count = 0
        self.failed_symbols = []

    async def refresh_stock(self, symbol: str) -> dict:
        """Refresh data for a single stock.

        Args:
            symbol: Stock ticker symbol

        Returns:
            Dictionary with refreshed data or error
        """
        try:
            stock = yf.Ticker(symbol)
            info = stock.info

            # 1. Update company info (including all metadata that can change)
            company_data = {
                "symbol": symbol,
                "market_cap": info.get("marketCap"),
                "last_updated": datetime.now(),
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
            }

            # 2. Get latest price data (last 30 days)
            price_data = []
            try:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=30)
                hist = stock.history(start=start_date, end=end_date)

                if not hist.empty:
                    for date, row in hist.iterrows():
                        price_data.append(
                            {
                                "symbol": symbol,
                                "date": date.date(),
                                "open": float(row["Open"]) if pd.notna(row["Open"]) else None,
                                "high": float(row["High"]) if pd.notna(row["High"]) else None,
                                "low": float(row["Low"]) if pd.notna(row["Low"]) else None,
                                "close": float(row["Close"]) if pd.notna(row["Close"]) else None,
                                "volume": int(row["Volume"]) if pd.notna(row["Volume"]) else None,
                                "adj_close": float(row["Close"]) if pd.notna(row["Close"]) else None,
                            }
                        )
            except Exception as e:
                logger.debug(f"{symbol}: Could not fetch recent prices - {e}")

            # 3. Get recent insider transactions (last 6 months)
            insider_data = []
            try:
                insider_txns = stock.insider_transactions

                if not insider_txns.empty:
                    # Filter for recent transactions
                    six_months_ago = datetime.now() - timedelta(days=180)

                    for _, txn in insider_txns.iterrows():
                        transaction_date = txn.get("Start Date")

                        # Skip if too old
                        if hasattr(transaction_date, "date"):
                            if transaction_date < six_months_ago:
                                continue

                        shares = txn.get("Shares")
                        value = txn.get("Value")

                        price_per_share = None
                        if shares and value and shares != 0:
                            price_per_share = float(value / shares)

                        insider_data.append(
                            {
                                "symbol": symbol,
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
                logger.debug(f"{symbol}: Could not fetch insider transactions - {e}")

            # 4. Update ownership data
            ownership_data = {
                "symbol": symbol,
                "as_of_date": datetime.now().date(),
                "insider_ownership_pct": info.get("heldPercentInsiders"),
                "institutional_ownership_pct": info.get("heldPercentInstitutions"),
                "shares_outstanding": info.get("sharesOutstanding"),
                "float_shares": info.get("floatShares"),
            }

            # 5. Refresh institutional holders (all current holders)
            institutional_holders = []
            try:
                inst_holders = stock.institutional_holders
                if not inst_holders.empty:
                    for _, holder in inst_holders.iterrows():
                        date_reported = holder.get("Date Reported")
                        institutional_holders.append(
                            {
                                "symbol": symbol,
                                "holder_name": holder.get("Holder"),
                                "shares": holder.get("Shares"),
                                "date_reported": date_reported.date()
                                if hasattr(date_reported, "date")
                                else date_reported,
                                "pct_out": holder.get("% Out"),
                                "value": holder.get("Value"),
                            }
                        )
            except Exception as e:
                logger.debug(f"{symbol}: Could not fetch institutional holders - {e}")

            # 6. Refresh major holders
            major_holders_data = {
                "symbol": symbol,
                "as_of_date": datetime.now().date(),
                "insiders_percent": None,
                "institutions_percent": None,
                "institutions_float_percent": None,
                "institutions_count": None,
            }
            try:
                major_holders = stock.major_holders
                if not major_holders.empty:
                    for idx, row in major_holders.iterrows():
                        desc = str(row.iloc[1]).lower() if len(row) > 1 else ""
                        value_str = str(row.iloc[0])

                        if "insider" in desc:
                            try:
                                major_holders_data["insiders_percent"] = (
                                    float(value_str.replace("%", "")) / 100 if "%" in value_str else float(value_str)
                                )
                            except:
                                pass
                        elif "institution" in desc and "float" in desc:
                            try:
                                major_holders_data["institutions_float_percent"] = (
                                    float(value_str.replace("%", "")) / 100 if "%" in value_str else float(value_str)
                                )
                            except:
                                pass
                        elif "institution" in desc:
                            try:
                                if "%" in value_str:
                                    major_holders_data["institutions_percent"] = float(value_str.replace("%", "")) / 100
                                else:
                                    major_holders_data["institutions_count"] = int(float(value_str))
                            except:
                                pass
            except Exception as e:
                logger.debug(f"{symbol}: Could not fetch major holders - {e}")

            # 7. Refresh executives (all current executives)
            executives = []
            try:
                officers = info.get("companyOfficers", [])
                if officers:
                    current_year = datetime.now().year
                    for officer in officers[:10]:
                        executives.append(
                            {
                                "symbol": symbol,
                                "name": officer.get("name"),
                                "title": officer.get("title"),
                                "total_pay": officer.get("totalPay"),
                                "exercised_value": officer.get("exercisedValue"),
                                "unexercised_value": officer.get("unexercisedValue"),
                                "year_born": officer.get("yearBorn"),
                                "fiscal_year": current_year,
                            }
                        )
            except Exception as e:
                logger.debug(f"{symbol}: Could not fetch executives - {e}")

            # 8. Refresh quarterly shares (last 2 quarters)
            quarterly_shares = []
            try:
                qtr_bs = stock.quarterly_balance_sheet
                if not qtr_bs.empty and "Ordinary Shares Number" in qtr_bs.index:
                    shares_series = qtr_bs.loc["Ordinary Shares Number"]
                    for date_col, shares in list(shares_series.items())[:2]:  # Last 2 quarters
                        if pd.notna(shares):
                            quarterly_shares.append(
                                {
                                    "symbol": symbol,
                                    "fiscal_year": date_col.year,
                                    "fiscal_quarter": (date_col.month - 1) // 3 + 1,
                                    "shares_outstanding": float(shares),
                                }
                            )
            except Exception as e:
                logger.debug(f"{symbol}: Could not fetch quarterly shares - {e}")

            # 9. Refresh quarterly fundamentals (last 2 quarters)
            quarterly_data = []
            try:
                qtr_financials = stock.quarterly_financials
                qtr_balance_sheet = stock.quarterly_balance_sheet
                qtr_cash_flow = stock.quarterly_cashflow

                if not qtr_financials.empty:
                    for date_col in qtr_financials.columns[:2]:  # Last 2 quarters
                        year = date_col.year
                        quarter = (date_col.month - 1) // 3 + 1

                        revenue = self._get_value(qtr_financials, "Total Revenue", date_col)
                        operating_income = self._get_value(qtr_financials, "Operating Income", date_col)
                        net_income = self._get_value(qtr_financials, "Net Income", date_col)
                        gross_profit = self._get_value(qtr_financials, "Gross Profit", date_col)

                        total_assets = self._get_value(qtr_balance_sheet, "Total Assets", date_col)
                        total_liabilities = self._get_value(
                            qtr_balance_sheet, "Total Liabilities Net Minority Interest", date_col
                        )
                        shareholders_equity = self._get_value(qtr_balance_sheet, "Stockholders Equity", date_col)
                        current_assets = self._get_value(qtr_balance_sheet, "Current Assets", date_col)
                        current_liabilities = self._get_value(qtr_balance_sheet, "Current Liabilities", date_col)

                        operating_cf = self._get_value(qtr_cash_flow, "Operating Cash Flow", date_col)
                        free_cf = self._get_value(qtr_cash_flow, "Free Cash Flow", date_col)

                        profit_margin = None
                        if net_income and revenue and revenue > 0:
                            profit_margin = float(net_income / revenue)

                        operating_margin = None
                        if operating_income and revenue and revenue > 0:
                            operating_margin = float(operating_income / revenue)

                        gross_margin = None
                        if gross_profit and revenue and revenue > 0:
                            gross_margin = float(gross_profit / revenue)

                        debt_to_equity = None
                        if total_liabilities and shareholders_equity and shareholders_equity > 0:
                            debt_to_equity = float(total_liabilities / shareholders_equity)

                        current_ratio = None
                        if current_assets and current_liabilities and current_liabilities > 0:
                            current_ratio = float(current_assets / current_liabilities)

                        quarterly_data.append(
                            {
                                "symbol": symbol,
                                "fiscal_year": year,
                                "fiscal_quarter": quarter,
                                "revenue": revenue,
                                "operating_income": operating_income,
                                "net_income": net_income,
                                "total_assets": total_assets,
                                "total_liabilities": total_liabilities,
                                "shareholders_equity": shareholders_equity,
                                "operating_cash_flow": operating_cf,
                                "free_cash_flow": free_cf,
                                "gross_profit": gross_profit,
                                "ebitda": None,
                                "profit_margin": profit_margin,
                                "operating_margin": operating_margin,
                                "gross_margin": gross_margin,
                                "debt_to_equity": debt_to_equity,
                                "current_ratio": current_ratio,
                            }
                        )
            except Exception as e:
                logger.debug(f"{symbol}: Could not fetch quarterly fundamentals - {e}")

            return {
                "success": True,
                "symbol": symbol,
                "company": company_data,
                "price_history": price_data,
                "insider_transactions": insider_data,
                "ownership": ownership_data,
                "institutional_holders": institutional_holders,
                "major_holders": major_holders_data,
                "executives": executives,
                "quarterly_shares": quarterly_shares,
                "quarterly_fundamentals": quarterly_data,
            }

        except Exception as e:
            return {"success": False, "symbol": symbol, "error": str(e)}

    def _get_value(self, df, row_name, col_name):
        """Safely get value from DataFrame."""
        try:
            if row_name in df.index and col_name in df.columns:
                val = df.loc[row_name, col_name]
                return float(val) if pd.notna(val) else None
        except:
            pass
        return None

    def save_to_database(self, stock_data: dict):
        """Save refreshed data to database.

        Args:
            stock_data: Dictionary with refreshed stock data
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # 1. Update company data (all metadata fields)
            cursor.execute(
                """
                UPDATE stocks
                SET market_cap = ?, last_updated = ?, beta = ?, enterprise_value = ?,
                    quick_ratio = ?, total_cash = ?, total_debt = ?, shares_short = ?,
                    implied_shares_outstanding = ?, dividend_yield = ?, dividend_rate = ?,
                    payout_ratio = ?, forward_pe = ?, forward_eps = ?, peg_ratio = ?
                WHERE symbol = ?
            """,
                (
                    stock_data["company"]["market_cap"],
                    stock_data["company"]["last_updated"],
                    stock_data["company"]["beta"],
                    stock_data["company"]["enterprise_value"],
                    stock_data["company"]["quick_ratio"],
                    stock_data["company"]["total_cash"],
                    stock_data["company"]["total_debt"],
                    stock_data["company"]["shares_short"],
                    stock_data["company"]["implied_shares_outstanding"],
                    stock_data["company"]["dividend_yield"],
                    stock_data["company"]["dividend_rate"],
                    stock_data["company"]["payout_ratio"],
                    stock_data["company"]["forward_pe"],
                    stock_data["company"]["forward_eps"],
                    stock_data["company"]["peg_ratio"],
                    stock_data["company"]["symbol"],
                ),
            )

            # 2. Add new price history
            for record in stock_data["price_history"]:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO price_history
                    (symbol, date, open, high, low, close, volume, adj_close)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        record["symbol"],
                        record["date"],
                        record["open"],
                        record["high"],
                        record["low"],
                        record["close"],
                        record["volume"],
                        record["adj_close"],
                    ),
                )

            # 3. Add new insider transactions
            for record in stock_data["insider_transactions"]:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO insider_transactions
                    (symbol, transaction_date, insider_name, insider_title,
                     transaction_type, shares, value, price_per_share)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        record["symbol"],
                        record["transaction_date"],
                        record["insider_name"],
                        record["insider_title"],
                        record["transaction_type"],
                        record["shares"],
                        record["value"],
                        record["price_per_share"],
                    ),
                )

            # 4. Update ownership
            ownership = stock_data["ownership"]
            cursor.execute(
                """
                INSERT OR REPLACE INTO ownership
                (symbol, as_of_date, insider_ownership_pct, institutional_ownership_pct,
                 shares_outstanding, float_shares)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    ownership["symbol"],
                    ownership["as_of_date"],
                    ownership["insider_ownership_pct"],
                    ownership["institutional_ownership_pct"],
                    ownership["shares_outstanding"],
                    ownership["float_shares"],
                ),
            )

            # 5. Refresh institutional holders (delete old, insert new)
            cursor.execute("DELETE FROM institutional_holders WHERE symbol = ?", (stock_data["company"]["symbol"],))
            for record in stock_data["institutional_holders"]:
                cursor.execute(
                    """
                    INSERT INTO institutional_holders
                    (symbol, holder_name, shares, date_reported, pct_out, value)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        record["symbol"],
                        record["holder_name"],
                        record["shares"],
                        record["date_reported"],
                        record["pct_out"],
                        record["value"],
                    ),
                )

            # 6. Update major holders
            major_holders = stock_data["major_holders"]
            cursor.execute(
                """
                INSERT OR REPLACE INTO major_holders
                (symbol, insiders_percent, institutions_percent, institutions_float_percent,
                 institutions_count, as_of_date)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    major_holders["symbol"],
                    major_holders["insiders_percent"],
                    major_holders["institutions_percent"],
                    major_holders["institutions_float_percent"],
                    major_holders["institutions_count"],
                    major_holders["as_of_date"],
                ),
            )

            # 7. Refresh executives (delete old for current year, insert new)
            current_year = datetime.now().year
            cursor.execute(
                "DELETE FROM executives WHERE symbol = ? AND fiscal_year = ?",
                (stock_data["company"]["symbol"], current_year),
            )
            for record in stock_data["executives"]:
                cursor.execute(
                    """
                    INSERT INTO executives
                    (symbol, name, title, total_pay, exercised_value, unexercised_value,
                     year_born, fiscal_year)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        record["symbol"],
                        record["name"],
                        record["title"],
                        record["total_pay"],
                        record["exercised_value"],
                        record["unexercised_value"],
                        record["year_born"],
                        record["fiscal_year"],
                    ),
                )

            # 8. Refresh quarterly shares (last 2 quarters)
            for record in stock_data["quarterly_shares"]:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO quarterly_shares
                    (symbol, fiscal_year, fiscal_quarter, shares_outstanding)
                    VALUES (?, ?, ?, ?)
                """,
                    (
                        record["symbol"],
                        record["fiscal_year"],
                        record["fiscal_quarter"],
                        record["shares_outstanding"],
                    ),
                )

            # 9. Refresh quarterly fundamentals (last 2 quarters)
            for record in stock_data["quarterly_fundamentals"]:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO fundamentals_quarterly
                    (symbol, fiscal_year, fiscal_quarter, revenue, operating_income, net_income,
                     total_assets, total_liabilities, shareholders_equity, operating_cash_flow,
                     free_cash_flow, gross_profit, ebitda, profit_margin, operating_margin,
                     gross_margin, debt_to_equity, current_ratio)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        record["symbol"],
                        record["fiscal_year"],
                        record["fiscal_quarter"],
                        record["revenue"],
                        record["operating_income"],
                        record["net_income"],
                        record["total_assets"],
                        record["total_liabilities"],
                        record["shareholders_equity"],
                        record["operating_cash_flow"],
                        record["free_cash_flow"],
                        record["gross_profit"],
                        record["ebitda"],
                        record["profit_margin"],
                        record["operating_margin"],
                        record["gross_margin"],
                        record["debt_to_equity"],
                        record["current_ratio"],
                    ),
                )

            conn.commit()

        except Exception as e:
            conn.rollback()
            logger.error(f"Error saving {stock_data.get('symbol')}: {e}")
            raise

        finally:
            conn.close()

    async def refresh_all(self, symbols: list[str], batch_size: int = 10):
        """Refresh data for all symbols.

        Args:
            symbols: List of ticker symbols
            batch_size: Number of stocks to process in parallel

        Returns:
            Dictionary with summary statistics
        """
        total = len(symbols)
        logger.info(f"Refreshing {total} stocks")
        logger.info("")

        # Process in batches
        with tqdm(total=total, desc="Refreshing", unit="stock") as pbar:
            for i in range(0, total, batch_size):
                batch = symbols[i : i + batch_size]

                # Refresh batch in parallel
                tasks = [self.refresh_stock(symbol) for symbol in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Save results
                for result in results:
                    if isinstance(result, Exception):
                        logger.error(f"Exception: {result}")
                        continue

                    if result.get("success"):
                        try:
                            self.save_to_database(result)
                            self.success_count += 1
                            pbar.set_postfix(
                                {"success": self.success_count, "failed": len(self.failed_symbols)},
                                refresh=False,
                            )
                        except Exception as e:
                            logger.error(f"Failed to save {result['symbol']}: {e}")
                            self.failed_symbols.append(result["symbol"])
                    else:
                        self.failed_symbols.append(result["symbol"])

                    pbar.update(1)

                # Rate limiting
                await asyncio.sleep(1)

        return {
            "total": total,
            "success": self.success_count,
            "failed": len(self.failed_symbols),
            "failed_symbols": self.failed_symbols,
        }


async def main():
    """Main execution."""
    parser = argparse.ArgumentParser(description="Refresh market database")
    parser.add_argument("--symbols", nargs="+", help="Specific symbols to refresh (e.g., AAPL MSFT)")
    parser.add_argument("--index", choices=["sp500", "russell2000"], help="Refresh specific index")
    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("Market Data Refresher")
    logger.info("=" * 80)
    logger.info("")

    # Get paths
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "agents" / "allocator" / "data"
    db_path = data_dir / "market.db"

    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        logger.error("Run download_market_data.py first to create database")
        return

    # Determine which symbols to refresh
    symbols = []

    if args.symbols:
        # Specific symbols provided
        symbols = args.symbols
        logger.info(f"Refreshing specific symbols: {', '.join(symbols)}")

    elif args.index:
        # Refresh specific index
        symbol_file = data_dir / f"{args.index}_symbols.json"
        if not symbol_file.exists():
            logger.error(f"Symbol file not found: {symbol_file}")
            return

        with open(symbol_file) as f:
            symbols = json.load(f)

        logger.info(f"Refreshing {args.index.upper()} ({len(symbols)} symbols)")

    else:
        # Refresh all stocks in database
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT symbol FROM stocks ORDER BY symbol")
        symbols = [row[0] for row in cursor.fetchall()]
        conn.close()

        logger.info(f"Refreshing all stocks in database ({len(symbols)} symbols)")

    logger.info("")

    # Initialize refresher
    refresher = MarketDataRefresher(str(db_path))

    logger.info("Starting refresh...")
    logger.info("")

    # Refresh all stocks
    start_time = datetime.now()
    results = await refresher.refresh_all(symbols, batch_size=10)
    elapsed = datetime.now() - start_time

    # Summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("Refresh Complete!")
    logger.info("=" * 80)
    logger.info(f"Total stocks: {results['total']}")
    logger.info(f"Success: {results['success']} ({100 * results['success'] / results['total']:.1f}%)")
    logger.info(f"Failed: {results['failed']}")
    logger.info(f"Elapsed time: {elapsed}")
    logger.info("")

    if results["failed_symbols"]:
        logger.info("Failed symbols:")
        for symbol in results["failed_symbols"][:20]:
            logger.info(f"  - {symbol}")
        if results["failed"] > 20:
            logger.info(f"  ... and {results['failed'] - 20} more")

    logger.info("")
    logger.info("Done! Database refreshed successfully.")


if __name__ == "__main__":
    asyncio.run(main())
