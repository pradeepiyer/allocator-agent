"""Refresh existing market database with latest data.

Updates price history, insider transactions, and ownership data.
Faster than full download since it only updates recent data.

Usage:
    # Refresh all stocks
    uv run python scripts/refresh_market_data.py

    # Refresh specific symbols
    uv run python scripts/refresh_market_data.py --symbols AAPL MSFT GOOGL

    # Refresh S&P 500 only
    uv run python scripts/refresh_market_data.py --index sp500

Estimated time: 30-60 minutes for all stocks
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

            # 1. Update company info (market cap can change)
            company_data = {
                "symbol": symbol,
                "market_cap": info.get("marketCap"),
                "last_updated": datetime.now(),
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

            return {
                "success": True,
                "symbol": symbol,
                "company": company_data,
                "price_history": price_data,
                "insider_transactions": insider_data,
                "ownership": ownership_data,
            }

        except Exception as e:
            return {"success": False, "symbol": symbol, "error": str(e)}

    def save_to_database(self, stock_data: dict):
        """Save refreshed data to database.

        Args:
            stock_data: Dictionary with refreshed stock data
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # 1. Update company data
            cursor.execute(
                """
                UPDATE stocks
                SET market_cap = ?, last_updated = ?
                WHERE symbol = ?
            """,
                (
                    stock_data["company"]["market_cap"],
                    stock_data["company"]["last_updated"],
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
