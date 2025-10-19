"""Download historical market data and build SQLite database.

Downloads 10 years of financial data for S&P 500 and Russell 2000 stocks.
Creates database at: agents/allocator/data/market.db

Usage:
    uv run python scripts/download_market_data.py

Estimated time: 4-5 hours for ~2500 stocks
"""

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


class MarketDataDownloader:
    """Download and store historical market data in SQLite database."""

    def __init__(self, db_path: str):
        """Initialize downloader.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.failed_symbols = []
        self.success_count = 0

    def setup_database(self):
        """Create database tables if they don't exist."""
        logger.info(f"Setting up database at {self.db_path}")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 1. Stocks table - Company master data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                symbol TEXT PRIMARY KEY,
                name TEXT,
                sector TEXT,
                industry TEXT,
                market_cap REAL,
                description TEXT,
                last_updated TIMESTAMP
            )
        """)

        # 2. Fundamentals annual - 10 years of annual financials
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fundamentals_annual (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                fiscal_year INTEGER,
                revenue REAL,
                operating_income REAL,
                net_income REAL,
                total_assets REAL,
                total_liabilities REAL,
                shareholders_equity REAL,
                operating_cash_flow REAL,
                free_cash_flow REAL,
                shares_outstanding REAL,
                roic REAL,
                roe REAL,
                profit_margin REAL,
                operating_margin REAL,
                gross_margin REAL,
                debt_to_equity REAL,
                current_ratio REAL,
                FOREIGN KEY (symbol) REFERENCES stocks(symbol),
                UNIQUE(symbol, fiscal_year)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fundamentals_symbol ON fundamentals_annual(symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fundamentals_year ON fundamentals_annual(fiscal_year)")

        # 3. Price history - 10 years of daily prices
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                date DATE,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                adj_close REAL,
                FOREIGN KEY (symbol) REFERENCES stocks(symbol),
                UNIQUE(symbol, date)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_symbol ON price_history(symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_date ON price_history(date)")

        # 4. Insider transactions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS insider_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                transaction_date DATE,
                insider_name TEXT,
                insider_title TEXT,
                transaction_type TEXT,
                shares INTEGER,
                value REAL,
                price_per_share REAL,
                FOREIGN KEY (symbol) REFERENCES stocks(symbol)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_insider_symbol ON insider_transactions(symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_insider_date ON insider_transactions(transaction_date)")

        # 5. Ownership
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ownership (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                as_of_date DATE,
                insider_ownership_pct REAL,
                institutional_ownership_pct REAL,
                shares_outstanding REAL,
                float_shares REAL,
                FOREIGN KEY (symbol) REFERENCES stocks(symbol),
                UNIQUE(symbol, as_of_date)
            )
        """)

        # 6. Buybacks - Share repurchase history
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS buybacks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                fiscal_year INTEGER,
                fiscal_quarter INTEGER,
                shares_repurchased REAL,
                buyback_amount REAL,
                FOREIGN KEY (symbol) REFERENCES stocks(symbol),
                UNIQUE(symbol, fiscal_year, fiscal_quarter)
            )
        """)

        conn.commit()
        conn.close()

        logger.info("Database schema created successfully")

    def calculate_roic(self, operating_income, total_assets, current_liabilities, tax_rate=0.21):
        """Calculate ROIC = NOPAT / Invested Capital.

        Args:
            operating_income: Operating income
            total_assets: Total assets
            current_liabilities: Current liabilities
            tax_rate: Effective tax rate (default 21%)

        Returns:
            ROIC as decimal (e.g., 0.25 = 25%) or None
        """
        if not operating_income or not total_assets or not current_liabilities:
            return None

        try:
            nopat = operating_income * (1 - tax_rate)
            invested_capital = total_assets - current_liabilities

            if invested_capital <= 0:
                return None

            return float(nopat / invested_capital)
        except:
            return None

    async def download_stock(self, symbol: str) -> dict:
        """Download all data for a single stock.

        Args:
            symbol: Stock ticker symbol

        Returns:
            Dictionary with all downloaded data or error
        """
        try:
            stock = yf.Ticker(symbol)
            info = stock.info

            # 1. Company info
            company_data = {
                "symbol": symbol,
                "name": info.get("longName"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "market_cap": info.get("marketCap"),
                "description": info.get("longBusinessSummary"),
                "last_updated": datetime.now(),
            }

            # 2. Annual financials (10 years)
            annual_data = []
            try:
                financials = stock.financials  # Annual income statement
                balance_sheet = stock.balance_sheet  # Annual balance sheet
                cash_flow = stock.cashflow  # Annual cash flow

                if not financials.empty and not balance_sheet.empty and not cash_flow.empty:
                    # Process up to 10 years
                    for i, date_col in enumerate(financials.columns[:10]):
                        year = date_col.year

                        # Extract metrics
                        revenue = self._get_value(financials, "Total Revenue", date_col)
                        operating_income = self._get_value(financials, "Operating Income", date_col)
                        net_income = self._get_value(financials, "Net Income", date_col)
                        gross_profit = self._get_value(financials, "Gross Profit", date_col)

                        total_assets = self._get_value(balance_sheet, "Total Assets", date_col)
                        total_liabilities = self._get_value(
                            balance_sheet, "Total Liabilities Net Minority Interest", date_col
                        )
                        shareholders_equity = self._get_value(balance_sheet, "Stockholders Equity", date_col)
                        current_liabilities = self._get_value(balance_sheet, "Current Liabilities", date_col)
                        current_assets = self._get_value(balance_sheet, "Current Assets", date_col)

                        operating_cf = self._get_value(cash_flow, "Operating Cash Flow", date_col)
                        free_cf = self._get_value(cash_flow, "Free Cash Flow", date_col)
                        shares_repurchased = self._get_value(cash_flow, "Repurchase Of Capital Stock", date_col)

                        shares_outstanding = self._get_value(balance_sheet, "Ordinary Shares Number", date_col)

                        # Calculate derived metrics
                        roic = self.calculate_roic(
                            operating_income,
                            total_assets,
                            current_liabilities,
                            info.get("effectiveTaxRate", 0.21),
                        )

                        roe = None
                        if net_income and shareholders_equity and shareholders_equity > 0:
                            roe = float(net_income / shareholders_equity)

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

                        annual_data.append(
                            {
                                "symbol": symbol,
                                "fiscal_year": year,
                                "revenue": revenue,
                                "operating_income": operating_income,
                                "net_income": net_income,
                                "total_assets": total_assets,
                                "total_liabilities": total_liabilities,
                                "shareholders_equity": shareholders_equity,
                                "operating_cash_flow": operating_cf,
                                "free_cash_flow": free_cf,
                                "shares_outstanding": shares_outstanding,
                                "roic": roic,
                                "roe": roe,
                                "profit_margin": profit_margin,
                                "operating_margin": operating_margin,
                                "gross_margin": gross_margin,
                                "debt_to_equity": debt_to_equity,
                                "current_ratio": current_ratio,
                            }
                        )
            except Exception as e:
                logger.debug(f"{symbol}: Could not fetch financials - {e}")

            # 3. Historical prices (10 years)
            price_data = []
            try:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=10 * 365)
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
                logger.debug(f"{symbol}: Could not fetch price history - {e}")

            # 4. Insider transactions
            insider_data = []
            try:
                insider_txns = stock.insider_transactions

                if not insider_txns.empty:
                    for _, txn in insider_txns.iterrows():
                        transaction_date = txn.get("Start Date")
                        shares = txn.get("Shares")
                        value = txn.get("Value")

                        # Calculate price per share
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
                                "insider_title": None,  # Not available in yfinance
                                "transaction_type": txn.get("Transaction"),
                                "shares": int(shares) if shares else None,
                                "value": float(value) if value else None,
                                "price_per_share": price_per_share,
                            }
                        )
            except Exception as e:
                logger.debug(f"{symbol}: Could not fetch insider transactions - {e}")

            # 5. Ownership data
            ownership_data = {
                "symbol": symbol,
                "as_of_date": datetime.now().date(),
                "insider_ownership_pct": info.get("heldPercentInsiders"),
                "institutional_ownership_pct": info.get("heldPercentInstitutions"),
                "shares_outstanding": info.get("sharesOutstanding"),
                "float_shares": info.get("floatShares"),
            }

            # 6. Buybacks from cash flow
            buyback_data = []
            try:
                quarterly_cf = stock.quarterly_cashflow

                if not quarterly_cf.empty and "Repurchase Of Capital Stock" in quarterly_cf.index:
                    buybacks = quarterly_cf.loc["Repurchase Of Capital Stock"]

                    for date_col, amount in buybacks.items():
                        if pd.notna(amount) and amount != 0:
                            buyback_data.append(
                                {
                                    "symbol": symbol,
                                    "fiscal_year": date_col.year,
                                    "fiscal_quarter": (date_col.month - 1) // 3 + 1,
                                    "shares_repurchased": None,  # Not directly available
                                    "buyback_amount": float(abs(amount)),  # Make positive
                                }
                            )
            except Exception as e:
                logger.debug(f"{symbol}: Could not fetch buyback data - {e}")

            return {
                "success": True,
                "symbol": symbol,
                "company": company_data,
                "annual_fundamentals": annual_data,
                "price_history": price_data,
                "insider_transactions": insider_data,
                "ownership": ownership_data,
                "buybacks": buyback_data,
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
        """Save downloaded data to SQLite database.

        Args:
            stock_data: Dictionary with all stock data
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # 1. Save company data
            cursor.execute(
                """
                INSERT OR REPLACE INTO stocks (symbol, name, sector, industry, market_cap, description, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    stock_data["company"]["symbol"],
                    stock_data["company"]["name"],
                    stock_data["company"]["sector"],
                    stock_data["company"]["industry"],
                    stock_data["company"]["market_cap"],
                    stock_data["company"]["description"],
                    stock_data["company"]["last_updated"],
                ),
            )

            # 2. Save annual fundamentals
            for record in stock_data["annual_fundamentals"]:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO fundamentals_annual
                    (symbol, fiscal_year, revenue, operating_income, net_income,
                     total_assets, total_liabilities, shareholders_equity,
                     operating_cash_flow, free_cash_flow, shares_outstanding,
                     roic, roe, profit_margin, operating_margin, gross_margin,
                     debt_to_equity, current_ratio)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        record["symbol"],
                        record["fiscal_year"],
                        record["revenue"],
                        record["operating_income"],
                        record["net_income"],
                        record["total_assets"],
                        record["total_liabilities"],
                        record["shareholders_equity"],
                        record["operating_cash_flow"],
                        record["free_cash_flow"],
                        record["shares_outstanding"],
                        record["roic"],
                        record["roe"],
                        record["profit_margin"],
                        record["operating_margin"],
                        record["gross_margin"],
                        record["debt_to_equity"],
                        record["current_ratio"],
                    ),
                )

            # 3. Save price history
            for record in stock_data["price_history"]:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO price_history
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

            # 4. Save insider transactions
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

            # 5. Save ownership
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

            # 6. Save buybacks
            for record in stock_data["buybacks"]:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO buybacks
                    (symbol, fiscal_year, fiscal_quarter, shares_repurchased, buyback_amount)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (
                        record["symbol"],
                        record["fiscal_year"],
                        record["fiscal_quarter"],
                        record["shares_repurchased"],
                        record["buyback_amount"],
                    ),
                )

            conn.commit()

        except Exception as e:
            conn.rollback()
            logger.error(f"Error saving {stock_data.get('symbol')}: {e}")
            raise

        finally:
            conn.close()

    async def download_all(self, symbols: list[str], batch_size: int = 10):
        """Download data for all symbols with parallel processing.

        Args:
            symbols: List of ticker symbols
            batch_size: Number of stocks to download in parallel

        Returns:
            Dictionary with summary statistics
        """
        total = len(symbols)
        logger.info(f"Starting download for {total} stocks")
        logger.info(f"Batch size: {batch_size}")
        logger.info(f"Estimated time: {total * 10 / 60:.1f} minutes")
        logger.info("")

        # Process in batches
        with tqdm(total=total, desc="Downloading", unit="stock") as pbar:
            for i in range(0, total, batch_size):
                batch = symbols[i : i + batch_size]

                # Download batch in parallel
                tasks = [self.download_stock(symbol) for symbol in batch]
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
                        logger.debug(f"✗ {result['symbol']} - {result.get('error')}")

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
    logger.info("=" * 80)
    logger.info("Market Data Downloader")
    logger.info("=" * 80)
    logger.info("")

    # Get paths
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "agents" / "allocator" / "data"
    db_path = data_dir / "market.db"

    # Load symbol lists
    sp500_file = data_dir / "sp500_symbols.json"
    russell2000_file = data_dir / "russell2000_symbols.json"

    if not sp500_file.exists() or not russell2000_file.exists():
        logger.error("Symbol files not found. Run download_symbols.py first.")
        logger.error(f"Expected files:")
        logger.error(f"  - {sp500_file}")
        logger.error(f"  - {russell2000_file}")
        return

    with open(sp500_file) as f:
        sp500_symbols = json.load(f)

    with open(russell2000_file) as f:
        russell2000_symbols = json.load(f)

    # Combine and deduplicate
    all_symbols = sorted(list(set(sp500_symbols + russell2000_symbols)))

    logger.info(f"Symbol lists loaded:")
    logger.info(f"  S&P 500: {len(sp500_symbols)} symbols")
    logger.info(f"  Russell 2000: {len(russell2000_symbols)} symbols")
    logger.info(f"  Total unique: {len(all_symbols)} symbols")
    logger.info("")

    # Initialize downloader
    downloader = MarketDataDownloader(str(db_path))
    downloader.setup_database()

    logger.info("")
    logger.info("Starting download...")
    logger.info("This will take approximately 4-5 hours")
    logger.info("")

    # Download all stocks
    start_time = datetime.now()
    results = await downloader.download_all(all_symbols, batch_size=10)
    elapsed = datetime.now() - start_time

    # Summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("Download Complete!")
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

    # Database stats
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM stocks")
    stock_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM fundamentals_annual")
    fundamentals_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM price_history")
    price_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM insider_transactions")
    insider_count = cursor.fetchone()[0]

    conn.close()

    # Get database file size
    db_size_mb = db_path.stat().st_size / (1024 * 1024)

    logger.info("Database statistics:")
    logger.info(f"  Location: {db_path}")
    logger.info(f"  Size: {db_size_mb:.1f} MB")
    logger.info(f"  Stocks: {stock_count:,}")
    logger.info(f"  Annual fundamentals: {fundamentals_count:,}")
    logger.info(f"  Price history records: {price_count:,}")
    logger.info(f"  Insider transactions: {insider_count:,}")
    logger.info("")
    logger.info("Done! Database is ready for use.")


if __name__ == "__main__":
    asyncio.run(main())
