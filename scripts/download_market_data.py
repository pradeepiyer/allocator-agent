"""Download historical market data and build SQLite database.

Downloads 10 years of financial data for Russell 3000 stocks.
Creates database at: agents/allocator/data/market.db

Usage:
    uv run python scripts/download_market_data.py

Estimated time: 5-6 hours for ~3000 stocks
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
                last_updated TIMESTAMP,
                beta REAL,
                enterprise_value REAL,
                quick_ratio REAL,
                total_cash REAL,
                total_debt REAL,
                shares_short REAL,
                implied_shares_outstanding REAL,
                dividend_yield REAL,
                dividend_rate REAL,
                payout_ratio REAL,
                forward_pe REAL,
                forward_eps REAL,
                peg_ratio REAL
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
                roa REAL,
                ebitda REAL,
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

        # 7. Institutional holders
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS institutional_holders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                holder_name TEXT,
                shares REAL,
                date_reported DATE,
                pct_out REAL,
                value REAL,
                FOREIGN KEY (symbol) REFERENCES stocks(symbol)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inst_holders_symbol ON institutional_holders(symbol)")

        # 8. Major holders
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS major_holders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                insiders_percent REAL,
                institutions_percent REAL,
                institutions_float_percent REAL,
                institutions_count INTEGER,
                as_of_date DATE,
                FOREIGN KEY (symbol) REFERENCES stocks(symbol),
                UNIQUE(symbol, as_of_date)
            )
        """)

        # 9. Executives
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS executives (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                name TEXT,
                title TEXT,
                total_pay REAL,
                exercised_value REAL,
                unexercised_value REAL,
                year_born INTEGER,
                fiscal_year INTEGER,
                FOREIGN KEY (symbol) REFERENCES stocks(symbol)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_executives_symbol ON executives(symbol)")

        # 10. Stock-based compensation
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_based_compensation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                fiscal_year INTEGER,
                sbc_amount REAL,
                FOREIGN KEY (symbol) REFERENCES stocks(symbol),
                UNIQUE(symbol, fiscal_year)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sbc_symbol ON stock_based_compensation(symbol)")

        # 11. Quarterly shares
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS quarterly_shares (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                fiscal_year INTEGER,
                fiscal_quarter INTEGER,
                shares_outstanding REAL,
                FOREIGN KEY (symbol) REFERENCES stocks(symbol),
                UNIQUE(symbol, fiscal_year, fiscal_quarter)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_qtr_shares_symbol ON quarterly_shares(symbol)")

        # 12. Quarterly fundamentals
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fundamentals_quarterly (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                fiscal_year INTEGER,
                fiscal_quarter INTEGER,
                revenue REAL,
                operating_income REAL,
                net_income REAL,
                total_assets REAL,
                total_liabilities REAL,
                shareholders_equity REAL,
                operating_cash_flow REAL,
                free_cash_flow REAL,
                gross_profit REAL,
                ebitda REAL,
                profit_margin REAL,
                operating_margin REAL,
                gross_margin REAL,
                debt_to_equity REAL,
                current_ratio REAL,
                FOREIGN KEY (symbol) REFERENCES stocks(symbol),
                UNIQUE(symbol, fiscal_year, fiscal_quarter)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_qtr_fund_symbol ON fundamentals_quarterly(symbol)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_qtr_fund_period ON fundamentals_quarterly(fiscal_year, fiscal_quarter)"
        )

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
        except Exception:
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

            # 1. Company info (includes all stock metadata)
            company_data = {
                "symbol": symbol,
                "name": info.get("longName"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "market_cap": info.get("marketCap"),
                "description": info.get("longBusinessSummary"),
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

                        shares_outstanding = self._get_value(balance_sheet, "Ordinary Shares Number", date_col)

                        # Calculate derived metrics
                        roic = self.calculate_roic(
                            operating_income, total_assets, current_liabilities, info.get("effectiveTaxRate", 0.21)
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

                        roa = None
                        if net_income and total_assets and total_assets > 0:
                            roa = float(net_income / total_assets)

                        # EBITDA = operating_income + depreciation (if available)
                        # For now, set to None as depreciation not easily extracted
                        ebitda = None

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
                                "roa": roa,
                                "ebitda": ebitda,
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

            # 7. Institutional holders
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

            # 8. Major holders
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
                    # major_holders DataFrame has format: [Value, Description]
                    # Rows vary but typically: insiders %, institutions %, institutions float %, institutions count
                    for idx, row in major_holders.iterrows():
                        desc = str(row.iloc[1]).lower() if len(row) > 1 else ""
                        value_str = str(row.iloc[0])

                        # Parse percentage values
                        if "insider" in desc:
                            try:
                                major_holders_data["insiders_percent"] = (
                                    float(value_str.replace("%", "")) / 100 if "%" in value_str else float(value_str)
                                )
                            except (ValueError, TypeError):
                                pass
                        elif "institution" in desc and "float" in desc:
                            try:
                                major_holders_data["institutions_float_percent"] = (
                                    float(value_str.replace("%", "")) / 100 if "%" in value_str else float(value_str)
                                )
                            except (ValueError, TypeError):
                                pass
                        elif "institution" in desc:
                            try:
                                # Could be percentage or count
                                if "%" in value_str:
                                    major_holders_data["institutions_percent"] = float(value_str.replace("%", "")) / 100
                                else:
                                    # Might be count
                                    major_holders_data["institutions_count"] = int(float(value_str))
                            except (ValueError, TypeError):
                                pass
            except Exception as e:
                logger.debug(f"{symbol}: Could not fetch major holders - {e}")

            # 9. Executives
            executives = []
            try:
                officers = info.get("companyOfficers", [])
                if officers:
                    current_year = datetime.now().year
                    for officer in officers[:10]:  # Top 10 executives
                        executives.append(
                            {
                                "symbol": symbol,
                                "name": officer.get("name"),
                                "title": officer.get("title"),
                                "total_pay": officer.get("totalPay"),
                                "exercised_value": officer.get("exercisedValue"),
                                "unexercised_value": officer.get("unexercisedValue"),
                                "year_born": officer.get("yearBorn"),
                                "fiscal_year": current_year,  # Current year for this snapshot
                            }
                        )
            except Exception as e:
                logger.debug(f"{symbol}: Could not fetch executives - {e}")

            # 10. Stock-based compensation (annual)
            sbc_data = []
            try:
                cash_flow = stock.cashflow  # Annual
                if not cash_flow.empty and "Stock Based Compensation" in cash_flow.index:
                    sbc = cash_flow.loc["Stock Based Compensation"]
                    for date_col, amount in sbc.items():
                        if pd.notna(amount):
                            sbc_data.append(
                                {"symbol": symbol, "fiscal_year": date_col.year, "sbc_amount": float(amount)}
                            )
            except Exception as e:
                logger.debug(f"{symbol}: Could not fetch stock-based compensation - {e}")

            # 11. Quarterly shares
            quarterly_shares = []
            try:
                qtr_bs = stock.quarterly_balance_sheet
                if not qtr_bs.empty and "Ordinary Shares Number" in qtr_bs.index:
                    shares_series = qtr_bs.loc["Ordinary Shares Number"]
                    for date_col, shares in shares_series.items():
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

            # 12. Quarterly fundamentals
            quarterly_data = []
            try:
                qtr_financials = stock.quarterly_financials
                qtr_balance_sheet = stock.quarterly_balance_sheet
                qtr_cash_flow = stock.quarterly_cashflow

                if not qtr_financials.empty:
                    # Process up to 40 quarters (10 years)
                    for date_col in qtr_financials.columns[:40]:
                        year = date_col.year
                        quarter = (date_col.month - 1) // 3 + 1

                        # Extract metrics (same as annual, but from quarterly statements)
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

                        # Calculate margins and ratios
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
                                "ebitda": None,  # Not easily calculated from quarterly
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
                "annual_fundamentals": annual_data,
                "price_history": price_data,
                "insider_transactions": insider_data,
                "ownership": ownership_data,
                "buybacks": buyback_data,
                "institutional_holders": institutional_holders,
                "major_holders": major_holders_data,
                "executives": executives,
                "stock_based_compensation": sbc_data,
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
        except Exception:
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
            # 1. Save company data (including all metadata fields)
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
                    stock_data["company"]["symbol"],
                    stock_data["company"]["name"],
                    stock_data["company"]["sector"],
                    stock_data["company"]["industry"],
                    stock_data["company"]["market_cap"],
                    stock_data["company"]["description"],
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
                     roic, roe, roa, ebitda, profit_margin, operating_margin, gross_margin,
                     debt_to_equity, current_ratio)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        record["roa"],
                        record["ebitda"],
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

            # 7. Save institutional holders
            for record in stock_data["institutional_holders"]:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO institutional_holders
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

            # 8. Save major holders
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

            # 9. Save executives
            for record in stock_data["executives"]:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO executives
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

            # 10. Save stock-based compensation
            for record in stock_data["stock_based_compensation"]:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO stock_based_compensation
                    (symbol, fiscal_year, sbc_amount)
                    VALUES (?, ?, ?)
                """,
                    (record["symbol"], record["fiscal_year"], record["sbc_amount"]),
                )

            # 11. Save quarterly shares
            for record in stock_data["quarterly_shares"]:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO quarterly_shares
                    (symbol, fiscal_year, fiscal_quarter, shares_outstanding)
                    VALUES (?, ?, ?, ?)
                """,
                    (record["symbol"], record["fiscal_year"], record["fiscal_quarter"], record["shares_outstanding"]),
                )

            # 12. Save quarterly fundamentals
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
                                {"success": self.success_count, "failed": len(self.failed_symbols)}, refresh=False
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

    # Load symbol list
    russell3000_file = data_dir / "russell3000_symbols.json"

    if not russell3000_file.exists():
        logger.error("Symbol file not found. Run download_symbols.py first.")
        logger.error(f"Expected file: {russell3000_file}")
        return

    with open(russell3000_file) as f:
        all_symbols = json.load(f)

    logger.info("Symbol list loaded:")
    logger.info(f"  Russell 3000: {len(all_symbols)} symbols")
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
