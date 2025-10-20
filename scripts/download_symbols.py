"""Download S&P 500 and Russell 2000 symbol lists.

Creates static JSON files with stock symbols for database download.
"""

import json
import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def get_sp500_symbols() -> list[str]:
    """Download S&P 500 symbols from Wikipedia.

    Returns:
        List of S&P 500 ticker symbols
    """
    logger.info("Downloading S&P 500 symbols from Wikipedia...")

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    # Add User-Agent header to avoid 403 Forbidden
    tables = pd.read_html(
        url, storage_options={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    )
    sp500_table = tables[0]

    symbols = sp500_table["Symbol"].tolist()

    # Clean symbols (remove dots, fix formatting)
    symbols = [s.replace(".", "-") for s in symbols]

    logger.info(f"Found {len(symbols)} S&P 500 symbols")
    return symbols


def get_russell2000_symbols() -> list[str]:
    """Download Russell 2000 symbols from iShares IWM ETF holdings CSV.

    Returns:
        List of Russell 2000 ticker symbols
    """
    logger.info("Downloading Russell 2000 symbols from iShares IWM ETF...")

    try:
        # Direct CSV download from iShares
        url = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"

        # Read CSV, skipping the first 9 rows (header is on row 10)
        df = pd.read_csv(url, skiprows=9, encoding="utf-8-sig")  # utf-8-sig to handle BOM

        # Find the Ticker column
        if "Ticker" in df.columns:
            symbols = df["Ticker"].dropna().tolist()

            # Clean up symbols
            symbols = [str(s).strip().upper() for s in symbols if pd.notna(s) and str(s).strip()]

            # Filter out:
            # - Cash/derivatives (starts with 'X' like 'XTSLA')
            # - Empty or '-' values
            # - Non-ticker entries
            symbols = [
                s.replace(".", "-")
                for s in symbols
                if s
                and s != "-"
                and len(s) <= 6
                and not s.startswith("X")
                and s.replace("-", "").replace(".", "").isalpha()
            ]

            logger.info(f"Found {len(symbols)} Russell 2000 symbols from iShares IWM")
            return symbols
        else:
            logger.error(f"Could not find 'Ticker' column in CSV. Columns: {df.columns.tolist()}")

    except Exception as e:
        logger.error(f"Error downloading from iShares: {e}")

    return []


def save_symbols(symbols: list[str], filename: str, data_dir: Path):
    """Save symbol list to JSON file.

    Args:
        symbols: List of ticker symbols
        filename: Output filename
        data_dir: Data directory path
    """
    filepath = data_dir / filename

    with open(filepath, "w") as f:
        json.dump(sorted(symbols), f, indent=2)

    logger.info(f"Saved {len(symbols)} symbols to {filepath}")


def main():
    """Download and save symbol lists."""
    logger.info("=" * 60)
    logger.info("Downloading Stock Symbol Lists")
    logger.info("=" * 60)
    logger.info("")

    # Get data directory
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "agents" / "allocator" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Download S&P 500
    sp500_symbols = get_sp500_symbols()
    save_symbols(sp500_symbols, "sp500_symbols.json", data_dir)

    logger.info("")

    # Download Russell 2000
    russell2000_symbols = get_russell2000_symbols()
    save_symbols(russell2000_symbols, "russell2000_symbols.json", data_dir)

    logger.info("")
    logger.info("=" * 60)
    logger.info("Summary")
    logger.info("=" * 60)
    logger.info(f"S&P 500: {len(sp500_symbols)} symbols")
    logger.info(f"Russell 2000: {len(russell2000_symbols)} symbols")

    # Calculate overlap
    sp500_set = set(sp500_symbols)
    russell2000_set = set(russell2000_symbols)
    overlap = sp500_set & russell2000_set
    unique_total = len(sp500_set | russell2000_set)

    logger.info(f"Overlap: {len(overlap)} symbols")
    logger.info(f"Total unique: {unique_total} symbols")
    logger.info("")
    logger.info("Symbol lists saved to:")
    logger.info(f"  - {data_dir / 'sp500_symbols.json'}")
    logger.info(f"  - {data_dir / 'russell2000_symbols.json'}")


if __name__ == "__main__":
    main()
