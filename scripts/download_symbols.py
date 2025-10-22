"""Download Russell 3000 symbol list.

Creates static JSON file with stock symbols for database download.
"""

import json
import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def get_russell3000_symbols() -> list[str]:
    """Download Russell 3000 symbols from iShares IWV ETF holdings CSV.

    Returns:
        List of Russell 3000 ticker symbols
    """
    logger.info("Downloading Russell 3000 symbols from iShares IWV ETF...")

    try:
        # Direct CSV download from iShares
        url = "https://www.ishares.com/us/products/239714/ishares-russell-3000-etf/1467271812596.ajax?fileType=csv&fileName=IWV_holdings&dataType=fund"

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

            logger.info(f"Found {len(symbols)} Russell 3000 symbols from iShares IWV")
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

    # Download Russell 3000
    russell3000_symbols = get_russell3000_symbols()
    save_symbols(russell3000_symbols, "russell3000_symbols.json", data_dir)

    logger.info("")
    logger.info("=" * 60)
    logger.info("Summary")
    logger.info("=" * 60)
    logger.info(f"Russell 3000: {len(russell3000_symbols)} symbols")
    logger.info("")
    logger.info("Symbol list saved to:")
    logger.info(f"  - {data_dir / 'russell3000_symbols.json'}")


if __name__ == "__main__":
    main()
