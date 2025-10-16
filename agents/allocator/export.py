"""PDF export functionality for Allocator Agent analysis results."""

import io
import logging
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import yfinance as yf
from matplotlib import font_manager
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.pdfmetrics import registerFontFamily
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle, Image

from agents.allocator.models import AllocatorReport
from agents.allocator.tools import (
    get_insider_ownership,
    get_stock_fundamentals,
    get_technical_indicators,
    get_valuation_metrics,
)

logger = logging.getLogger(__name__)


def _register_unicode_fonts():
    """Register DejaVu fonts bundled with matplotlib for Unicode support.

    Registers DejaVu Sans font family to support Unicode characters like
    en dash (–), em dash (—), and other special characters that Helvetica
    doesn't support.
    """
    try:
        # Get path to matplotlib's bundled DejaVu fonts
        dejavu_path = Path(font_manager.findfont(font_manager.FontProperties(family='DejaVu Sans')))
        dejavu_dir = dejavu_path.parent

        # Register font family variants
        pdfmetrics.registerFont(TTFont('DejaVuSans', str(dejavu_dir / 'DejaVuSans.ttf')))
        pdfmetrics.registerFont(TTFont('DejaVuSans-Bold', str(dejavu_dir / 'DejaVuSans-Bold.ttf')))
        pdfmetrics.registerFont(TTFont('DejaVuSans-Oblique', str(dejavu_dir / 'DejaVuSans-Oblique.ttf')))
        pdfmetrics.registerFont(TTFont('DejaVuSans-BoldOblique', str(dejavu_dir / 'DejaVuSans-BoldOblique.ttf')))

        # Register font family for automatic bold/italic switching
        registerFontFamily('DejaVuSans',
                          normal='DejaVuSans',
                          bold='DejaVuSans-Bold',
                          italic='DejaVuSans-Oblique',
                          boldItalic='DejaVuSans-BoldOblique')

        logger.info(f"Registered DejaVu fonts from {dejavu_dir}")
    except Exception as e:
        logger.warning(f"Could not register DejaVu fonts: {e}. Falling back to Helvetica.")


# Register Unicode fonts once at module load
_register_unicode_fonts()


async def _fetch_metrics(symbol: str) -> dict:
    """Fetch all metrics for a symbol.

    Args:
        symbol: Stock ticker symbol

    Returns:
        Dictionary with fundamentals, valuation, technical, and ownership data
    """
    try:
        fundamentals = await get_stock_fundamentals(symbol)
        valuation = await get_valuation_metrics(symbol)
        technical = await get_technical_indicators(symbol)
        ownership = await get_insider_ownership(symbol)

        return {
            "fundamentals": fundamentals,
            "valuation": valuation,
            "technical": technical,
            "ownership": ownership,
        }
    except Exception as e:
        logger.error(f"Error fetching metrics for {symbol}: {e}")
        return {}


def _format_value(value, format_type: str = "general") -> str:
    """Format a value for display in PDF table.

    Args:
        value: Value to format
        format_type: Type of formatting (general, currency, percent, number)

    Returns:
        Formatted string
    """
    if value is None:
        return "N/A"

    try:
        if format_type == "currency":
            if value >= 1_000_000_000_000:
                return f"${value / 1_000_000_000_000:.2f}T"
            elif value >= 1_000_000_000:
                return f"${value / 1_000_000_000:.2f}B"
            elif value >= 1_000_000:
                return f"${value / 1_000_000:.2f}M"
            else:
                return f"${value:,.0f}"
        elif format_type == "percent":
            return f"{value * 100:.2f}%"
        elif format_type == "number":
            return f"{value:,.2f}"
        else:
            return str(value)
    except (ValueError, TypeError):
        return "N/A"


async def _create_unified_comparison_table(report: AllocatorReport) -> Table:
    """Create unified comparison table with reference stock and all similar stocks.

    Args:
        report: AllocatorReport with reference stock and similar stocks

    Returns:
        Reportlab Table object with complete metrics comparison
    """
    # Fetch metrics for reference stock
    ref_metrics = await _fetch_metrics(report.symbol)

    # Fetch metrics for similar stocks
    similar_metrics_list = []
    for stock in report.similar_stocks[:10]:  # Limit to 10 for table width
        metrics = await _fetch_metrics(stock.symbol)
        similar_metrics_list.append((stock.symbol, metrics))

    # Build table data
    table_data = []

    # Header row with symbols
    header = ["Metric", report.symbol]  # Reference stock first
    header.extend([sym for sym, _ in similar_metrics_list])
    table_data.append(header)

    # Helper to get metric value from nested dict
    def get_metric(metrics_dict, category, key, format_type):
        val = metrics_dict.get(category, {}).get(key)
        if format_type == "percent" and val is not None and val != "N/A":
            return _format_value(val, "percent")
        elif format_type == "currency_no_decimal":
            # For returns, don't use currency format
            if val is not None:
                return f"{val:.1f}%" if isinstance(val, (int, float)) else str(val)
            return "N/A"
        else:
            return _format_value(val, format_type)

    # Financial Metrics Section
    table_data.append(["FINANCIAL METRICS", "", *["" for _ in similar_metrics_list]])

    financial_metrics = [
        ("Market Cap", "fundamentals", "market_cap", "currency"),
        ("ROIC", "fundamentals", "roic", "percent"),
        ("ROE", "fundamentals", "roe", "percent"),
        ("Profit Margin", "fundamentals", "profit_margin", "percent"),
        ("Operating Margin", "fundamentals", "operating_margin", "percent"),
        ("Gross Margin", "fundamentals", "gross_margin", "percent"),
        ("Revenue Growth", "fundamentals", "revenue_growth", "percent"),
        ("Earnings Growth", "fundamentals", "earnings_growth", "percent"),
        ("Debt/Equity", "fundamentals", "debt_to_equity", "number"),
        ("Current Ratio", "fundamentals", "current_ratio", "number"),
        ("Free Cash Flow", "fundamentals", "free_cash_flow", "currency"),
        ("Operating Cash Flow", "fundamentals", "operating_cash_flow", "currency"),
    ]

    for label, category, key, fmt in financial_metrics:
        row = [label, get_metric(ref_metrics, category, key, fmt)]
        row.extend([get_metric(metrics, category, key, fmt) for _, metrics in similar_metrics_list])
        table_data.append(row)

    # Valuation Metrics Section
    table_data.append(["VALUATION METRICS", "", *["" for _ in similar_metrics_list]])

    valuation_metrics = [
        ("Trailing P/E", "valuation", "trailing_pe", "number"),
        ("Forward P/E", "valuation", "forward_pe", "number"),
        ("Price/Book", "valuation", "price_to_book", "number"),
        ("Price/Sales", "valuation", "price_to_sales", "number"),
        ("EV/EBITDA", "valuation", "enterprise_to_ebitda", "number"),
        ("PEG Ratio", "valuation", "peg_ratio", "number"),
    ]

    for label, category, key, fmt in valuation_metrics:
        row = [label, get_metric(ref_metrics, category, key, fmt)]
        row.extend([get_metric(metrics, category, key, fmt) for _, metrics in similar_metrics_list])
        table_data.append(row)

    # Technical Indicators Section
    table_data.append(["TECHNICAL INDICATORS", "", *["" for _ in similar_metrics_list]])

    technical_metrics = [
        ("Current Price", "technical", "current_price", "currency"),
        ("RSI", "technical", "rsi", "number"),
        ("Trend", "technical", "trend", "general"),
        ("1M Return %", "technical", "returns_1m_pct", "currency_no_decimal"),
        ("3M Return %", "technical", "returns_3m_pct", "currency_no_decimal"),
        ("50-day MA", "technical", "sma_50", "currency"),
        ("200-day MA", "technical", "sma_200", "currency"),
    ]

    for label, category, key, fmt in technical_metrics:
        row = [label, get_metric(ref_metrics, category, key, fmt)]
        row.extend([get_metric(metrics, category, key, fmt) for _, metrics in similar_metrics_list])
        table_data.append(row)

    # Ownership Section
    table_data.append(["OWNERSHIP", "", *["" for _ in similar_metrics_list]])

    ownership_metrics = [
        ("Insider %", "ownership", "insider_ownership_pct", "percent"),
        ("Institutional %", "ownership", "institutional_ownership_pct", "percent"),
        ("Shares Outstanding", "ownership", "shares_outstanding", "currency"),
    ]

    for label, category, key, fmt in ownership_metrics:
        row = [label, get_metric(ref_metrics, category, key, fmt)]
        row.extend([get_metric(metrics, category, key, fmt) for _, metrics in similar_metrics_list])
        table_data.append(row)

    # Calculate column widths dynamically
    num_cols = len(header)
    col_width = 6.5 * inch / num_cols  # Fit within page width
    col_widths = [col_width * 1.5] + [col_width] * (num_cols - 1)  # Wider first column

    # Create table
    table = Table(table_data, colWidths=col_widths)

    # Style the table
    style_commands = [
        ("FONTNAME", (0, 0), (-1, 0), "DejaVuSans-Bold"),  # Header row
        ("FONTSIZE", (0, 0), (-1, -1), 7),  # Smaller font to fit
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f77b4")),  # Header background
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),  # Header text
        ("BACKGROUND", (1, 0), (1, -1), colors.HexColor("#e6f2ff")),  # Highlight reference column
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("ALIGN", (0, 0), (0, -1), "LEFT"),  # Left-align metric names
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
    ]

    # Bold section headers
    section_rows = [i for i, row in enumerate(table_data) if row[0] in ["FINANCIAL METRICS", "VALUATION METRICS", "TECHNICAL INDICATORS", "OWNERSHIP"]]
    for row_idx in section_rows:
        style_commands.append(("FONTNAME", (0, row_idx), (-1, row_idx), "DejaVuSans-Bold"))
        style_commands.append(("BACKGROUND", (0, row_idx), (-1, row_idx), colors.HexColor("#f0f0f0")))

    table.setStyle(TableStyle(style_commands))

    return table


def _create_price_chart(symbol: str, period: str = "6mo") -> io.BytesIO | None:
    """Generate price chart with technical indicators.

    Args:
        symbol: Stock ticker symbol
        period: Historical period (3mo, 6mo, 1y, 2y)

    Returns:
        BytesIO containing PNG image, or None if error
    """
    try:
        stock = yf.Ticker(symbol)
        hist = stock.history(period=period)

        if hist.empty:
            return None

        # Create figure with subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), height_ratios=[3, 1])

        # Price and moving averages
        ax1.plot(hist.index, hist["Close"], label="Price", linewidth=2)
        if len(hist) >= 50:
            ma50 = hist["Close"].rolling(50).mean()
            ax1.plot(hist.index, ma50, label="50-day MA", linestyle="--", alpha=0.7)
        if len(hist) >= 200:
            ma200 = hist["Close"].rolling(200).mean()
            ax1.plot(hist.index, ma200, label="200-day MA", linestyle="--", alpha=0.7)

        ax1.set_ylabel("Price ($)")
        ax1.set_title(f"{symbol} Price Chart ({period})")
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        # Volume
        ax2.bar(hist.index, hist["Volume"], alpha=0.5, color="gray")
        ax2.set_ylabel("Volume")
        ax2.set_xlabel("Date")
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()

        # Save to BytesIO
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=150, bbox_inches="tight")
        buf.seek(0)
        plt.close(fig)

        return buf
    except Exception as e:
        logger.error(f"Error creating price chart for {symbol}: {e}")
        return None


async def export_allocator_report_pdf(report: AllocatorReport, filename: str) -> None:
    """Export comprehensive AllocatorReport to formatted PDF.

    Args:
        report: AllocatorReport Pydantic model
        filename: Output PDF filename
    """
    doc = SimpleDocTemplate(filename, pagesize=letter, topMargin=0.75 * inch, bottomMargin=0.75 * inch)
    story = []
    styles_dict = getSampleStyleSheet()

    # Custom styles with Unicode font
    title_style = ParagraphStyle(
        "CustomTitle", parent=styles_dict["Heading1"], fontSize=24, textColor=colors.HexColor("#1f77b4"), spaceAfter=12,
        fontName='DejaVuSans-Bold'
    )
    heading_style = ParagraphStyle("CustomHeading", parent=styles_dict["Heading2"], fontSize=16, spaceAfter=10,
        fontName='DejaVuSans-Bold'
    )
    subheading_style = ParagraphStyle("CustomSubheading", parent=styles_dict["Heading3"], fontSize=12, spaceAfter=8,
        fontName='DejaVuSans-Bold'
    )
    body_style = ParagraphStyle("DejaVuBody", parent=styles_dict["BodyText"], fontName='DejaVuSans')

    styles = {
        "title": title_style,
        "heading": heading_style,
        "subheading": subheading_style,
        "body": body_style,
    }

    analysis = report.analysis

    # ===================
    # PART 1: Stock Analysis
    # ===================

    # Title page
    story.append(Paragraph(f"{analysis.symbol} - {analysis.company_name}", title_style))
    story.append(Paragraph("Allocator Report", heading_style))
    story.append(Spacer(1, 0.2 * inch))

    # Recommendation box
    rec_data = [
        ["Recommendation:", analysis.recommendation],
        ["Conviction:", analysis.conviction_level],
        ["Date:", datetime.now().strftime("%Y-%m-%d %H:%M")],
    ]
    rec_table = Table(rec_data, colWidths=[2.0 * inch, 4 * inch])
    rec_table.setStyle(
        TableStyle([
            ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f0f0f0")),
            ("TEXTCOLOR", (0, 0), (-1, -1), colors.black),
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("FONTNAME", (0, 0), (0, -1), "DejaVuSans-Bold"),
            ("FONTNAME", (1, 0), (1, -1), "DejaVuSans"),
            ("FONTSIZE", (0, 0), (-1, -1), 12),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 1, colors.grey),
        ])
    )
    story.append(rec_table)
    story.append(Spacer(1, 0.3 * inch))

    # Price chart
    chart_buf = _create_price_chart(report.symbol)
    if chart_buf:
        img = Image(chart_buf, width=6 * inch, height=4 * inch)
        story.append(img)
        story.append(Spacer(1, 0.2 * inch))

    story.append(PageBreak())

    # Key Metrics & Signals Section - Unified Comparison Table
    story.append(Paragraph("Key Metrics & Signals - Comparative Analysis", heading_style))
    story.append(Spacer(1, 0.1 * inch))

    # Create unified comparison table with reference + similar stocks
    comparison_table = await _create_unified_comparison_table(report)
    story.append(comparison_table)
    story.append(Spacer(1, 0.2 * inch))

    story.append(PageBreak())

    # Investment Thesis
    story.append(Paragraph("Investment Thesis", heading_style))
    story.append(Paragraph(analysis.investment_thesis, body_style))
    story.append(Spacer(1, 0.2 * inch))

    # Analysis sections
    sections = [
        ("Management Quality", analysis.management_quality),
        ("Capital Allocation", analysis.capital_allocation),
        ("Financial Quality", analysis.financial_quality),
        ("Competitive Position", analysis.competitive_position),
        ("Valuation Assessment", analysis.valuation_assessment),
        ("Technical Setup", analysis.technical_setup),
    ]

    for section_title, section_content in sections:
        story.append(Paragraph(section_title, heading_style))
        story.append(Paragraph(section_content, body_style))
        story.append(Spacer(1, 0.15 * inch))

    story.append(PageBreak())

    # Key Positives
    story.append(Paragraph("Key Positives", heading_style))
    for positive in analysis.key_positives:
        story.append(Paragraph(f"• {positive}", body_style))
        story.append(Spacer(1, 0.05 * inch))
    story.append(Spacer(1, 0.2 * inch))

    # Key Risks
    story.append(Paragraph("Key Risks", heading_style))
    for risk in analysis.key_risks:
        story.append(Paragraph(f"• {risk}", body_style))
        story.append(Spacer(1, 0.05 * inch))
    story.append(Spacer(1, 0.3 * inch))

    # ===================
    # PART 2: Similar Stocks
    # ===================

    if report.similar_stocks:
        story.append(PageBreak())
        story.append(Paragraph(f"Similar Stocks to {report.symbol}", title_style))
        story.append(Spacer(1, 0.2 * inch))

        # Similarity scores chart
        fig, ax = plt.subplots(figsize=(8, 4))
        symbols = [s.symbol for s in report.similar_stocks]
        scores = [s.similarity_score for s in report.similar_stocks]
        ax.barh(symbols, scores, color=plt.cm.Blues_r(plt.Normalize(0, 100)(scores)))
        ax.set_xlabel("Similarity Score")
        ax.set_title("Similarity Comparison")
        ax.grid(True, alpha=0.3, axis="x")
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=150, bbox_inches="tight")
        buf.seek(0)
        plt.close(fig)

        img = Image(buf, width=6 * inch, height=3 * inch)
        story.append(img)
        story.append(Spacer(1, 0.3 * inch))

        # Details for each similar stock
        for i, stock in enumerate(report.similar_stocks, 1):
            story.append(
                Paragraph(
                    f"{i}. {stock.symbol} - {stock.company_name} (Similarity: {stock.similarity_score}/100)",
                    heading_style,
                )
            )

            story.append(Paragraph("<b>Key Similarities:</b>", body_style))
            for sim in stock.key_similarities:
                story.append(Paragraph(f"• {sim}", body_style))
            story.append(Spacer(1, 0.1 * inch))

            story.append(Paragraph("<b>Key Differences:</b>", body_style))
            for diff in stock.key_differences:
                story.append(Paragraph(f"• {diff}", body_style))
            story.append(Spacer(1, 0.1 * inch))

            story.append(Paragraph("<b>Relative Attractiveness:</b>", body_style))
            story.append(Paragraph(stock.relative_attractiveness, body_style))
            story.append(Spacer(1, 0.2 * inch))

    # Sources
    if report.sources:
        story.append(PageBreak())
        story.append(Paragraph("Sources", heading_style))
        for i, source in enumerate(report.sources, 1):
            story.append(Paragraph(f"[{i}] {source}", body_style))
            story.append(Spacer(1, 0.05 * inch))

    # Build PDF
    doc.build(story)
    logger.info(f"Exported AllocatorReport to {filename}")
