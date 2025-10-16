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

from allocator.models import SimilarStocksResult, StockAnalysis

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


def export_stock_analysis_pdf(analysis: StockAnalysis, filename: str, symbol: str) -> None:
    """Export StockAnalysis to formatted PDF report.

    Args:
        analysis: StockAnalysis Pydantic model
        filename: Output PDF filename
        symbol: Stock ticker symbol (for chart generation)
    """
    doc = SimpleDocTemplate(filename, pagesize=letter, topMargin=0.75 * inch, bottomMargin=0.75 * inch)
    story = []
    styles = getSampleStyleSheet()

    # Custom styles with Unicode font
    title_style = ParagraphStyle(
        "CustomTitle", parent=styles["Heading1"], fontSize=24, textColor=colors.HexColor("#1f77b4"), spaceAfter=12,
        fontName='DejaVuSans-Bold'
    )
    heading_style = ParagraphStyle("CustomHeading", parent=styles["Heading2"], fontSize=16, spaceAfter=10,
        fontName='DejaVuSans-Bold'
    )
    subheading_style = ParagraphStyle("CustomSubheading", parent=styles["Heading3"], fontSize=12, spaceAfter=8,
        fontName='DejaVuSans-Bold'
    )
    body_style = ParagraphStyle("DejaVuBody", parent=styles["BodyText"], fontName='DejaVuSans')

    # Title page
    story.append(Paragraph(f"{analysis.symbol} - {analysis.company_name}", title_style))
    story.append(Spacer(1, 0.2 * inch))

    # Recommendation box
    rec_data = [
        ["Recommendation:", analysis.recommendation],
        ["Conviction:", analysis.conviction_level],
        ["Date:", datetime.now().strftime("%Y-%m-%d %H:%M")],
    ]
    rec_table = Table(rec_data, colWidths=[2.0 * inch, 4 * inch])
    rec_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f0f0f0")),
                ("TEXTCOLOR", (0, 0), (-1, -1), colors.black),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("FONTNAME", (0, 0), (0, -1), "DejaVuSans-Bold"),
                ("FONTNAME", (1, 0), (1, -1), "DejaVuSans"),
                ("FONTSIZE", (0, 0), (-1, -1), 12),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ("GRID", (0, 0), (-1, -1), 1, colors.grey),
            ]
        )
    )
    story.append(rec_table)
    story.append(Spacer(1, 0.3 * inch))

    # Price chart
    chart_buf = _create_price_chart(symbol)
    if chart_buf:
        img = Image(chart_buf, width=6 * inch, height=4 * inch)
        story.append(img)
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
    story.append(Spacer(1, 0.2 * inch))

    # Sources
    if analysis.sources:
        story.append(Paragraph("Sources", heading_style))
        for i, source in enumerate(analysis.sources, 1):
            story.append(Paragraph(f"[{i}] {source}", body_style))
            story.append(Spacer(1, 0.05 * inch))

    # Build PDF
    doc.build(story)
    logger.info(f"Exported StockAnalysis to {filename}")


def export_similar_stocks_pdf(result: SimilarStocksResult, filename: str) -> None:
    """Export SimilarStocksResult to formatted PDF report.

    Args:
        result: SimilarStocksResult Pydantic model
        filename: Output PDF filename
    """
    doc = SimpleDocTemplate(filename, pagesize=letter, topMargin=0.75 * inch, bottomMargin=0.75 * inch)
    story = []
    styles = getSampleStyleSheet()

    title_style = ParagraphStyle(
        "CustomTitle", parent=styles["Heading1"], fontSize=20, textColor=colors.HexColor("#1f77b4"), spaceAfter=12,
        fontName='DejaVuSans-Bold'
    )
    heading_style = ParagraphStyle("CustomHeading", parent=styles["Heading2"], fontSize=14, spaceAfter=10,
        fontName='DejaVuSans-Bold'
    )
    body_style = ParagraphStyle("DejaVuBody", parent=styles["BodyText"], fontName='DejaVuSans')

    # Title
    story.append(Paragraph(f"Stocks Similar to {result.reference_symbol}", title_style))
    story.append(Spacer(1, 0.2 * inch))

    # Similarity scores chart
    if result.similar_stocks:
        fig, ax = plt.subplots(figsize=(8, 4))
        symbols = [s.symbol for s in result.similar_stocks]
        scores = [s.similarity_score for s in result.similar_stocks]
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

    # Details for each stock
    for i, stock in enumerate(result.similar_stocks, 1):
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
    if result.sources:
        story.append(PageBreak())
        story.append(Paragraph("Sources", heading_style))
        for i, source in enumerate(result.sources, 1):
            story.append(Paragraph(f"[{i}] {source}", body_style))
            story.append(Spacer(1, 0.05 * inch))

    doc.build(story)
    logger.info(f"Exported SimilarStocksResult to {filename}")
