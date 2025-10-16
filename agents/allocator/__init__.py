"""Allocator Agent - Capital allocation and stock analysis agent."""

from agents.allocator.agent import AllocatorAgent
from agents.allocator.export import export_allocator_report_pdf
from agents.allocator.models import (
    AllocatorReport,
    SimilarStock,
    SimilarStocksResult,
    StockAnalysis,
)

__all__ = [
    "AllocatorAgent",
    "StockAnalysis",
    "SimilarStock",
    "SimilarStocksResult",
    "AllocatorReport",
    "export_allocator_report_pdf",
]
__version__ = "0.1.0"
