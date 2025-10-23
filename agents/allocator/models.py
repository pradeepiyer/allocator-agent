"""Pydantic models for structured outputs from Allocator Agent."""

from typing import Literal

from pydantic import BaseModel, Field


class StockAnalysis(BaseModel):
    """Comprehensive stock analysis with investment recommendation."""

    model_config = {"extra": "forbid"}

    symbol: str
    company_name: str
    management_quality: str
    capital_allocation: str
    financial_quality: str
    competitive_position: str
    valuation_assessment: str
    technical_setup: str
    investment_thesis: str
    key_positives: list[str]
    key_risks: list[str]
    recommendation: Literal["Strong Buy", "Buy", "Hold", "Sell", "Pass"]
    conviction_level: Literal["High", "Medium", "Low"]
    sources: list[str]


class SimilarStock(BaseModel):
    """Details of a stock similar to the reference stock."""

    model_config = {"extra": "forbid"}

    symbol: str
    company_name: str
    similarity_score: int
    key_similarities: list[str]
    key_differences: list[str]
    relative_attractiveness: str


class SimilarStocksResult(BaseModel):
    """Result of finding stocks similar to a reference stock."""

    model_config = {"extra": "forbid"}

    reference_symbol: str
    similar_stocks: list[SimilarStock]
    sources: list[str]


class AllocatorReport(BaseModel):
    """Comprehensive analysis report including stock analysis and similar stocks."""

    model_config = {"extra": "forbid"}

    symbol: str
    analysis: StockAnalysis
    similar_stocks: list[SimilarStock]
    sources: list[str]


class KeyMetrics(BaseModel):
    """Key financial metrics for a screened stock."""

    model_config = {"extra": "forbid"}

    roic: float
    roe: float
    profit_margin: float
    debt_to_equity: float
    insider_ownership_pct: float
    forward_pe: float
    market_cap: float


class ScreenedStock(BaseModel):
    """Stock identified through screening with quality score and metrics."""

    model_config = {"extra": "forbid"}

    symbol: str
    name: str
    sector: str
    quality_score: int
    key_strengths: list[str]
    key_metrics: KeyMetrics


class ScreeningResult(BaseModel):
    """Result of screening the database for investment opportunities."""

    model_config = {"extra": "forbid"}

    screened_stocks: list[ScreenedStock]
    total_analyzed: int
    screening_criteria: str
    sources: list[str]


class AllocatorRequest(BaseModel):
    """Unified request model for allocator agent (REST/MCP interfaces)."""

    query: str = Field(..., description="Natural language investment query")
    session_id: str | None = Field(None, description="Session ID for conversation continuation")


class AllocatorResponse(BaseModel):
    """Unified response model for allocator agent (REST/MCP interfaces)."""

    response: str = Field(..., description="Analysis response (JSON for structured outputs)")
    session_id: str = Field(..., description="Session ID")
