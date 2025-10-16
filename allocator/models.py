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


