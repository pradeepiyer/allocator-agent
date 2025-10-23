"""Minimal console interface for Allocator Agent."""

import json
from datetime import datetime
from pathlib import Path
from typing import cast

from rich.markdown import Markdown

from agent_kit.api.console.server import SlashCommands
from agents.allocator.agent import AllocatorAgent
from agents.allocator.export import export_allocator_report_pdf, export_screening_result_pdf
from agents.allocator.models import AllocatorReport, ScreeningResult, SimilarStocksResult, StockAnalysis


class AllocatorCommands(SlashCommands):
    """Minimal console for allocator agent - unified conversational interface."""

    def __init__(self, *args, **kwargs):
        """Initialize console with response caching for export."""
        super().__init__(*args, **kwargs)
        self.last_response: str | None = None

        # Register allocator-specific commands
        self.register_command(
            "/export",
            self._handle_export_wrapper,
            "Export last analysis to PDF",
            "Export the last analysis result to PDF\nUsage: /export [filename.pdf]",
        )

    def _format_json_as_markdown(self, json_str: str) -> str:
        """Format JSON response as readable markdown."""
        try:
            data = json.loads(json_str)

            # Check if it's a StockAnalysis
            if "symbol" in data and "investment_thesis" in data:
                return self._format_stock_analysis(data)
            # Check if it's a SimilarStocksResult
            elif "similar_stocks" in data:
                return self._format_similar_stocks(data)
            # Check if it's a ScreeningResult
            elif "screened_stocks" in data:
                return self._format_screening_result(data)
            else:
                # Unknown structure - return formatted JSON
                return f"```json\n{json.dumps(data, indent=2)}\n```"
        except json.JSONDecodeError:
            # Not JSON - return as-is
            return json_str

    def _format_stock_analysis(self, data: dict) -> str:
        """Format StockAnalysis as markdown."""
        lines = [
            f"# {data['symbol']} - {data['company_name']}",
            "",
            f"**Recommendation:** {data['recommendation']} (Conviction: {data['conviction_level']})",
            "",
            "## Investment Thesis",
            data["investment_thesis"],
            "",
            "## Management Quality",
            data["management_quality"],
            "",
            "## Capital Allocation",
            data["capital_allocation"],
            "",
            "## Financial Quality",
            data["financial_quality"],
            "",
            "## Competitive Position",
            data["competitive_position"],
            "",
            "## Valuation Assessment",
            data["valuation_assessment"],
            "",
            "## Technical Setup",
            data["technical_setup"],
            "",
            "## Key Positives",
        ]
        for positive in data["key_positives"]:
            lines.append(f"- {positive}")

        lines.extend(["", "## Key Risks"])
        for risk in data["key_risks"]:
            lines.append(f"- {risk}")

        if data.get("sources"):
            lines.extend(["", "## Sources"])
            for i, source in enumerate(data["sources"], 1):
                lines.append(f"[{i}] {source}")

        return "\n".join(lines)

    def _format_similar_stocks(self, data: dict) -> str:
        """Format SimilarStocksResult as markdown."""
        lines = ["# Similar Stocks", ""]
        for i, stock in enumerate(data["similar_stocks"], 1):
            lines.extend(
                [
                    f"## {i}. {stock['symbol']} - {stock['company_name']} (Similarity: {stock['similarity_score']}/100)",
                    "",
                    "### Key Similarities",
                ]
            )
            for sim in stock["key_similarities"]:
                lines.append(f"- {sim}")

            lines.extend(["", "### Key Differences"])
            for diff in stock["key_differences"]:
                lines.append(f"- {diff}")

            lines.extend(["", "### Relative Attractiveness", stock["relative_attractiveness"], ""])

        if data.get("sources"):
            lines.extend(["", "## Sources"])
            for i, source in enumerate(data["sources"], 1):
                lines.append(f"[{i}] {source}")

        return "\n".join(lines)

    def _format_screening_result(self, data: dict) -> str:
        """Format ScreeningResult as markdown."""
        lines = [
            "# Investment Opportunities Screener",
            "",
            f"**Screening Criteria:** {data['screening_criteria']}",
            f"**Total Analyzed:** {data['total_analyzed']}",
            f"**High-Quality Matches:** {len(data['screened_stocks'])}",
            "",
        ]

        for i, stock in enumerate(data["screened_stocks"], 1):
            lines.extend(
                [
                    f"## {i}. {stock['symbol']} - {stock['name']}",
                    f"**Sector:** {stock['sector']} | **Quality Score:** {stock['quality_score']}/100",
                    "",
                    "### Key Strengths",
                ]
            )
            for strength in stock["key_strengths"]:
                lines.append(f"- {strength}")

            lines.extend(["", "### Key Metrics"])
            metrics = stock["key_metrics"]
            if metrics.get("roic") is not None:
                lines.append(f"- ROIC: {metrics['roic'] * 100:.1f}%")
            if metrics.get("roe") is not None:
                lines.append(f"- ROE: {metrics['roe'] * 100:.1f}%")
            if metrics.get("profit_margin") is not None:
                lines.append(f"- Profit Margin: {metrics['profit_margin'] * 100:.1f}%")
            if metrics.get("debt_to_equity") is not None:
                lines.append(f"- Debt/Equity: {metrics['debt_to_equity']:.2f}")
            if metrics.get("insider_ownership_pct") is not None:
                lines.append(f"- Insider Ownership: {metrics['insider_ownership_pct'] * 100:.1f}%")
            if metrics.get("forward_pe") is not None:
                lines.append(f"- Forward P/E: {metrics['forward_pe']:.1f}")
            if metrics.get("market_cap") is not None:
                market_cap_b = metrics["market_cap"] / 1_000_000_000
                lines.append(f"- Market Cap: ${market_cap_b:.1f}B")

            lines.append("")

        if data.get("sources"):
            lines.extend(["", "## Sources"])
            for i, source in enumerate(data["sources"], 1):
                lines.append(f"[{i}] {source}")

        return "\n".join(lines)

    def show_help(self) -> None:
        """Show help with allocator-specific conversational guidance."""
        # Show standard help table first
        super()._print_help([])

        # Add allocator-specific conversational guidance
        self.console.print("\n[dim]Conversational Analysis:[/dim]")
        self.console.print("[dim]Ask natural language questions like:[/dim]")
        self.console.print('[dim]  • "analyze MSFT"[/dim]')
        self.console.print('[dim]  • "find similar stocks to AAPL"[/dim]')
        self.console.print('[dim]  • "screen for high ROIC companies"[/dim]')
        self.console.print('[dim]  • "tell me more about the management"[/dim]')
        self.console.print()

    async def _handle_export_wrapper(self, args: list[str]) -> None:
        """Wrapper for command registry (expects args: list[str])."""
        filename = args[0] if args else None
        await self._handle_export(filename)

    async def _handle_export(self, filename: str | None = None) -> None:
        """Handle /export command to generate PDF reports."""
        # Check if there's a cached response
        if not self.last_response:
            self.console.print("[red]No analysis to export. Run analyze or screen first.[/red]")
            return

        try:
            # Parse JSON response
            data = json.loads(self.last_response)

            # Create reports directory
            reports_dir = Path("reports")
            reports_dir.mkdir(exist_ok=True)

            # Determine type and export
            if "symbol" in data and "investment_thesis" in data:
                # StockAnalysis
                analysis = StockAnalysis(**data)
                symbol = analysis.symbol
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

                if not filename:
                    filename = str(reports_dir / f"analysis-{symbol}-{timestamp}.pdf")

                # Wrap in AllocatorReport with empty similar_stocks
                report = AllocatorReport(symbol=symbol, analysis=analysis, similar_stocks=[], sources=analysis.sources)

                await export_allocator_report_pdf(report, filename, skip_analysis=False)
                self.console.print(f"[green]✓ PDF report saved to {filename}[/green]")

            elif "similar_stocks" in data and "reference_symbol" in data:
                # SimilarStocksResult
                result = SimilarStocksResult(**data)
                symbol = result.reference_symbol
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

                if not filename:
                    filename = str(reports_dir / f"similar-{symbol}-{timestamp}.pdf")

                # Create minimal StockAnalysis as placeholder
                placeholder_analysis = StockAnalysis(
                    symbol=symbol,
                    company_name=symbol,
                    management_quality="N/A",
                    capital_allocation="N/A",
                    financial_quality="N/A",
                    competitive_position="N/A",
                    valuation_assessment="N/A",
                    technical_setup="N/A",
                    investment_thesis="N/A",
                    key_positives=[],
                    key_risks=[],
                    recommendation="Hold",
                    conviction_level="Low",
                    sources=result.sources,
                )

                # Wrap in AllocatorReport
                report = AllocatorReport(
                    symbol=symbol,
                    analysis=placeholder_analysis,
                    similar_stocks=result.similar_stocks,
                    sources=result.sources,
                )

                await export_allocator_report_pdf(report, filename, skip_analysis=True)
                self.console.print(f"[green]✓ PDF report saved to {filename}[/green]")

            elif "screened_stocks" in data:
                # ScreeningResult
                result = ScreeningResult(**data)
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

                if not filename:
                    filename = str(reports_dir / f"screening-{timestamp}.pdf")

                export_screening_result_pdf(result, filename)
                self.console.print(f"[green]✓ PDF report saved to {filename}[/green]")

            else:
                self.console.print("[red]Unknown response format. Cannot export.[/red]")

        except json.JSONDecodeError as e:
            self.console.print(f"[red]Error parsing response: {e}[/red]")
        except Exception as e:
            self.console.print(f"[red]Error exporting PDF: {e}[/red]")

    async def handle_input(self, user_input: str) -> bool:
        """Route all input through agent.process() for consistency with REST/MCP."""
        # Try registered commands first (framework + /export)
        if await super().handle_input(user_input):
            return True

        # If it's an unknown slash command, show error
        if user_input.startswith("/"):
            cmd = user_input.split()[0] if user_input.split() else "/"
            self.console.print(f"[red]Unknown command: {cmd}[/red]")
            self.console.print("Type [cyan]/help[/cyan] to see available commands")
            return True

        # Everything else goes to agent.process()
        if not self.session_id:
            self.console.print("[red]Session not initialized[/red]")
            return True

        try:
            session = await self.session_store.get_session(self.session_id)
            if not session:
                self.console.print("[red]Session not found[/red]")
                return True

            agent = cast(AllocatorAgent, await session.use_agent(AllocatorAgent))
            response = await agent.process(user_input, continue_conversation=True)

            # Cache response for export
            self.last_response = response

            # Format JSON as markdown for better readability
            formatted = self._format_json_as_markdown(response)

            self.console.print("\n[bold green]Allocator Agent:[/bold green]")
            self.console.print(Markdown(formatted))
            self.console.print()
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

        return True
