"""Console interface for Allocator Agent with custom slash commands."""

from typing import cast

from rich.console import Console

from agent_kit.api.console.server import SlashCommands
from allocator.agent import AllocatorAgent
from allocator.models import SimilarStocksResult, StockAnalysis


class AllocatorCommands(SlashCommands):
    """Extended commands with Allocator agent integration."""

    def __init__(self, console: Console):
        """Initialize Allocator commands and register agent-specific commands."""
        super().__init__(console)

        # Track last analysis result for export
        self.last_result: StockAnalysis | SimilarStocksResult | None = None
        self.last_symbol: str | None = None

        # Register allocator agent commands
        self.register_command(
            "/analyze",
            self._handle_analyze,
            "Analyze a stock comprehensively",
            "Comprehensive stock analysis\nUsage: /analyze <symbol>\nExample: /analyze AAPL"
        )

        self.register_command(
            "/similar",
            self._handle_similar,
            "Find stocks similar to given symbol",
            "Find similar stocks\nUsage: /similar <symbol>\nExample: /similar MSFT"
        )

        self.register_command(
            "/export",
            self._handle_export,
            "Export last analysis to PDF",
            "Export analysis to PDF\nUsage: /export [filename]\nExample: /export AAPL-analysis.pdf"
        )

    def _format_stock_analysis(self, analysis: StockAnalysis) -> str:
        """Format StockAnalysis model as readable markdown."""
        lines = [
            f"# {analysis.symbol} - {analysis.company_name}",
            "",
            f"**Recommendation:** {analysis.recommendation} (Conviction: {analysis.conviction_level})",
            "",
            "## Investment Thesis",
            analysis.investment_thesis,
            "",
            "## Management Quality",
            analysis.management_quality,
            "",
            "## Capital Allocation",
            analysis.capital_allocation,
            "",
            "## Financial Quality",
            analysis.financial_quality,
            "",
            "## Competitive Position",
            analysis.competitive_position,
            "",
            "## Valuation Assessment",
            analysis.valuation_assessment,
            "",
            "## Technical Setup",
            analysis.technical_setup,
            "",
            "## Key Positives",
        ]
        for positive in analysis.key_positives:
            lines.append(f"- {positive}")

        lines.extend(["", "## Key Risks"])
        for risk in analysis.key_risks:
            lines.append(f"- {risk}")

        if analysis.sources:
            lines.extend(["", "## Sources"])
            for i, source in enumerate(analysis.sources, 1):
                lines.append(f"[{i}] {source}")

        return "\n".join(lines)

    def _format_similar_stocks(self, result: SimilarStocksResult) -> str:
        """Format SimilarStocksResult model as readable markdown."""
        lines = [
            f"# Similar Stocks to {result.reference_symbol}",
            "",
        ]

        for i, stock in enumerate(result.similar_stocks, 1):
            lines.extend([
                f"## {i}. {stock.symbol} - {stock.company_name} (Similarity: {stock.similarity_score}/100)",
                "",
                "### Key Similarities",
            ])
            for sim in stock.key_similarities:
                lines.append(f"- {sim}")

            lines.extend(["", "### Key Differences"])
            for diff in stock.key_differences:
                lines.append(f"- {diff}")

            lines.extend([
                "",
                "### Relative Attractiveness",
                stock.relative_attractiveness,
                "",
            ])

        if result.sources:
            lines.extend(["## Sources", ""])
            for i, source in enumerate(result.sources, 1):
                lines.append(f"[{i}] {source}")

        return "\n".join(lines)

    async def handle_input(self, user_input: str) -> bool:
        """Handle commands via registry, then fall back to chat.

        Args:
            user_input: User input string

        Returns:
            True if input was handled, False otherwise
        """
        # Try registered commands first (framework + agent)
        if await super().handle_input(user_input):
            return True

        # If it's an unknown slash command, show error
        if user_input.startswith("/"):
            cmd = user_input.split()[0] if user_input.split() else "/"
            self.console.print(f"[red]Unknown command: {cmd}[/red]")
            self.console.print("Type [cyan]/help[/cyan] to see available commands")
            return True

        # Handle non-command input as chat with AllocatorAgent
        await self._handle_chat(user_input)
        return True

    async def _handle_analyze(self, args: list[str]) -> None:
        """Handle /analyze command for comprehensive stock analysis.

        Args:
            args: Command arguments (should contain stock symbol)
        """
        if not args:
            self.console.print("[dim]Usage: /analyze <symbol>[/dim]")
            self.console.print("[dim]Example: /analyze AAPL[/dim]")
            return

        if not self.session_id:
            self.console.print("[red]Session not initialized[/red]")
            return

        symbol = args[0].upper()
        self.console.print(f"[dim]▶ Analyzing {symbol}...[/dim]")
        self.console.print()

        try:
            session = await self.session_store.get_session(self.session_id)
            if not session:
                self.console.print("[red]Session not found[/red]")
                return

            agent = cast(AllocatorAgent, await session.use_agent(AllocatorAgent))
            analysis = await agent.analyze_stock(symbol, continue_conversation=False)

            # Store for export
            self.last_result = analysis
            self.last_symbol = symbol

            formatted = self._format_stock_analysis(analysis)
            self.console.print(formatted, markup=False)
            self.console.print()
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    async def _handle_similar(self, args: list[str]) -> None:
        """Handle /similar command to find similar stocks.

        Args:
            args: Command arguments (should contain stock symbol)
        """
        if not args:
            self.console.print("[dim]Usage: /similar <symbol>[/dim]")
            self.console.print("[dim]Example: /similar MSFT[/dim]")
            return

        if not self.session_id:
            self.console.print("[red]Session not initialized[/red]")
            return

        symbol = args[0].upper()
        self.console.print(f"[dim]▶ Finding stocks similar to {symbol}...[/dim]")
        self.console.print()

        try:
            session = await self.session_store.get_session(self.session_id)
            if not session:
                self.console.print("[red]Session not found[/red]")
                return

            agent = cast(AllocatorAgent, await session.use_agent(AllocatorAgent))
            results = await agent.find_similar_stocks(symbol, continue_conversation=False)

            # Store for export
            self.last_result = results
            self.last_symbol = symbol

            formatted = self._format_similar_stocks(results)
            self.console.print(formatted, markup=False)
            self.console.print()
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    async def _handle_export(self, args: list[str]) -> None:
        """Handle /export command to save analysis to PDF.

        Args:
            args: Command arguments (optional filename)
        """
        if not self.last_result:
            self.console.print("[yellow]No analysis to export. Run /analyze or /similar first.[/yellow]")
            return

        # Generate filename if not provided
        if args:
            filename = args[0]
            if not filename.endswith(".pdf"):
                filename += ".pdf"
        else:
            from datetime import datetime

            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            if isinstance(self.last_result, StockAnalysis):
                filename = f"analysis-{self.last_symbol}-{timestamp}.pdf"
            else:  # SimilarStocksResult
                filename = f"similar-{self.last_symbol}-{timestamp}.pdf"

        try:
            from allocator.export import (
                export_similar_stocks_pdf,
                export_stock_analysis_pdf,
            )

            self.console.print(f"[dim]▶ Exporting to {filename}...[/dim]")

            if isinstance(self.last_result, StockAnalysis):
                export_stock_analysis_pdf(self.last_result, filename, self.last_symbol or "UNKNOWN")
            else:  # SimilarStocksResult
                export_similar_stocks_pdf(self.last_result, filename)

            self.console.print(f"[green]✓ Exported to {filename}[/green]")
            self.console.print()
        except Exception as e:
            self.console.print(f"[red]Error exporting PDF: {e}[/red]")

    async def _handle_chat(self, user_input: str) -> None:
        """Handle chat input using AllocatorAgent.

        Args:
            user_input: User input string
        """
        if not self.session_id:
            self.console.print("[red]Session not initialized[/red]")
            return

        try:
            session = await self.session_store.get_session(self.session_id)
            if not session:
                self.console.print("[red]Session not found[/red]")
                return

            agent = cast(AllocatorAgent, await session.use_agent(AllocatorAgent))
            response = await agent.process(user_input, continue_conversation=True)

            self.console.print("\n[bold green]Allocator Agent:[/bold green]")
            self.console.print(response, markup=False)
            self.console.print()
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")
