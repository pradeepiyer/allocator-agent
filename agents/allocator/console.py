"""Console interface for Allocator Agent with custom slash commands."""

from pathlib import Path
from typing import cast

from rich.console import Console

from agent_kit.api.console.server import SlashCommands
from agents.allocator.agent import AllocatorAgent
from agents.allocator.models import AllocatorReport


class AllocatorCommands(SlashCommands):
    """Extended commands with Allocator agent integration."""

    def __init__(self, console: Console):
        """Initialize Allocator commands and register agent-specific commands."""
        super().__init__(console)

        # Register allocator agent commands
        self.register_command(
            "/analyse",
            self._handle_analyse,
            "Generate comprehensive allocator report with PDF",
            "Comprehensive analysis with similar stocks and PDF export\nUsage: /analyse <symbol>\nExample: /analyse AAPL",
        )

    def _format_allocator_report(self, report: AllocatorReport) -> str:
        """Format AllocatorReport model as readable markdown."""
        analysis = report.analysis
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

        # Add similar stocks section
        if report.similar_stocks:
            lines.extend(["", "", "# Similar Stocks", ""])
            for i, stock in enumerate(report.similar_stocks, 1):
                lines.extend(
                    [
                        f"## {i}. {stock.symbol} - {stock.company_name} (Similarity: {stock.similarity_score}/100)",
                        "",
                        "### Key Similarities",
                    ]
                )
                for sim in stock.key_similarities:
                    lines.append(f"- {sim}")

                lines.extend(["", "### Key Differences"])
                for diff in stock.key_differences:
                    lines.append(f"- {diff}")

                lines.extend(["", "### Relative Attractiveness", stock.relative_attractiveness, ""])

        # Add sources
        if report.sources:
            lines.extend(["", "## Sources"])
            for i, source in enumerate(report.sources, 1):
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

    async def _handle_analyse(self, args: list[str]) -> None:
        """Handle /analyse command for comprehensive allocator report.

        Args:
            args: Command arguments (should contain stock symbol)
        """
        if not args:
            self.console.print("[dim]Usage: /analyse <symbol>[/dim]")
            self.console.print("[dim]Example: /analyse AAPL[/dim]")
            return

        if not self.session_id:
            self.console.print("[red]Session not initialized[/red]")
            return

        symbol = args[0].upper()
        self.console.print(f"[dim]▶ Generating comprehensive report for {symbol}...[/dim]")
        self.console.print()

        try:
            session = await self.session_store.get_session(self.session_id)
            if not session:
                self.console.print("[red]Session not found[/red]")
                return

            agent = cast(AllocatorAgent, await session.use_agent(AllocatorAgent))
            report = await agent.generate_allocator_report(symbol, continue_conversation=False)

            # Display formatted report
            formatted = self._format_allocator_report(report)
            self.console.print(formatted, markup=False)
            self.console.print()

            # Generate PDF automatically
            from datetime import datetime
            from agents.allocator.export import export_allocator_report_pdf

            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            filename = f"allocator-{symbol}-{timestamp}.pdf"

            reports_dir = Path("reports")
            reports_dir.mkdir(exist_ok=True)
            filepath = reports_dir / filename

            self.console.print("[dim]▶ Generating PDF report...[/dim]")
            await export_allocator_report_pdf(report, str(filepath))
            self.console.print(f"[green]✓ PDF report saved to {filepath}[/green]")
            self.console.print()

        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

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
