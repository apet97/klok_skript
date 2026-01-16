import typer
import getpass
from pathlib import Path
from src.sync_engine import ClockifySyncEngine
from typing import Optional

app = typer.Typer(help="Enterprise Clockify User Sync & Governance Tool")

@app.command()
def sync(
    csv_path: Path = typer.Argument(..., help="Path to the source-of-truth CSV file", exists=True, readable=True),
    config: Path = typer.Option("config.yaml", "--config", "-c", help="Path to config.yaml"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Report planned changes without mutating data"),
    cleanup: bool = typer.Option(False, "--cleanup", help="Delete non-managed groups (CAUTION)"),
    deactivate: bool = typer.Option(False, "--deactivate", help="Deactivate users missing from CSV"),
):
    """
    Synchronize Clockify users, groups, and manager hierarchies from CSV.
    """
    typer.secho("\n🔐 Credentials Required", fg=typer.colors.CYAN, bold=True)
    api_key = getpass.getpass("Enter Clockify API Key: ")
    workspace_id = typer.prompt("Enter Workspace ID")

    if not api_key or not workspace_id:
        typer.secho("❌ Credentials missing. Aborting.", fg=typer.colors.RED)
        raise typer.Exit(1)

    if cleanup or deactivate:
        typer.secho("\n⚠️  CAUTION: Destructive Actions Requested", fg=typer.colors.YELLOW, bold=True)
        confirm = typer.prompt("Type 'I UNDERSTAND' to proceed")
        if confirm != "I UNDERSTAND":
            typer.secho("❌ Aborted by user.", fg=typer.colors.RED)
            raise typer.Exit(1)

    engine = ClockifySyncEngine(
        config_path=str(config),
        api_key=api_key,
        workspace_id=workspace_id,
        dry_run=dry_run
    )

    try:
        engine.run(csv_path=str(csv_path), cleanup=cleanup, deactivate=deactivate)
    except Exception as e:
        typer.secho(f"\n❌ Sync failed: {e}", fg=typer.colors.RED)
        raise typer.Exit(1)

if __name__ == "__main__":
    app()
