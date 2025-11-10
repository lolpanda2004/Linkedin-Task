"""
app/cli.py

Command-line interface for manual LinkedIn data ingestion operations.

Usage:
    python -m app.cli run-once --zip path/to/export.zip
    python -m app.cli run-once  # Uses latest from incoming/
    python -m app.cli health
    python -m app.cli list-archived
    python -m app.cli list-output
    python -m app.cli db-stats
    python -m app.cli test-email
"""

import sys
import logging
from pathlib import Path
from typing import Optional
import click

from app.config import settings
from app.scheduler.jobs import ingestion_job, health_check_job
from app.services.storage import storage_service
from app.services.reconcile import reconciliation_service
from app.services.emailer import emailer_service
from app.db.models import init_db
from app.db.repo import repository

# Configure logging for CLI
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(settings.LOG_FILE)
    ]
)

logger = logging.getLogger(__name__)


@click.group()
def cli():
    """LinkedIn Data Ingestor CLI - Manual operations and utilities."""
    pass


@cli.command()
@click.option(
    '--zip',
    'zip_path',
    type=click.Path(exists=True),
    help='Path to LinkedIn export ZIP file. If not provided, uses latest from incoming/'
)
@click.option(
    '--init-db',
    is_flag=True,
    default=False,
    help='Initialize database before running (creates tables if needed)'
)
def run_once(zip_path: Optional[str], init_db_flag: bool):
    """
    Run ingestion once for a specific or latest ZIP file.
    
    Examples:
        python -m app.cli run-once --zip /path/to/export.zip
        python -m app.cli run-once  # Uses latest from incoming/
        python -m app.cli run-once --init-db --zip export.zip
    """
    click.echo("=" * 70)
    click.echo("LinkedIn Data Ingestor - Manual Run")
    click.echo("=" * 70)
    
    # Initialize database if requested
    if init_db_flag:
        click.echo("\n📦 Initializing database...")
        init_db()
        click.echo("✓ Database initialized")
    
    # Run ingestion
    click.echo("\n🚀 Starting ingestion...\n")
    
    try:
        run_status = ingestion_job.run_ingestion(zip_path=zip_path)
        
        # Display results
        click.echo("\n" + "=" * 70)
        if run_status["status"] == "SUCCESS":
            click.echo("✓ INGESTION COMPLETED SUCCESSFULLY")
            click.secho("Status: SUCCESS", fg="green", bold=True)
        else:
            click.echo("✗ INGESTION FAILED")
            click.secho(f"Status: {run_status['status']}", fg="red", bold=True)
        click.echo("=" * 70)
        
        # Display stats
        click.echo(f"\nRun ID: {run_status['run_id']}")
        click.echo(f"Start Time: {run_status['start_time']}")
        click.echo(f"End Time: {run_status['end_time']}")
        
        if "stats" in run_status and run_status["stats"]:
            click.echo("\n📊 Statistics:")
            stats = run_status["stats"]
            
            if "source_file" in stats:
                click.echo(f"  Source File: {stats['source_file']}")
            
            if "db_counts" in stats:
                click.echo("\n  Database Records Inserted:")
                for table, count in stats["db_counts"].items():
                    click.echo(f"    • {table}: {count:,}")
            
            if "reconciliation" in stats:
                recon = stats["reconciliation"]
                click.echo(f"\n  Reconciliation: {recon['status']}")
                click.echo(f"    Matched: {recon['matched_tables']}/{recon['total_tables']} tables")
            
            if "output_file" in stats:
                click.echo(f"\n  Output Package: {stats['output_file']}")
            
            if "email_sent" in stats:
                if stats["email_sent"]:
                    click.echo("  ✓ Email sent successfully")
                else:
                    click.echo("  ⚠ Email not sent")
        
        # Display error if present
        if run_status.get("error"):
            click.echo("\n❌ Error Details:")
            click.secho(run_status["error"], fg="red")
            
            if run_status.get("traceback"):
                click.echo("\nTraceback:")
                click.echo(run_status["traceback"])
        
        click.echo("\n" + "=" * 70)
        
        # Exit with appropriate code
        sys.exit(0 if run_status["status"] == "SUCCESS" else 1)
        
    except KeyboardInterrupt:
        click.echo("\n\n⚠ Ingestion interrupted by user")
        sys.exit(130)
    except Exception as e:
        click.echo(f"\n❌ Unexpected error: {e}", err=True)
        logger.exception("CLI run-once failed")
        sys.exit(1)


@cli.command()
def health():
    """
    Run system health check.
    
    Checks database connectivity, storage, and email configuration.
    """
    click.echo("🏥 Running health check...\n")
    
    try:
        health_status = health_check_job.run_health_check()
        
        # Display results
        if health_status["status"] == "HEALTHY":
            click.secho(f"✓ System Status: {health_status['status']}", fg="green", bold=True)
        else:
            click.secho(f"⚠ System Status: {health_status['status']}", fg="yellow", bold=True)
        
        click.echo(f"Timestamp: {health_status['timestamp']}\n")
        
        # Component status
        click.echo("Components:")
        for component, status in health_status["components"].items():
            if status == "OK" or status == "CONFIGURED":
                click.echo(f"  ✓ {component}: {status}")
            elif status == "DISABLED":
                click.echo(f"  ⊝ {component}: {status}")
            else:
                click.secho(f"  ✗ {component}: {status}", fg="red")
        
        # Storage stats if available
        if "storage_stats" in health_status:
            click.echo("\nStorage Statistics:")
            stats = health_status["storage_stats"]
            for location, data in stats.items():
                click.echo(f"  {location}:")
                click.echo(f"    Files: {data['count']}")
                click.echo(f"    Size: {data['total_size_mb']:.2f} MB")
        
        sys.exit(0 if health_status["status"] == "HEALTHY" else 1)
        
    except Exception as e:
        click.secho(f"❌ Health check failed: {e}", fg="red", err=True)
        logger.exception("Health check failed")
        sys.exit(1)


@cli.command()
@click.option('--limit', default=10, help='Maximum number of files to list')
def list_archived(limit: int):
    """List recently archived raw ZIP files."""
    click.echo(f"📦 Recently Archived Files (limit: {limit}):\n")
    
    try:
        files = storage_service.list_archived_zips(limit=limit)
        
        if not files:
            click.echo("  No archived files found")
        else:
            for i, file_info in enumerate(files, 1):
                click.echo(f"{i}. {file_info['filename']}")
                click.echo(f"   Size: {file_info['size_mb']:.2f} MB")
                click.echo(f"   Modified: {file_info['modified']}")
                click.echo()
        
    except Exception as e:
        click.secho(f"❌ Failed to list archived files: {e}", fg="red", err=True)
        sys.exit(1)


@cli.command()
@click.option('--limit', default=10, help='Maximum number of files to list')
def list_output(limit: int):
    """List recently generated output ZIP files."""
    click.echo(f"📤 Recently Generated Output Files (limit: {limit}):\n")
    
    try:
        files = storage_service.list_output_zips(limit=limit)
        
        if not files:
            click.echo("  No output files found")
        else:
            for i, file_info in enumerate(files, 1):
                click.echo(f"{i}. {file_info['filename']}")
                click.echo(f"   Size: {file_info['size_mb']:.2f} MB")
                click.echo(f"   Modified: {file_info['modified']}")
                click.echo()
        
    except Exception as e:
        click.secho(f"❌ Failed to list output files: {e}", fg="red", err=True)
        sys.exit(1)


@cli.command()
def db_stats():
    """Display database statistics (record counts per table)."""
    click.echo("📊 Database Statistics:\n")
    
    try:
        stats = repository.get_table_counts()
        
        if not stats:
            click.echo("  No tables found or database is empty")
        else:
            total = 0
            for table_name, count in sorted(stats.items()):
                click.echo(f"  {table_name}: {count:,} records")
                total += count
            
            click.echo(f"\n  Total Records: {total:,}")
        
    except Exception as e:
        click.secho(f"❌ Failed to get database stats: {e}", fg="red", err=True)
        logger.exception("Failed to get database stats")
        sys.exit(1)


@cli.command()
def test_email():
    """Test email configuration."""
    click.echo("📧 Testing email configuration...\n")
    
    if not emailer_service.enabled:
        click.secho("⚠ Email is not enabled or configured", fg="yellow")
        click.echo("\nCheck your .env file for:")
        click.echo("  - EMAIL_ENABLED=true")
        click.echo("  - SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD")
        click.echo("  - FROM_EMAIL, RECIPIENT_EMAILS")
        sys.exit(1)
    
    try:
        click.echo(f"SMTP Host: {emailer_service.smtp_host}")
        click.echo(f"SMTP Port: {emailer_service.smtp_port}")
        click.echo(f"SMTP User: {emailer_service.smtp_user}")
        click.echo(f"From Email: {emailer_service.from_email}")
        click.echo()
        
        success = emailer_service.test_connection()
        
        if success:
            click.secho("✓ Email configuration test PASSED", fg="green", bold=True)
            click.echo("SMTP connection and authentication successful")
            sys.exit(0)
        else:
            click.secho("✗ Email configuration test FAILED", fg="red", bold=True)
            sys.exit(1)
        
    except Exception as e:
        click.secho(f"❌ Email test failed: {e}", fg="red", err=True)
        logger.exception("Email test failed")
        sys.exit(1)


@cli.command()
@click.option('--limit', default=5, help='Maximum number of reports to show')
@click.option('--detailed', is_flag=True, help='Show detailed report')
def reconciliation(limit: int, detailed: bool):
    """View reconciliation reports."""
    click.echo(f"🔍 Reconciliation Reports (limit: {limit}):\n")
    
    try:
        reports = reconciliation_service.get_report_history(limit=limit)
        
        if not reports:
            click.echo("  No reconciliation reports found")
            sys.exit(0)
        
        for i, report in enumerate(reports, 1):
            click.echo(f"\n{i}. Run ID: {report.get('run_id', 'N/A')}")
            click.echo(f"   Timestamp: {report['timestamp']}")
            
            status = report['status']
            if status == "SUCCESS":
                click.secho(f"   Status: {status}", fg="green")
            elif status == "PARTIAL":
                click.secho(f"   Status: {status}", fg="yellow")
            else:
                click.secho(f"   Status: {status}", fg="red")
            
            summary = report['summary']
            click.echo(f"   Tables Matched: {summary['tables_matched']}/{summary['total_tables']}")
            click.echo(f"   Total Records: Source={summary['total_source_records']:,}, DB={summary['total_db_records']:,}")
            
            if detailed:
                click.echo("\n   Table Details:")
                for table_name, table_report in report['tables'].items():
                    match_icon = "✓" if table_report['matched'] else "✗"
                    click.echo(f"     {match_icon} {table_name}: Source={table_report['source_count']}, DB={table_report['db_count']}")
            
            click.echo("   " + "-" * 60)
        
    except Exception as e:
        click.secho(f"❌ Failed to get reconciliation reports: {e}", fg="red", err=True)
        sys.exit(1)


@cli.command()
def init_database():
    """Initialize database (create tables)."""
    click.echo("📦 Initializing database...\n")
    
    try:
        init_db()
        click.secho("✓ Database initialized successfully", fg="green", bold=True)
        click.echo(f"Database URL: {settings.DATABASE_URL}")
        
    except Exception as e:
        click.secho(f"❌ Database initialization failed: {e}", fg="red", err=True)
        logger.exception("Database initialization failed")
        sys.exit(1)


if __name__ == '__main__':
    cli()