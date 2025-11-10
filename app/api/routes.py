"""
app/api/routes.py

FastAPI routes for manual control and status monitoring.
"""

import logging
from typing import Optional
from pathlib import Path

from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel, Field

from app.scheduler.jobs import ingestion_job, health_check_job
from app.services.storage import storage_service
from app.services.reconcile import reconciliation_service
from app.db.repo import DatabaseRepository

logger = logging.getLogger(__name__)

router = APIRouter()


# Request/Response Models
class RunIngestionRequest(BaseModel):
    """Request model for triggering ingestion."""
    zip_path: Optional[str] = Field(
        None,
        description="Optional path to specific ZIP file. If not provided, uses latest from incoming/"
    )


class RunIngestionResponse(BaseModel):
    """Response model for ingestion trigger."""
    message: str
    run_id: str
    status: str


class LastRunResponse(BaseModel):
    """Response model for last run status."""
    run_id: str
    start_time: str
    end_time: Optional[str]
    status: str
    stage: Optional[str]
    stats: dict
    error: Optional[str] = None


class HealthResponse(BaseModel):
    """Response model for detailed health check."""
    timestamp: str
    status: str
    components: dict
    storage_stats: Optional[dict] = None


class StorageStatsResponse(BaseModel):
    """Response model for storage statistics."""
    incoming: dict
    raw_archive: dict
    output: dict


# Routes

@router.get("/health", response_model=HealthResponse)
async def get_health():
    """
    Get detailed system health status.
    
    Returns health status of all components including database, storage, and email.
    """
    try:
        health_status = health_check_job.run_health_check()
        return health_status
    except Exception as e:
        logger.error(f"Health check failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")


@router.post("/run-now", response_model=RunIngestionResponse)
async def run_now(
    request: RunIngestionRequest,
    background_tasks: BackgroundTasks
):
    """
    Trigger data ingestion immediately.
    
    This endpoint starts ingestion in the background and returns immediately.
    Use /last-run to check status.
    
    Args:
        request: Optional ZIP file path
        
    Returns:
        Confirmation with run ID
    """
    # Check if already running
    if ingestion_job.is_running():
        raise HTTPException(
            status_code=409,
            detail="Ingestion job is already running"
        )
    
    # Validate ZIP path if provided
    if request.zip_path:
        zip_file = Path(request.zip_path)
        if not zip_file.exists():
            raise HTTPException(
                status_code=404,
                detail=f"ZIP file not found: {request.zip_path}"
            )
    
    # Run in background
    def run_task():
        ingestion_job.run_ingestion(zip_path=request.zip_path)
    
    background_tasks.add_task(run_task)
    
    # Generate temporary run ID for response
    import uuid
    run_id = str(uuid.uuid4())[:8]
    
    logger.info(f"Ingestion triggered via API [run_id: {run_id}]")
    
    return RunIngestionResponse(
        message="Ingestion started in background",
        run_id=run_id,
        status="STARTED"
    )


@router.get("/last-run", response_model=Optional[LastRunResponse])
async def get_last_run():
    """
    Get status of the last completed ingestion run.
    
    Returns:
        Last run status with stats and reconciliation info, or null if no runs yet
    """
    last_run = ingestion_job.get_last_run_status()
    
    if not last_run:
        return None
    
    return LastRunResponse(**last_run)


@router.get("/current-run")
async def get_current_run():
    """
    Check if an ingestion job is currently running.
    
    Returns:
        Current run status or null if not running
    """
    if ingestion_job.is_running():
        return {
            "running": True,
            "run_id": ingestion_job.current_run_id,
            "message": "Ingestion in progress"
        }
    else:
        return {
            "running": False,
            "run_id": None,
            "message": "No ingestion currently running"
        }


@router.get("/storage/stats", response_model=StorageStatsResponse)
async def get_storage_stats():
    """
    Get storage statistics for all directories.
    
    Returns:
        File counts and sizes for incoming, raw_archive, and output directories
    """
    try:
        stats = storage_service.get_storage_stats()
        return stats
    except Exception as e:
        logger.error(f"Failed to get storage stats: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve storage stats: {str(e)}"
        )


@router.get("/storage/archived")
async def list_archived_files(limit: int = Query(10, ge=1, le=100)):
    """
    List recently archived raw ZIP files.
    
    Args:
        limit: Maximum number of files to return (1-100)
        
    Returns:
        List of archived files with metadata
    """
    try:
        files = storage_service.list_archived_zips(limit=limit)
        return {"files": files, "count": len(files)}
    except Exception as e:
        logger.error(f"Failed to list archived files: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list archived files: {str(e)}"
        )


@router.get("/storage/output")
async def list_output_files(limit: int = Query(10, ge=1, le=100)):
    """
    List recently generated output ZIP files.
    
    Args:
        limit: Maximum number of files to return (1-100)
        
    Returns:
        List of output files with metadata
    """
    try:
        files = storage_service.list_output_zips(limit=limit)
        return {"files": files, "count": len(files)}
    except Exception as e:
        logger.error(f"Failed to list output files: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list output files: {str(e)}"
        )


@router.get("/reconciliation/last")
async def get_last_reconciliation():
    """
    Get the most recent reconciliation report.
    
    Returns:
        Last reconciliation report or null if no reports exist
    """
    report = reconciliation_service.get_last_report()
    if not report:
        return None
    return report


@router.get("/reconciliation/history")
async def get_reconciliation_history(limit: int = Query(10, ge=1, le=50)):
    """
    Get recent reconciliation reports.
    
    Args:
        limit: Maximum number of reports to return (1-50)
        
    Returns:
        List of reconciliation reports (most recent first)
    """
    reports = reconciliation_service.get_report_history(limit=limit)
    return {"reports": reports, "count": len(reports)}


@router.get("/db/stats")
async def get_database_stats():
    """
    Get database statistics (record counts per table).
    
    Returns:
        Record counts for all tables
    """
    try:
        stats = repository.get_table_counts()
        return {
            "tables": stats,
            "total_records": sum(stats.values())
        }
    except Exception as e:
        logger.error(f"Failed to get database stats: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve database stats: {str(e)}"
        )


@router.post("/db/vacuum")
async def vacuum_database():
    """
    Vacuum/optimize database (SQLite only).
    
    Returns:
        Confirmation message
    """
    try:
        repository.vacuum()
        return {"message": "Database vacuumed successfully"}
    except Exception as e:
        logger.error(f"Failed to vacuum database: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to vacuum database: {str(e)}"
        )


@router.get("/config")
async def get_config():
    """
    Get current configuration (sanitized, no secrets).
    
    Returns:
        Configuration dictionary with sensitive values masked
    """
    from app.config import settings
    
    config = {
        "scheduler_enabled": settings.SCHEDULER_ENABLED,
        "schedule": {
            "day_of_week": settings.SCHEDULE_DAY_OF_WEEK,
            "hour": settings.SCHEDULE_HOUR,
            "minute": settings.SCHEDULE_MINUTE,
            "timezone": settings.TIMEZONE
        },
        "email_enabled": settings.EMAIL_ENABLED,
        "smtp_host": settings.SMTP_HOST,
        "smtp_port": settings.SMTP_PORT,
        "from_email": settings.FROM_EMAIL,
        "recipient_count": len(settings.RECIPIENT_EMAILS) if settings.RECIPIENT_EMAILS else 0,
        "delete_incoming_after_processing": settings.DELETE_INCOMING_AFTER_PROCESSING,
        "database_url": settings.DATABASE_URL.split("@")[-1] if "@" in settings.DATABASE_URL else "sqlite",
        "log_level": settings.LOG_LEVEL
    }
    
    return config


@router.post("/test/email")
async def test_email():
    """
    Test email configuration by attempting connection.
    
    Returns:
        Test result
    """
    from app.services.emailer import emailer_service
    
    if not emailer_service.enabled:
        raise HTTPException(
            status_code=400,
            detail="Email is not enabled or configured"
        )
    
    try:
        success = emailer_service.test_connection()
        if success:
            return {"message": "Email configuration test successful"}
        else:
            raise HTTPException(
                status_code=500,
                detail="Email configuration test failed"
            )
    except Exception as e:
        logger.error(f"Email test failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Email test failed: {str(e)}"
        )