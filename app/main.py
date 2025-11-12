"""
app/main.py

Application entrypoint. Starts FastAPI server and background scheduler.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import settings
from app.api.routes import router
from app.db.models import init_db
from app.scheduler.jobs import scheduled_ingestion, scheduled_health_check

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(settings.LOG_FILE)
    ]
)

logger = logging.getLogger(__name__)

# Global scheduler instance
scheduler = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events.
    """
    # Startup
    logger.info("Starting LinkedIn Ingestor application...")
    
    # Initialize database
    logger.info("Initializing database...")
    init_db()
    logger.info("Database initialized successfully")
    
    # Start scheduler
    global scheduler
    scheduler = BackgroundScheduler(timezone=settings.TIMEZONE)
    
    # Add weekly ingestion job
    if settings.SCHEDULER_ENABLED:
        scheduler.add_job(
            scheduled_ingestion,
            trigger=CronTrigger(
                day_of_week=settings.SCHEDULE_DAY_OF_WEEK,
                hour=settings.SCHEDULE_HOUR,
                minute=2, #settings.SCHEDULE_MINUTE
                timezone=settings.TIMEZONE
            ),
            id='weekly_ingestion',
            name='Weekly LinkedIn Data Ingestion',
            replace_existing=True
        )
        logger.info(
            f"Scheduled weekly ingestion: {settings.SCHEDULE_DAY_OF_WEEK} "
            f"at {settings.SCHEDULE_HOUR:02d}:{settings.SCHEDULE_MINUTE:02d} {settings.TIMEZONE}"
        )
    
    # Add health check job (every 6 hours)
    scheduler.add_job(
        scheduled_health_check,
        trigger=CronTrigger(hour='*/6'),
        id='health_check',
        name='System Health Check',
        replace_existing=True
    )
    logger.info("Scheduled health checks: every 6 hours")
    
    scheduler.start()
    logger.info("Scheduler started successfully")
    
    logger.info(f"Application started - API available at http://0.0.0.0:{settings.PORT}")
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")
    if scheduler and scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler stopped")
    
    logger.info("Application shutdown complete")


# Create FastAPI application
app = FastAPI(
    title="LinkedIn Data Ingestor",
    description="Automated LinkedIn data export processing and normalization pipeline",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware (configure as needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(router, prefix="/api/v1")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "LinkedIn Data Ingestor",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs"
    }


@app.get("/health")
async def health():
    """Basic health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": settings.get_current_timestamp()
    }


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.PORT,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower()
    )