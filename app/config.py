# app/config.py
"""
app/config.py
Configuration management using environment variables.
Loads settings from .env file and provides typed configuration objects.
"""
import os
from pathlib import Path
from typing import List, Optional
from datetime import datetime
import pytz
from pydantic_settings import BaseSettings
from pydantic import Field, field_validator

class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # ==================== APPLICATION ====================
    APP_NAME: str = "LinkedIn Data Ingestor"
    DEBUG: bool = Field(default=False, description="Enable debug mode")
    PORT: int = Field(default=8000, description="API server port")
    LOG_LEVEL: str = Field(default="INFO", description="Logging level")
    LOG_FILE: str = Field(default="app/logs/ingestor.log", description="Log file path")
    
    # ==================== DIRECTORIES ====================
    BASE_DIR: Path = Field(default_factory=lambda: Path(__file__).parent.parent)
    INCOMING_DIR: str = Field(default="data/incoming", description="Directory for incoming LinkedIn exports")
    RAW_ZIP_DIR: str = Field(default="data/raw_zip", description="Archive directory for processed raw ZIPs")
    OUT_ZIP_DIR: str = Field(default="data/out_zip", description="Directory for generated output packages")
    
    @property
    def incoming_path(self) -> Path:
        """Get absolute path for incoming directory."""
        return self.BASE_DIR / self.INCOMING_DIR
    
    @property
    def raw_zip_path(self) -> Path:
        """Get absolute path for raw zip archive directory."""
        return self.BASE_DIR / self.RAW_ZIP_DIR
    
    @property
    def out_zip_path(self) -> Path:
        """Get absolute path for output zip directory."""
        return self.BASE_DIR / self.OUT_ZIP_DIR
    
    # ==================== DATABASE ====================
    DATABASE_URL: str = Field(
        default="sqlite:///./linkedin_data.db",
        description="Database connection URL (SQLite or PostgreSQL)"
    )
    DB_ECHO: bool = Field(default=False, description="Echo SQL queries (debug)")
    DB_POOL_SIZE: int = Field(default=5, description="Database connection pool size")
    DB_MAX_OVERFLOW: int = Field(default=10, description="Max overflow connections")
    
    # ==================== SCHEDULER ====================
    SCHEDULER_ENABLED: bool = Field(default=True, description="Enable automatic scheduled ingestion")
    SCHEDULE_DAY_OF_WEEK: str = Field(default="mon", description="Day of week for ingestion (mon-sun)")
    SCHEDULE_HOUR: int = Field(default=9, ge=0, le=23, description="Hour for ingestion (0-23)")
    SCHEDULE_MINUTE: int = Field(default=0, ge=0, le=59, description="Minute for ingestion (0-59)")
    TIMEZONE: str = Field(default="UTC", description="Timezone for scheduler")
    
    @field_validator('TIMEZONE')
    @classmethod
    def validate_timezone(cls, v):
        """Validate timezone string."""
        try:
            pytz.timezone(v)
            return v
        except pytz.exceptions.UnknownTimeZoneError:
            raise ValueError(f"Invalid timezone: {v}")
    
    @field_validator('SCHEDULE_DAY_OF_WEEK')
    @classmethod
    def validate_day_of_week(cls, v):
        """Validate day of week."""
        valid_days = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
        v_lower = v.lower()
        if v_lower not in valid_days:
            raise ValueError(f"Invalid day of week: {v}. Must be one of {valid_days}")
        return v_lower
        
    
    # ==================== EMAIL ====================
    EMAIL_ENABLED: bool = Field(default=False, description="Enable email notifications")
    SMTP_HOST: str = Field(default="", description="SMTP server host")
    SMTP_PORT: int = Field(default=587, description="SMTP server port")
    SMTP_USER: str = Field(default="", description="SMTP username/email")
    SMTP_PASSWORD: str = Field(default="", description="SMTP password/app password")
    SMTP_USE_TLS: bool = Field(default=True, description="Use TLS for SMTP")
    FROM_EMAIL: str = Field(default="", description="From email address")
    RECIPIENT_EMAILS: str = Field(default="", description="Comma-separated recipient emails")
    
    # ==================== PROCESSING ====================
    DELETE_INCOMING_AFTER_PROCESSING: bool = Field(
        default=False,
        description="Delete ZIP from incoming/ after successful processing"
    )
    MAX_RECORDS_PER_BATCH: int = Field(
        default=1000,
        description="Maximum records to insert in single DB transaction"
    )
    ENABLE_DATA_VALIDATION: bool = Field(
        default=True,
        description="Enable data quality validation"
    )
    
    # ==================== CORS ====================
    CORS_ORIGINS: str = Field(
        default="http://localhost:3000,http://localhost:8000",
        description="Comma-separated CORS origins"
    )
    
    # ==================== RETRY LOGIC ====================
    MAX_RETRIES: int = Field(default=3, description="Max retries for failed operations")
    RETRY_DELAY_SECONDS: int = Field(default=5, description="Delay between retries in seconds")
    
    # ==================== LINKEDIN SPECIFIC ====================
    EXPECTED_TABLES: str = Field(
        default="participants,conversations,messages,connections,profile,reactions",
        description="Comma-separated expected table names in LinkedIn export"
    )
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "ignore"
    }
    
    # ==================== COMPUTED PROPERTIES ====================
    
    @property
    def recipient_emails_list(self) -> List[str]:
        """Parse RECIPIENT_EMAILS into list."""
        if not self.RECIPIENT_EMAILS:
            return []
        return [email.strip() for email in self.RECIPIENT_EMAILS.split(',') if email.strip()]
    
    @property
    def cors_origins_list(self) -> List[str]:
        """Parse CORS_ORIGINS into list."""
        if not self.CORS_ORIGINS:
            return ["http://localhost:3000", "http://localhost:8000"]
        return [origin.strip() for origin in self.CORS_ORIGINS.split(',') if origin.strip()]
    
    @property
    def expected_tables_list(self) -> List[str]:
        """Parse EXPECTED_TABLES into list."""
        if not self.EXPECTED_TABLES:
            return ["participants", "conversations", "messages", "connections", "profile", "reactions"]
        return [table.strip() for table in self.EXPECTED_TABLES.split(',') if table.strip()]
    
    # ==================== UTILITY METHODS ====================
    
    def get_current_timestamp(self) -> str:
        """Get current timestamp in configured timezone."""
        tz = pytz.timezone(self.TIMEZONE)
        return datetime.now(tz).isoformat()
    
    def get_schedule_description(self) -> str:
        """Get human-readable schedule description."""
        day_names = {
            'mon': 'Monday',
            'tue': 'Tuesday',
            'wed': 'Wednesday',
            'thu': 'Thursday',
            'fri': 'Friday',
            'sat': 'Saturday',
            'sun': 'Sunday'
        }
        day = day_names.get(self.SCHEDULE_DAY_OF_WEEK, self.SCHEDULE_DAY_OF_WEEK)
        return f"Every {day} at {self.SCHEDULE_HOUR:02d}:{self.SCHEDULE_MINUTE:02d} {self.TIMEZONE}"
    
    def ensure_directories(self):
        """Create all required directories if they don't exist."""
        directories = [
            self.incoming_path,
            self.raw_zip_path,
            self.out_zip_path,
            Path(self.LOG_FILE).parent
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def validate_email_config(self) -> tuple[bool, Optional[str]]:
        """
        Validate email configuration.
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not self.EMAIL_ENABLED:
            return True, None
        
        required_fields = {
            "SMTP_HOST": self.SMTP_HOST,
            "SMTP_USER": self.SMTP_USER,
            "SMTP_PASSWORD": self.SMTP_PASSWORD,
            "FROM_EMAIL": self.FROM_EMAIL,
        }
        
        missing = [key for key, value in required_fields.items() if not value]
        
        if missing:
            return False, f"Missing required email configuration: {', '.join(missing)}"
        
        if not self.recipient_emails_list:
            return False, "No recipient emails configured (RECIPIENT_EMAILS)"
        
        return True, None
    
    def get_database_type(self) -> str:
        """Get database type from connection URL."""
        if self.DATABASE_URL.startswith("sqlite"):
            return "sqlite"
        elif self.DATABASE_URL.startswith("postgresql"):
            return "postgresql"
        else:
            return "unknown"
    
    def __repr__(self) -> str:
        """String representation (without sensitive data)."""
        return (
            f"Settings(APP_NAME='{self.APP_NAME}', "
            f"DEBUG={self.DEBUG}, "
            f"DATABASE={self.get_database_type()}, "
            f"EMAIL_ENABLED={self.EMAIL_ENABLED}, "
            f"SCHEDULER_ENABLED={self.SCHEDULER_ENABLED})"
        )

# Singleton instance
settings = Settings()

# Ensure directories exist on import
settings.ensure_directories()