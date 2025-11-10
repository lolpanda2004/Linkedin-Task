"""
app/services/storage.py

Handles filesystem operations for storing and archiving LinkedIn export files.
Manages three directories:
- incoming/: Where users manually drop LinkedIn export ZIPs
- raw_zip/: Archive of successfully processed raw export ZIPs
- out_zip/: Generated normalized data packages ready for distribution
"""

import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional
import logging

from app.config import settings

logger = logging.getLogger(__name__)


class StorageService:
    """Manages file storage operations for LinkedIn data exports."""
    
    def __init__(self):
        """Initialize storage paths from config."""
        self.incoming_dir = Path(settings.INCOMING_DIR)
        self.raw_zip_dir = Path(settings.RAW_ZIP_DIR)
        self.out_zip_dir = Path(settings.OUT_ZIP_DIR)
        
        # Ensure all directories exist
        self._ensure_directories()
    
    def _ensure_directories(self) -> None:
        """Create storage directories if they don't exist."""
        for directory in [self.incoming_dir, self.raw_zip_dir, self.out_zip_dir]:
            directory.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured directory exists: {directory}")
    
    def get_latest_incoming_zip(self) -> Optional[Path]:
        """
        Find the most recently modified ZIP file in incoming directory.
        
        Returns:
            Path to latest ZIP file, or None if no ZIPs found
        """
        zip_files = list(self.incoming_dir.glob("*.zip"))
        
        if not zip_files:
            logger.info("No ZIP files found in incoming directory")
            return None
        
        # Sort by modification time, most recent first
        latest_zip = max(zip_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"Found latest incoming ZIP: {latest_zip.name}")
        return latest_zip
    
    def archive_raw_zip(self, source_zip_path: Path) -> Path:
        """
        Copy processed raw export ZIP to archive directory with timestamp.
        
        Args:
            source_zip_path: Path to the ZIP file to archive
            
        Returns:
            Path to the archived copy
            
        Raises:
            FileNotFoundError: If source ZIP doesn't exist
            IOError: If copy operation fails
        """
        if not source_zip_path.exists():
            raise FileNotFoundError(f"Source ZIP not found: {source_zip_path}")
        
        # Generate timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archived_name = f"{source_zip_path.stem}_{timestamp}.zip"
        archived_path = self.raw_zip_dir / archived_name
        
        try:
            shutil.copy2(source_zip_path, archived_path)
            logger.info(f"Archived raw ZIP: {source_zip_path.name} → {archived_path.name}")
            return archived_path
        except Exception as e:
            logger.error(f"Failed to archive raw ZIP: {e}")
            raise IOError(f"Could not archive ZIP file: {e}") from e
    
    def save_output_zip(self, source_zip_path: Path, run_id: Optional[str] = None) -> Path:
        """
        Move generated output package to out_zip directory.
        
        Args:
            source_zip_path: Path to generated output ZIP
            run_id: Optional run identifier to include in filename
            
        Returns:
            Path to stored output ZIP
            
        Raises:
            FileNotFoundError: If source ZIP doesn't exist
            IOError: If move operation fails
        """
        if not source_zip_path.exists():
            raise FileNotFoundError(f"Output ZIP not found: {source_zip_path}")
        
        # Generate descriptive filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if run_id:
            output_name = f"linkedin_data_{run_id}_{timestamp}.zip"
        else:
            output_name = f"linkedin_data_{timestamp}.zip"
        
        output_path = self.out_zip_dir / output_name
        
        try:
            shutil.move(str(source_zip_path), str(output_path))
            logger.info(f"Saved output ZIP: {output_path.name}")
            return output_path
        except Exception as e:
            logger.error(f"Failed to save output ZIP: {e}")
            raise IOError(f"Could not save output ZIP: {e}") from e
    
    def cleanup_incoming(self, zip_path: Path, delete: bool = False) -> None:
        """
        Clean up processed ZIP from incoming directory.
        
        Args:
            zip_path: Path to ZIP file in incoming directory
            delete: If True, delete the file; if False, just log (default: False)
        """
        if not zip_path.exists():
            logger.warning(f"ZIP file already removed: {zip_path}")
            return
        
        if delete:
            try:
                zip_path.unlink()
                logger.info(f"Deleted processed ZIP from incoming: {zip_path.name}")
            except Exception as e:
                logger.error(f"Failed to delete incoming ZIP: {e}")
        else:
            logger.info(f"Keeping processed ZIP in incoming (delete=False): {zip_path.name}")
    
    def get_output_zip_by_timestamp(self, timestamp: str) -> Optional[Path]:
        """
        Find an output ZIP by timestamp string.
        
        Args:
            timestamp: Timestamp string to search for (format: YYYYMMDD_HHMMSS)
            
        Returns:
            Path to matching ZIP file, or None if not found
        """
        pattern = f"*_{timestamp}.zip"
        matches = list(self.out_zip_dir.glob(pattern))
        
        if not matches:
            return None
        
        return matches[0]  # Return first match
    
    def list_archived_zips(self, limit: int = 10) -> list[dict]:
        """
        List recently archived raw ZIPs.
        
        Args:
            limit: Maximum number of files to return
            
        Returns:
            List of dicts with filename, size, and modification time
        """
        zip_files = sorted(
            self.raw_zip_dir.glob("*.zip"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )[:limit]
        
        return [
            {
                "filename": f.name,
                "size_mb": f.stat().st_size / (1024 * 1024),
                "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat()
            }
            for f in zip_files
        ]
    
    def list_output_zips(self, limit: int = 10) -> list[dict]:
        """
        List recently generated output ZIPs.
        
        Args:
            limit: Maximum number of files to return
            
        Returns:
            List of dicts with filename, size, and modification time
        """
        zip_files = sorted(
            self.out_zip_dir.glob("*.zip"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )[:limit]
        
        return [
            {
                "filename": f.name,
                "size_mb": f.stat().st_size / (1024 * 1024),
                "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat()
            }
            for f in zip_files
        ]
    
    def get_storage_stats(self) -> dict:
        """
        Get storage statistics for all directories.
        
        Returns:
            Dict with counts and total sizes for each directory
        """
        def dir_stats(directory: Path) -> dict:
            files = list(directory.glob("*.zip"))
            total_size = sum(f.stat().st_size for f in files)
            return {
                "count": len(files),
                "total_size_mb": total_size / (1024 * 1024)
            }
        
        return {
            "incoming": dir_stats(self.incoming_dir),
            "raw_archive": dir_stats(self.raw_zip_dir),
            "output": dir_stats(self.out_zip_dir)
        }


# Singleton instance
storage_service = StorageService()