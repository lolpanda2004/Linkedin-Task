"""
app/services/zip_package.py

Creates ZIP packages containing normalized LinkedIn data.
Generates CSV and JSON files for each table, plus metadata manifest.
Output structure:
  linkedin_data_YYYYMMDD_HHMMSS.zip
  ├─ participants.csv
  ├─ participants.json
  ├─ conversations.csv
  ├─ conversations.json
  ├─ messages.csv
  ├─ messages.json
  ├─ ...
  └─ manifest.json
"""

import json
import csv
import zipfile
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import tempfile
import logging

from app.config import settings

logger = logging.getLogger(__name__)


class ZipPackageService:
    """Creates ZIP packages with CSV/JSON exports of normalized data."""
    
    def __init__(self):
        """Initialize package service."""
        self.temp_dir = Path(tempfile.gettempdir()) / "linkedin_ingestor"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
    
    def create_package(
        self,
        data: Dict[str, List[Dict[str, Any]]],
        run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Path:
        """
        Create ZIP package containing CSV and JSON files for all tables.
        
        Args:
            data: Dict mapping table names to list of records (as dicts)
            run_id: Optional run identifier for filename
            metadata: Optional metadata to include in manifest
            
        Returns:
            Path to created ZIP file
            
        Example data structure:
            {
                "participants": [
                    {"id": 1, "name": "John Doe", "email": "john@example.com"},
                    {"id": 2, "name": "Jane Smith", "email": "jane@example.com"}
                ],
                "conversations": [
                    {"id": 1, "participant_ids": "1,2", "created_at": "2024-01-01"}
                ]
            }
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if run_id:
            zip_filename = f"linkedin_data_{run_id}_{timestamp}.zip"
        else:
            zip_filename = f"linkedin_data_{timestamp}.zip"
        
        zip_path = self.temp_dir / zip_filename
        
        logger.info(f"Creating data package: {zip_filename}")
        
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Write CSV and JSON for each table
                for table_name, records in data.items():
                    if not records:
                        logger.warning(f"No records for table: {table_name}")
                        continue
                    
                    # Generate CSV
                    csv_content = self._generate_csv(records)
                    zipf.writestr(f"{table_name}.csv", csv_content)
                    
                    # Generate JSON
                    json_content = self._generate_json(records)
                    zipf.writestr(f"{table_name}.json", json_content)
                    
                    logger.debug(f"Added {len(records)} records for {table_name}")
                
                # Generate and add manifest
                manifest = self._generate_manifest(data, run_id, timestamp, metadata)
                zipf.writestr("manifest.json", json.dumps(manifest, indent=2))
            
            file_size_mb = zip_path.stat().st_size / (1024 * 1024)
            logger.info(f"Package created successfully: {zip_filename} ({file_size_mb:.2f} MB)")
            
            return zip_path
            
        except Exception as e:
            logger.error(f"Failed to create package: {e}", exc_info=True)
            # Clean up partial file if it exists
            if zip_path.exists():
                zip_path.unlink()
            raise
    
    def _generate_csv(self, records: List[Dict[str, Any]]) -> str:
        """
        Generate CSV content from list of records.
        
        Args:
            records: List of dictionaries representing table rows
            
        Returns:
            CSV content as string
        """
        if not records:
            return ""
        
        # Get all unique field names across all records
        fieldnames = set()
        for record in records:
            fieldnames.update(record.keys())
        fieldnames = sorted(fieldnames)
        
        # Build CSV in memory
        import io
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        
        for record in records:
            # Convert None to empty string, handle other types
            row = {}
            for key, value in record.items():
                if value is None:
                    row[key] = ""
                elif isinstance(value, (list, dict)):
                    row[key] = json.dumps(value)
                else:
                    row[key] = str(value)
            writer.writerow(row)
        
        return output.getvalue()
    
    def _generate_json(self, records: List[Dict[str, Any]]) -> str:
        """
        Generate JSON content from list of records.
        
        Args:
            records: List of dictionaries representing table rows
            
        Returns:
            JSON content as string
        """
        # Convert datetime objects to ISO format strings
        def default_serializer(obj):
            if hasattr(obj, 'isoformat'):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        
        return json.dumps(records, indent=2, default=default_serializer)
    
    def _generate_manifest(
        self,
        data: Dict[str, List[Dict[str, Any]]],
        run_id: Optional[str],
        timestamp: str,
        metadata: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Generate manifest file with package metadata.
        
        Args:
            data: Dict mapping table names to records
            run_id: Optional run identifier
            timestamp: Package creation timestamp
            metadata: Optional additional metadata
            
        Returns:
            Manifest dictionary
        """
        manifest = {
            "package_info": {
                "generated_at": datetime.now().isoformat(),
                "run_id": run_id,
                "timestamp": timestamp,
                "version": "1.0",
                "source": "LinkedIn Data Export"
            },
            "tables": {}
        }
        
        # Add table statistics
        for table_name, records in data.items():
            manifest["tables"][table_name] = {
                "record_count": len(records),
                "files": [
                    f"{table_name}.csv",
                    f"{table_name}.json"
                ]
            }
            
            # Add field names if records exist
            if records:
                manifest["tables"][table_name]["fields"] = sorted(records[0].keys())
        
        # Add custom metadata if provided
        if metadata:
            manifest["metadata"] = metadata
        
        # Add totals
        manifest["summary"] = {
            "total_tables": len(data),
            "total_records": sum(len(records) for records in data.values()),
            "total_files": len(data) * 2 + 1  # CSV + JSON per table + manifest
        }
        
        return manifest
    
    def extract_package(self, zip_path: Path, extract_to: Optional[Path] = None) -> Path:
        """
        Extract ZIP package to directory.
        
        Args:
            zip_path: Path to ZIP file
            extract_to: Optional destination directory (default: temp dir)
            
        Returns:
            Path to extraction directory
            
        Raises:
            FileNotFoundError: If ZIP file doesn't exist
            zipfile.BadZipFile: If file is not a valid ZIP
        """
        if not zip_path.exists():
            raise FileNotFoundError(f"ZIP file not found: {zip_path}")
        
        if extract_to is None:
            extract_to = self.temp_dir / f"extracted_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        extract_to.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Extracting package to: {extract_to}")
        
        with zipfile.ZipFile(zip_path, 'r') as zipf:
            zipf.extractall(extract_to)
        
        logger.info(f"Package extracted successfully")
        return extract_to
    
    def read_manifest(self, zip_path: Path) -> Dict[str, Any]:
        """
        Read manifest from ZIP package without full extraction.
        
        Args:
            zip_path: Path to ZIP file
            
        Returns:
            Manifest dictionary
            
        Raises:
            FileNotFoundError: If ZIP file doesn't exist
            KeyError: If manifest.json not found in ZIP
        """
        if not zip_path.exists():
            raise FileNotFoundError(f"ZIP file not found: {zip_path}")
        
        with zipfile.ZipFile(zip_path, 'r') as zipf:
            manifest_content = zipf.read("manifest.json")
            return json.loads(manifest_content)
    
    def validate_package(self, zip_path: Path) -> Dict[str, Any]:
        """
        Validate ZIP package structure and contents.
        
        Args:
            zip_path: Path to ZIP file
            
        Returns:
            Validation result dict with 'valid' boolean and 'issues' list
        """
        issues = []
        
        if not zip_path.exists():
            return {"valid": False, "issues": ["ZIP file does not exist"]}
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zipf:
                file_list = zipf.namelist()
                
                # Check for manifest
                if "manifest.json" not in file_list:
                    issues.append("Missing manifest.json")
                else:
                    # Validate manifest structure
                    try:
                        manifest = json.loads(zipf.read("manifest.json"))
                        
                        # Check required manifest fields
                        if "package_info" not in manifest:
                            issues.append("Manifest missing package_info")
                        if "tables" not in manifest:
                            issues.append("Manifest missing tables section")
                        
                        # Validate each table has CSV and JSON
                        if "tables" in manifest:
                            for table_name in manifest["tables"].keys():
                                csv_file = f"{table_name}.csv"
                                json_file = f"{table_name}.json"
                                
                                if csv_file not in file_list:
                                    issues.append(f"Missing {csv_file}")
                                if json_file not in file_list:
                                    issues.append(f"Missing {json_file}")
                    
                    except json.JSONDecodeError:
                        issues.append("Invalid manifest.json (not valid JSON)")
                
                # Check for testzip (file integrity)
                bad_file = zipf.testzip()
                if bad_file:
                    issues.append(f"Corrupted file in ZIP: {bad_file}")
        
        except zipfile.BadZipFile:
            issues.append("File is not a valid ZIP archive")
        except Exception as e:
            issues.append(f"Validation error: {str(e)}")
        
        return {
            "valid": len(issues) == 0,
            "issues": issues
        }
    
    def cleanup_temp_files(self, older_than_hours: int = 24) -> int:
        """
        Clean up old temporary files.
        
        Args:
            older_than_hours: Remove files older than this many hours
            
        Returns:
            Number of files deleted
        """
        if not self.temp_dir.exists():
            return 0
        
        cutoff_time = datetime.now().timestamp() - (older_than_hours * 3600)
        deleted_count = 0
        
        for file_path in self.temp_dir.glob("*"):
            try:
                if file_path.stat().st_mtime < cutoff_time:
                    if file_path.is_file():
                        file_path.unlink()
                        deleted_count += 1
                    elif file_path.is_dir():
                        import shutil
                        shutil.rmtree(file_path)
                        deleted_count += 1
            except Exception as e:
                logger.warning(f"Failed to delete {file_path}: {e}")
        
        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} temporary files/directories")
        
        return deleted_count


# Singleton instance
zip_package_service = ZipPackageService()