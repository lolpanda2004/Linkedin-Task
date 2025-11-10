"""
app/services/reconcile.py

Reconciles source data with database records to ensure data integrity.
Compares raw input counts with inserted database counts, generates checksums,
and produces reconciliation reports for auditing.
"""

import hashlib
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class ReconciliationService:
    """Validates data integrity between source and database."""
    
    def __init__(self):
        """Initialize reconciliation service."""
        self.reconciliation_history = []
    
    def reconcile(
        self,
        source_data: Dict[str, List[Dict[str, Any]]],
        db_counts: Dict[str, int],
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Reconcile source data with database insertion results.
        
        Args:
            source_data: Dict mapping table names to list of source records
            db_counts: Dict mapping table names to count of DB records inserted
            run_id: Optional run identifier
            
        Returns:
            Reconciliation report dictionary
            
        Example:
            source_data = {
                "participants": [{...}, {...}],  # 2 records
                "messages": [{...}, {...}, {...}]  # 3 records
            }
            db_counts = {
                "participants": 2,
                "messages": 3
            }
        """
        logger.info("Starting reconciliation...")
        
        report = {
            "run_id": run_id,
            "timestamp": datetime.now().isoformat(),
            "tables": {},
            "summary": {
                "total_tables": 0,
                "tables_matched": 0,
                "tables_mismatched": 0,
                "total_source_records": 0,
                "total_db_records": 0,
            },
            "status": "UNKNOWN"
        }
        
        # Reconcile each table
        for table_name in set(list(source_data.keys()) + list(db_counts.keys())):
            table_report = self._reconcile_table(
                table_name=table_name,
                source_records=source_data.get(table_name, []),
                db_count=db_counts.get(table_name, 0)
            )
            report["tables"][table_name] = table_report
            
            # Update summary
            report["summary"]["total_tables"] += 1
            if table_report["matched"]:
                report["summary"]["tables_matched"] += 1
            else:
                report["summary"]["tables_mismatched"] += 1
        
        # Calculate totals
        report["summary"]["total_source_records"] = sum(
            len(records) for records in source_data.values()
        )
        report["summary"]["total_db_records"] = sum(db_counts.values())
        
        # Determine overall status
        if report["summary"]["tables_mismatched"] == 0:
            report["status"] = "SUCCESS"
        elif report["summary"]["tables_matched"] > 0:
            report["status"] = "PARTIAL"
        else:
            report["status"] = "FAILURE"
        
        # Log summary
        self._log_report_summary(report)
        
        # Store in history
        self.reconciliation_history.append(report)
        
        return report
    
    def _reconcile_table(
        self,
        table_name: str,
        source_records: List[Dict[str, Any]],
        db_count: int
    ) -> Dict[str, Any]:
        """
        Reconcile a single table.
        
        Args:
            table_name: Name of the table
            source_records: List of source records
            db_count: Number of records in database
            
        Returns:
            Table reconciliation report
        """
        source_count = len(source_records)
        matched = source_count == db_count
        
        table_report = {
            "source_count": source_count,
            "db_count": db_count,
            "matched": matched,
            "difference": db_count - source_count,
            "source_checksum": None,
            "issues": []
        }
        
        # Calculate checksum if we have source data
        if source_records:
            table_report["source_checksum"] = self._calculate_checksum(source_records)
        
        # Document issues
        if not matched:
            if source_count > db_count:
                table_report["issues"].append(
                    f"Missing {source_count - db_count} records in database"
                )
            elif db_count > source_count:
                table_report["issues"].append(
                    f"Database has {db_count - source_count} extra records"
                )
        
        if source_count == 0 and db_count == 0:
            table_report["issues"].append("No records in source or database")
        
        return table_report
    
    def _calculate_checksum(self, records: List[Dict[str, Any]]) -> str:
        """
        Calculate MD5 checksum of records for integrity verification.
        
        Args:
            records: List of record dictionaries
            
        Returns:
            MD5 checksum as hex string
        """
        # Sort records by all keys to ensure consistent ordering
        # Convert to JSON string (sorted keys) for hashing
        try:
            sorted_records = sorted(
                records,
                key=lambda x: json.dumps(x, sort_keys=True, default=str)
            )
            json_str = json.dumps(sorted_records, sort_keys=True, default=str)
            return hashlib.md5(json_str.encode()).hexdigest()
        except Exception as e:
            logger.warning(f"Failed to calculate checksum: {e}")
            return "ERROR"
    
    def _log_report_summary(self, report: Dict[str, Any]) -> None:
        """
        Log reconciliation report summary.
        
        Args:
            report: Reconciliation report dictionary
        """
        summary = report["summary"]
        status = report["status"]
        
        log_msg = (
            f"Reconciliation {status}: "
            f"{summary['tables_matched']}/{summary['total_tables']} tables matched, "
            f"Source: {summary['total_source_records']} records, "
            f"DB: {summary['total_db_records']} records"
        )
        
        if status == "SUCCESS":
            logger.info(log_msg)
        elif status == "PARTIAL":
            logger.warning(log_msg)
        else:
            logger.error(log_msg)
        
        # Log individual table mismatches
        for table_name, table_report in report["tables"].items():
            if not table_report["matched"]:
                logger.warning(
                    f"  {table_name}: Source={table_report['source_count']}, "
                    f"DB={table_report['db_count']} (diff={table_report['difference']})"
                )
    
    def generate_detailed_report(
        self,
        report: Dict[str, Any],
        include_checksums: bool = True
    ) -> str:
        """
        Generate human-readable detailed reconciliation report.
        
        Args:
            report: Reconciliation report dictionary
            include_checksums: Include checksum information
            
        Returns:
            Formatted report string
        """
        lines = []
        lines.append("=" * 80)
        lines.append("RECONCILIATION REPORT")
        lines.append("=" * 80)
        lines.append(f"Timestamp: {report['timestamp']}")
        lines.append(f"Run ID: {report.get('run_id', 'N/A')}")
        lines.append(f"Overall Status: {report['status']}")
        lines.append("")
        
        # Summary section
        lines.append("SUMMARY")
        lines.append("-" * 80)
        summary = report["summary"]
        lines.append(f"Total Tables: {summary['total_tables']}")
        lines.append(f"  ✓ Matched: {summary['tables_matched']}")
        lines.append(f"  ✗ Mismatched: {summary['tables_mismatched']}")
        lines.append(f"Total Source Records: {summary['total_source_records']:,}")
        lines.append(f"Total DB Records: {summary['total_db_records']:,}")
        lines.append("")
        
        # Table details
        lines.append("TABLE DETAILS")
        lines.append("-" * 80)
        
        for table_name, table_report in sorted(report["tables"].items()):
            status_icon = "✓" if table_report["matched"] else "✗"
            lines.append(f"{status_icon} {table_name}")
            lines.append(f"  Source Count: {table_report['source_count']:,}")
            lines.append(f"  DB Count: {table_report['db_count']:,}")
            lines.append(f"  Difference: {table_report['difference']:+,}")
            
            if include_checksums and table_report.get("source_checksum"):
                lines.append(f"  Source Checksum: {table_report['source_checksum']}")
            
            if table_report["issues"]:
                lines.append("  Issues:")
                for issue in table_report["issues"]:
                    lines.append(f"    - {issue}")
            
            lines.append("")
        
        lines.append("=" * 80)
        
        return "\n".join(lines)
    
    def get_last_report(self) -> Optional[Dict[str, Any]]:
        """
        Get the most recent reconciliation report.
        
        Returns:
            Last report dictionary, or None if no reports exist
        """
        if not self.reconciliation_history:
            return None
        return self.reconciliation_history[-1]
    
    def get_report_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent reconciliation reports.
        
        Args:
            limit: Maximum number of reports to return
            
        Returns:
            List of report dictionaries (most recent first)
        """
        return list(reversed(self.reconciliation_history[-limit:]))
    
    def validate_data_quality(
        self,
        source_data: Dict[str, List[Dict[str, Any]]],
        required_fields: Optional[Dict[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """
        Validate data quality of source records.
        
        Args:
            source_data: Dict mapping table names to records
            required_fields: Optional dict mapping table names to required field lists
            
        Returns:
            Data quality report
        """
        quality_report = {
            "timestamp": datetime.now().isoformat(),
            "tables": {},
            "overall_quality": "GOOD"
        }
        
        for table_name, records in source_data.items():
            table_quality = {
                "total_records": len(records),
                "issues": [],
                "null_counts": {},
                "duplicate_check": None
            }
            
            if not records:
                table_quality["issues"].append("No records found")
                quality_report["tables"][table_name] = table_quality
                continue
            
            # Check required fields
            if required_fields and table_name in required_fields:
                for record in records:
                    for field in required_fields[table_name]:
                        if field not in record or record[field] is None:
                            table_quality["null_counts"][field] = \
                                table_quality["null_counts"].get(field, 0) + 1
            
            # Check for completely empty records
            empty_count = sum(1 for r in records if not any(r.values()))
            if empty_count > 0:
                table_quality["issues"].append(f"{empty_count} empty records")
            
            # Detect potential duplicates (by converting to JSON and comparing)
            record_hashes = [
                hashlib.md5(json.dumps(r, sort_keys=True, default=str).encode()).hexdigest()
                for r in records
            ]
            unique_hashes = len(set(record_hashes))
            if unique_hashes < len(records):
                duplicates = len(records) - unique_hashes
                table_quality["duplicate_check"] = f"{duplicates} potential duplicates"
                table_quality["issues"].append(table_quality["duplicate_check"])
            
            quality_report["tables"][table_name] = table_quality
            
            # Update overall quality
            if table_quality["issues"]:
                quality_report["overall_quality"] = "ISSUES_FOUND"
        
        return quality_report
    
    def compare_runs(
        self,
        report1: Dict[str, Any],
        report2: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Compare two reconciliation reports to detect changes.
        
        Args:
            report1: First (earlier) report
            report2: Second (later) report
            
        Returns:
            Comparison report showing differences
        """
        comparison = {
            "report1_timestamp": report1["timestamp"],
            "report2_timestamp": report2["timestamp"],
            "changes": {}
        }
        
        # Compare each table
        all_tables = set(
            list(report1["tables"].keys()) + list(report2["tables"].keys())
        )
        
        for table_name in all_tables:
            table1 = report1["tables"].get(table_name, {})
            table2 = report2["tables"].get(table_name, {})
            
            count1 = table1.get("db_count", 0)
            count2 = table2.get("db_count", 0)
            
            if count1 != count2:
                comparison["changes"][table_name] = {
                    "previous_count": count1,
                    "current_count": count2,
                    "change": count2 - count1
                }
        
        return comparison


# Singleton instance
reconciliation_service = ReconciliationService()