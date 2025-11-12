"""
complete_test_suite.py

Complete test suite to validate LinkedIn data ingestion.
Tests extraction, normalization, database storage, and data integrity.

Usage:
    python complete_test_suite.py /path/to/linkedin_export.zip
"""

import sys
import zipfile
import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
import json

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.repo import DatabaseRepository
from app.connectors.data_export import DataExportConnector
from app.services.normalize import normalize_service
from app.config import settings


class ComprehensiveValidator:
    """Complete validation suite for LinkedIn data ingestion."""
    
    def __init__(self, db_url: str = None):
        """Initialize validator."""
        self.db_url = db_url or settings.DATABASE_URL
        self.repo = DatabaseRepository(self.db_url)
        self.connector = DataExportConnector()
        self.results = {
            "tests": [],
            "passed": 0,
            "failed": 0,
            "warnings": 0
        }
    
    def run_all_tests(self, zip_path: Path) -> Dict[str, Any]:
        """Run complete test suite."""
        print("\n" + "="*80)
        print("LINKEDIN DATA INGESTION VALIDATION SUITE")
        print("="*80)
        print(f"ZIP File: {zip_path.name}")
        print(f"Database: {self.db_url}")
        print(f"Timestamp: {datetime.now().isoformat()}")
        print("="*80 + "\n")
        
        # Test 1: ZIP file validation
        self._test_zip_structure(zip_path)
        
        # Test 2: Data extraction
        raw_data = self._test_extraction(zip_path)
        if not raw_data:
            print("\n✗ Cannot continue - extraction failed")
            return self.results
        
        # Test 3: Data normalization
        normalized_data = self._test_normalization(raw_data)
        if not normalized_data:
            print("\n✗ Cannot continue - normalization failed")
            return self.results
        
        # Test 4: Database connectivity
        session = self._test_database_connection()
        if not session:
            print("\n✗ Cannot continue - database connection failed")
            return self.results
        
        try:
            # Test 5: Participant validation
            self._test_participants(normalized_data, session)
            
            # Test 6: Conversation validation
            self._test_conversations(normalized_data, session)
            
            # Test 7: Message validation
            self._test_messages(normalized_data, session)
            
            # Test 8: Referential integrity
            self._test_referential_integrity(normalized_data, session)
            
            # Test 9: Data quality checks
            self._test_data_quality(normalized_data, session)
            
            # Test 10: Count reconciliation
            self._test_count_reconciliation(normalized_data, session)
            
        finally:
            session.close()
        
        # Print summary
        self._print_summary()
        
        return self.results
    
    def _test_zip_structure(self, zip_path: Path):
        """Test 1: Validate ZIP file structure."""
        test_name = "ZIP File Structure"
        print(f"\n{'─'*80}")
        print(f"Test 1: {test_name}")
        print(f"{'─'*80}")
        
        try:
            if not zip_path.exists():
                self._fail(test_name, f"File does not exist: {zip_path}")
                return
            
            with zipfile.ZipFile(zip_path, 'r') as zf:
                files = zf.namelist()
                print(f"  Files in ZIP: {len(files)}")
                
                # Check for expected files
                expected_files = ['messages.csv', 'Connections.csv']
                found = []
                missing = []
                
                for exp_file in expected_files:
                    if any(exp_file.lower() in f.lower() for f in files):
                        found.append(exp_file)
                        print(f"  ✓ Found: {exp_file}")
                    else:
                        missing.append(exp_file)
                        print(f"  ⚠ Missing: {exp_file}")
                
                if missing:
                    self._warn(test_name, f"Some expected files missing: {missing}")
                else:
                    self._pass(test_name, "ZIP structure valid")
                    
        except zipfile.BadZipFile:
            self._fail(test_name, "Invalid ZIP file")
        except Exception as e:
            self._fail(test_name, str(e))
    
    def _test_extraction(self, zip_path: Path) -> Dict[str, List]:
        """Test 2: Extract raw data from ZIP."""
        test_name = "Data Extraction"
        print(f"\n{'─'*80}")
        print(f"Test 2: {test_name}")
        print(f"{'─'*80}")
        
        try:
            raw_data = self.connector.extract(zip_path)
            
            if not raw_data:
                self._fail(test_name, "No data extracted")
                return None
            
            print(f"  Tables extracted: {list(raw_data.keys())}")
            for table, records in raw_data.items():
                print(f"  {table}: {len(records)} records")
            
            total_records = sum(len(r) for r in raw_data.values())
            self._pass(test_name, f"Extracted {total_records} total records from {len(raw_data)} tables")
            
            return raw_data
            
        except Exception as e:
            self._fail(test_name, str(e))
            import traceback
            traceback.print_exc()
            return None
    
    def _test_normalization(self, raw_data: Dict) -> Dict:
        """Test 3: Normalize raw data."""
        test_name = "Data Normalization"
        print(f"\n{'─'*80}")
        print(f"Test 3: {test_name}")
        print(f"{'─'*80}")
        
        try:
            normalized_data = normalize_service.normalize_all(raw_data)
            
            print(f"  Participants: {len(normalized_data.get('participants', []))}")
            print(f"  Conversations: {len(normalized_data.get('conversations', []))}")
            print(f"  Messages: {len(normalized_data.get('messages', []))}")
            print(f"  Conversation-Participant Links: {len(normalized_data.get('conversation_participants', []))}")
            
            # Check stats
            stats = normalized_data.get('stats', {})
            if stats:
                print(f"\n  Normalization Stats:")
                print(f"    Processed: {stats.get('participants_processed', 0)} participants")
                print(f"    Processed: {stats.get('conversations_processed', 0)} conversations")
                print(f"    Processed: {stats.get('messages_processed', 0)} messages")
                print(f"    Skipped: {stats.get('participants_skipped', 0)} participants")
                print(f"    Skipped: {stats.get('conversations_skipped', 0)} conversations")
                print(f"    Skipped: {stats.get('messages_skipped', 0)} messages")
                print(f"    Validation Errors: {stats.get('validation_errors', 0)}")
            
            if stats.get('validation_errors', 0) > 0:
                self._warn(test_name, f"{stats['validation_errors']} validation errors during normalization")
            else:
                self._pass(test_name, "Data normalized successfully")
            
            return normalized_data
            
        except Exception as e:
            self._fail(test_name, str(e))
            import traceback
            traceback.print_exc()
            return None
    
    def _test_database_connection(self):
        """Test 4: Database connectivity."""
        test_name = "Database Connection"
        print(f"\n{'─'*80}")
        print(f"Test 4: {test_name}")
        print(f"{'─'*80}")
        
        try:
            session = self.repo.get_session()
            
            # Try a simple query
            count = self.repo.get_message_count(session)
            print(f"  Database accessible")
            print(f"  Current message count: {count}")
            
            self._pass(test_name, "Database connection successful")
            return session
            
        except Exception as e:
            self._fail(test_name, str(e))
            return None
    
    def _test_participants(self, normalized_data: Dict, session):
        """Test 5: Validate participants in database."""
        test_name = "Participant Validation"
        print(f"\n{'─'*80}")
        print(f"Test 5: {test_name}")
        print(f"{'─'*80}")
        
        participants = normalized_data.get('participants', [])
        if not participants:
            self._warn(test_name, "No participants to validate")
            return
        
        print(f"  Checking {len(participants)} participants...")
        
        found = 0
        missing = []
        sample_checks = []
        
        # Check all participants
        for p in participants:
            linkedin_id = p['linkedin_id']
            db_p = self.repo.get_participant_by_linkedin_id(session, linkedin_id)
            
            if db_p:
                found += 1
                # Sample detailed check for first 3
                if len(sample_checks) < 3:
                    sample_checks.append({
                        'linkedin_id': linkedin_id,
                        'name_match': db_p.full_name == p['full_name'],
                        'email_match': db_p.email == p.get('email')
                    })
            else:
                missing.append(linkedin_id)
        
        print(f"  Found in DB: {found}/{len(participants)}")
        
        if sample_checks:
            print(f"\n  Sample validation (first 3):")
            for i, check in enumerate(sample_checks, 1):
                print(f"    {i}. {check['linkedin_id']}")
                print(f"       Name match: {'✓' if check['name_match'] else '✗'}")
                print(f"       Email match: {'✓' if check['email_match'] else '✗'}")
        
        if found == len(participants):
            self._pass(test_name, f"All {found} participants found in database")
        elif found > 0:
            self._warn(test_name, f"Only {found}/{len(participants)} participants found. Missing: {len(missing)}")
            if missing[:5]:
                print(f"  First 5 missing: {missing[:5]}")
        else:
            self._fail(test_name, "No participants found in database")
    
    def _test_conversations(self, normalized_data: Dict, session):
        """Test 6: Validate conversations in database."""
        test_name = "Conversation Validation"
        print(f"\n{'─'*80}")
        print(f"Test 6: {test_name}")
        print(f"{'─'*80}")
        
        conversations = normalized_data.get('conversations', [])
        if not conversations:
            self._warn(test_name, "No conversations to validate")
            return
        
        print(f"  Checking {len(conversations)} conversations...")
        
        found = 0
        missing = []
        
        for conv in conversations:
            conv_id = conv['conversation_id']
            db_conv = self.repo.get_conversation_by_conversation_id(session, conv_id)
            
            if db_conv:
                found += 1
            else:
                missing.append(conv_id)
        
        print(f"  Found in DB: {found}/{len(conversations)}")
        
        if found == len(conversations):
            self._pass(test_name, f"All {found} conversations found in database")
        elif found > 0:
            self._warn(test_name, f"Only {found}/{len(conversations)} conversations found. Missing: {len(missing)}")
        else:
            self._fail(test_name, "No conversations found in database")
    
    def _test_messages(self, normalized_data: Dict, session):
        """Test 7: Validate messages in database."""
        test_name = "Message Validation"
        print(f"\n{'─'*80}")
        print(f"Test 7: {test_name}")
        print(f"{'─'*80}")
        
        messages = normalized_data.get('messages', [])
        if not messages:
            self._warn(test_name, "No messages to validate")
            return
        
        print(f"  Checking {len(messages)} messages...")
        
        found = 0
        missing = []
        content_matches = 0
        
        # Sample check first 100 messages (for performance)
        sample_size = min(100, len(messages))
        for msg in messages[:sample_size]:
            msg_id = msg['message_id']
            db_msg = self.repo.get_message_by_message_id(session, msg_id)
            
            if db_msg:
                found += 1
                # Check content match
                if db_msg.content == msg.get('content'):
                    content_matches += 1
            else:
                missing.append(msg_id)
        
        print(f"  Sample checked: {sample_size} messages")
        print(f"  Found in DB: {found}/{sample_size}")
        print(f"  Content matches: {content_matches}/{found}")
        
        if found == sample_size:
            self._pass(test_name, f"All sampled messages found with {content_matches}/{found} content matches")
        elif found > sample_size * 0.9:
            self._warn(test_name, f"Most messages found ({found}/{sample_size})")
        else:
            self._fail(test_name, f"Only {found}/{sample_size} messages found")
    
    def _test_referential_integrity(self, normalized_data: Dict, session):
        """Test 8: Validate referential integrity."""
        test_name = "Referential Integrity"
        print(f"\n{'─'*80}")
        print(f"Test 8: {test_name}")
        print(f"{'─'*80}")
        
        issues = []
        
        # Check 1: Message senders exist
        messages = normalized_data.get('messages', [])
        participants = normalized_data.get('participants', [])
        participant_ids = {p['linkedin_id'] for p in participants}
        
        orphan_senders = [
            m['message_id'] for m in messages
            if m.get('sender_linkedin_id') not in participant_ids
        ]
        
        print(f"  Messages with invalid senders: {len(orphan_senders)}")
        if orphan_senders:
            issues.append(f"{len(orphan_senders)} messages have invalid senders")
        
        # Check 2: Message conversations exist
        conversations = normalized_data.get('conversations', [])
        conversation_ids = {c['conversation_id'] for c in conversations}
        
        orphan_convs = [
            m['message_id'] for m in messages
            if m.get('conversation_id') not in conversation_ids
        ]
        
        print(f"  Messages with invalid conversations: {len(orphan_convs)}")
        if orphan_convs:
            issues.append(f"{len(orphan_convs)} messages have invalid conversation_ids")
        
        # Check 3: Conversation participants exist
        conv_participants = normalized_data.get('conversation_participants', [])
        invalid_participants = [
            cp for cp in conv_participants
            if cp.get('participant_linkedin_id') not in participant_ids
        ]
        
        print(f"  Conversation links with invalid participants: {len(invalid_participants)}")
        if invalid_participants:
            issues.append(f"{len(invalid_participants)} conversation-participant links are invalid")
        
        if not issues:
            self._pass(test_name, "All referential integrity checks passed")
        else:
            self._fail(test_name, "; ".join(issues))
    
    def _test_data_quality(self, normalized_data: Dict, session):
        """Test 9: Data quality checks."""
        test_name = "Data Quality"
        print(f"\n{'─'*80}")
        print(f"Test 9: {test_name}")
        print(f"{'─'*80}")
        
        issues = []
        
        # Check participants
        participants = normalized_data.get('participants', [])
        empty_names = sum(1 for p in participants if not p.get('full_name'))
        print(f"  Participants with empty names: {empty_names}")
        if empty_names > 0:
            issues.append(f"{empty_names} participants have empty names")
        
        # Check messages
        messages = normalized_data.get('messages', [])
        empty_content = sum(1 for m in messages if not m.get('content'))
        print(f"  Messages with empty content: {empty_content}")
        if empty_content > len(messages) * 0.5:
            issues.append(f"{empty_content} messages have empty content")
        
        # Check dates
        invalid_dates = sum(1 for m in messages if not m.get('sent_at'))
        print(f"  Messages with invalid dates: {invalid_dates}")
        if invalid_dates > 0:
            issues.append(f"{invalid_dates} messages have invalid dates")
        
        if not issues:
            self._pass(test_name, "Data quality checks passed")
        else:
            self._warn(test_name, "; ".join(issues))
    
    def _test_count_reconciliation(self, normalized_data: Dict, session):
        """Test 10: Reconcile counts between source and database."""
        test_name = "Count Reconciliation"
        print(f"\n{'─'*80}")
        print(f"Test 10: {test_name}")
        print(f"{'─'*80}")
        
        # Get source counts
        source_counts = {
            'participants': len(normalized_data.get('participants', [])),
            'conversations': len(normalized_data.get('conversations', [])),
            'messages': len(normalized_data.get('messages', []))
        }
        
        # Get DB counts
        db_counts = {
            'participants': self.repo.get_participant_count(session),
            'conversations': self.repo.get_conversation_count(session),
            'messages': self.repo.get_message_count(session)
        }
        
        print(f"\n  {'Table':<20} {'Source':<15} {'Database':<15} {'Match':<10}")
        print(f"  {'-'*60}")
        
        all_match = True
        for table in ['participants', 'conversations', 'messages']:
            source = source_counts[table]
            db = db_counts[table]
            match = "✓" if source <= db else "✗"
            if source > db:
                all_match = False
            
            print(f"  {table:<20} {source:<15} {db:<15} {match:<10}")
        
        if all_match:
            self._pass(test_name, "All source records found in database (or more)")
        else:
            self._fail(test_name, "Database has fewer records than source")
    
    def _pass(self, test_name: str, message: str):
        """Record a passed test."""
        self.results['tests'].append({
            'name': test_name,
            'status': 'PASS',
            'message': message
        })
        self.results['passed'] += 1
        print(f"\n✓ PASS: {message}")
    
    def _fail(self, test_name: str, message: str):
        """Record a failed test."""
        self.results['tests'].append({
            'name': test_name,
            'status': 'FAIL',
            'message': message
        })
        self.results['failed'] += 1
        print(f"\n✗ FAIL: {message}")
    
    def _warn(self, test_name: str, message: str):
        """Record a warning."""
        self.results['tests'].append({
            'name': test_name,
            'status': 'WARN',
            'message': message
        })
        self.results['warnings'] += 1
        print(f"\n⚠ WARN: {message}")
    
    def _print_summary(self):
        """Print test summary."""
        print("\n" + "="*80)
        print("TEST SUMMARY")
        print("="*80)
        print(f"Total Tests: {len(self.results['tests'])}")
        print(f"✓ Passed: {self.results['passed']}")
        print(f"✗ Failed: {self.results['failed']}")
        print(f"⚠ Warnings: {self.results['warnings']}")
        
        if self.results['failed'] == 0:
            print("\n✓ ALL TESTS PASSED")
        else:
            print("\n✗ SOME TESTS FAILED")
        
        print("="*80 + "\n")
    
    def save_report(self, output_path: Path = None):
        """Save detailed report to JSON."""
        if output_path is None:
            output_path = Path(f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        with open(output_path, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        print(f"✓ Detailed report saved to: {output_path}\n")


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python complete_test_suite.py /path/to/linkedin_export.zip")
        sys.exit(1)
    
    zip_path = Path(sys.argv[1])
    if not zip_path.exists():
        print(f"✗ Error: File not found: {zip_path}")
        sys.exit(1)
    
    # Run tests
    validator = ComprehensiveValidator()
    results = validator.run_all_tests(zip_path)
    
    # Save report
    validator.save_report()
    
    # Exit with error if tests failed
    if results['failed'] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()