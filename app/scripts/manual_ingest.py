"""
manual_ingest.py

Manually trigger data ingestion without scheduler or email.
Perfect for testing and development.

Usage:
    python manual_ingest.py /path/to/linkedin_export.zip
"""

import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.scheduler.jobs import ingestion_job


def main():
    """Run manual ingestion."""
    if len(sys.argv) < 2:
        print("Usage: python manual_ingest.py /path/to/linkedin_export.zip")
        print("\nExample:")
        print("  python manual_ingest.py ./data/incoming/Basic_LinkedInDataExport.zip")
        sys.exit(1)
    
    zip_path = Path(sys.argv[1])
    
    if not zip_path.exists():
        print(f"✗ Error: File not found: {zip_path}")
        sys.exit(1)
    
    print("\n" + "="*80)
    print("MANUAL LINKEDIN DATA INGESTION")
    print("="*80)
    print(f"Source: {zip_path}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")
    
    # Run ingestion (email disabled automatically if not configured)
    try:
        result = ingestion_job.run_ingestion(zip_path=str(zip_path))
        
        # Print results
        print("\n" + "="*80)
        print("INGESTION RESULT")
        print("="*80)
        print(f"Status: {result['status']}")
        print(f"Run ID: {result['run_id']}")
        print(f"Start Time: {result['start_time']}")
        print(f"End Time: {result['end_time']}")
        
        if result.get('error'):
            print(f"\n✗ Error: {result['error']}")
        
        if result.get('stats'):
            print(f"\nStatistics:")
            stats = result['stats']
            
            if 'raw_counts' in stats:
                print(f"\n  Raw Data Extracted:")
                for key, value in stats['raw_counts'].items():
                    print(f"    {key}: {value}")
            
            if 'normalized_counts' in stats:
                print(f"\n  Normalized Data:")
                for key, value in stats['normalized_counts'].items():
                    print(f"    {key}: {value}")
            
            if 'db_inserted' in stats:
                print(f"\n  Database Insertions:")
                for key, value in stats['db_inserted'].items():
                    print(f"    {key}: {value}")
            
            if 'reconciliation' in stats:
                print(f"\n  Reconciliation:")
                recon = stats['reconciliation']
                print(f"    Status: {recon.get('status', 'N/A')}")
                if 'expected' in recon:
                    print(f"    Expected: {recon['expected']}")
                if 'inserted' in recon:
                    print(f"    Inserted: {recon['inserted']}")
        
        print("="*80 + "\n")
        
        if result['status'] == 'SUCCESS':
            print("✓ Ingestion completed successfully!")
            if result['stats'].get('output_file'):
                print(f"✓ Output file: {result['stats']['output_file']}")
            return 0
        elif result['status'] == 'SKIPPED':
            print("⚠ Ingestion skipped (duplicate)")
            return 0
        else:
            print("✗ Ingestion failed")
            return 1
            
    except KeyboardInterrupt:
        print("\n\n⚠ Ingestion interrupted by user")
        return 1
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())