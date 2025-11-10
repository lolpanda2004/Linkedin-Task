"""
app/scheduler/jobs.py

Background scheduled jobs for LinkedIn data ingestion.
Uses APScheduler to run weekly ingestion and health checks.
"""

import logging
from datetime import datetime
from typing import Optional
from pathlib import Path
import traceback
import uuid

from app.config import settings
from app.services.storage import storage_service
from app.services.emailer import emailer_service
from app.connectors.data_export import DataExportConnector
from app.services.normalize import normalize_service
from app.services.zip_package import zip_package_service
from app.services.reconcile import reconciliation_service
from app.db.repo import DatabaseRepository

logger = logging.getLogger(__name__)

# Initialize database repository
repository = DatabaseRepository(db_url=settings.DATABASE_URL)


class IngestionJob:
    """Handles the complete LinkedIn data ingestion workflow."""
    
    def __init__(self):
        """Initialize ingestion job."""
        self.last_run_status = None
        self.current_run_id = None
        self.repo = repository
    
    def run_ingestion(self, zip_path: Optional[str] = None) -> dict:
        """
        Execute complete ingestion workflow using the new repo.py architecture.
        
        Args:
            zip_path: Optional path to specific ZIP file. If None, uses latest from incoming/
            
        Returns:
            Run status dictionary
        """
        run_id = str(uuid.uuid4())
        self.current_run_id = run_id
        
        run_status = {
            "run_id": run_id,
            "start_time": datetime.utcnow().isoformat(),
            "end_time": None,
            "status": "RUNNING",
            "stage": None,
            "error": None,
            "stats": {}
        }
        
        logger.info(f"Starting ingestion job [run_id: {run_id}]")
        
        # Create database session
        session = self.repo.get_session()
        ingestion_run = None
        
        try:
            # Stage 1: Find or validate ZIP file
            run_status["stage"] = "locating_source"
            if zip_path:
                source_zip = Path(zip_path)
                if not source_zip.exists():
                    raise FileNotFoundError(f"Specified ZIP not found: {zip_path}")
            else:
                source_zip = storage_service.get_latest_incoming_zip()
                if not source_zip:
                    raise FileNotFoundError("No ZIP files found in incoming directory")
            
            logger.info(f"Using source ZIP: {source_zip.name}")
            run_status["stats"]["source_file"] = source_zip.name
            
            # Compute ZIP hash and check if already ingested
            run_status["stage"] = "checking_duplicate"
            zip_hash = self.repo.compute_file_hash(str(source_zip))
            run_status["stats"]["source_hash"] = zip_hash
            
            if self.repo.check_zip_already_ingested(session, zip_hash):
                logger.warning(f"ZIP already ingested: {source_zip.name} (hash: {zip_hash[:8]}...)")
                run_status["status"] = "SKIPPED"
                run_status["stage"] = "duplicate_detected"
                run_status["error"] = "ZIP file already successfully ingested"
                return run_status
            
            # Stage 2: Create ingestion run record
            run_status["stage"] = "initializing_run"
            ingestion_run = self.repo.create_ingestion_run(
                session=session,
                run_id=run_id,
                source_zip_path=str(source_zip),
                source_zip_hash=zip_hash,
                started_at=datetime.utcnow()
            )
            session.commit()
            logger.info(f"Created ingestion run record: {run_id}")
            
            # Stage 3: Extract and parse raw data
            run_status["stage"] = "parsing"
            connector = DataExportConnector()
            raw_data = connector.extract(source_zip)
            
            run_status["stats"]["raw_counts"] = {
                "participants": len(raw_data.get('participants', [])),
                "conversations": len(raw_data.get('conversations', [])),
                "messages": len(raw_data.get('messages', []))
            }
            logger.info(f"Extracted raw data: {run_status['stats']['raw_counts']}")
            
            # Stage 4: Normalize data
            run_status["stage"] = "normalizing"
            normalized_data = normalize_service.normalize_all(raw_data)
            
            # Get normalized counts
            norm_counts = {
                "participants": len(normalized_data.get('participants', [])),
                "conversations": len(normalized_data.get('conversations', [])),
                "messages": len(normalized_data.get('messages', [])),
                "conversation_participants": len(normalized_data.get('conversation_participants', []))
            }
            run_status["stats"]["normalized_counts"] = norm_counts
            run_status["stats"]["normalization_stats"] = normalized_data.get('stats', {})
            logger.info(f"Normalized data: {norm_counts}")
            
            # Stage 5: Insert into database using repo.py
            run_status["stage"] = "inserting_db"
            inserted_counts = self._insert_normalized_data(
                session=session,
                normalized_data=normalized_data,
                ingestion_run_db_id=ingestion_run.id
            )
            
            run_status["stats"]["db_inserted"] = inserted_counts
            logger.info(f"Database insertion complete: {inserted_counts}")
            
            # Commit all inserts
            session.commit()
            
            # Stage 6: Reconcile data
            run_status["stage"] = "reconciling"
            reconciliation_report = self._reconcile_data(
                session=session,
                normalized_data=normalized_data,
                inserted_counts=inserted_counts,
                run_id=run_id
            )
            
            run_status["stats"]["reconciliation"] = reconciliation_report
            
            # Stage 7: Update ingestion run status
            run_status["stage"] = "finalizing_run"
            self.repo.update_ingestion_run(
                session=session,
                run_db_id=ingestion_run.id,
                status='success',
                completed_at=datetime.utcnow(),
                stats={
                    'messages_found': norm_counts['messages'],
                    'messages_inserted': inserted_counts['messages'],
                    'conversations_found': norm_counts['conversations'],
                    'conversations_inserted': inserted_counts['conversations'],
                    'participants_found': norm_counts['participants'],
                    'participants_inserted': inserted_counts['participants']
                }
            )
            session.commit()
            
            # Stage 8: Create output package
            run_status["stage"] = "packaging"
            output_zip = zip_package_service.create_package(
                data=normalized_data,
                run_id=run_id,
                metadata={
                    "source_file": source_zip.name,
                    "reconciliation": reconciliation_report,
                    "inserted_counts": inserted_counts
                }
            )
            
            # Save output package
            saved_output = storage_service.save_output_zip(output_zip, run_id)
            run_status["stats"]["output_file"] = saved_output.name
            logger.info(f"Created output package: {saved_output.name}")
            
            # Stage 9: Archive source ZIP
            run_status["stage"] = "archiving"
            archived_zip = storage_service.archive_raw_zip(source_zip)
            run_status["stats"]["archived_file"] = archived_zip.name
            
            # Optional: Clean up incoming (if configured)
            if settings.DELETE_INCOMING_AFTER_PROCESSING:
                storage_service.cleanup_incoming(source_zip, delete=True)
            
            # Stage 10: Send email
            run_status["stage"] = "emailing"
            if settings.EMAIL_ENABLED and settings.RECIPIENT_EMAILS:
                email_sent = emailer_service.send_data_package(
                    to_emails=settings.RECIPIENT_EMAILS,
                    zip_path=saved_output,
                    run_id=run_id,
                    record_counts=inserted_counts
                )
                run_status["stats"]["email_sent"] = email_sent
            else:
                run_status["stats"]["email_sent"] = False
                logger.info("Email disabled or no recipients configured")
            
            # Success!
            run_status["status"] = "SUCCESS"
            run_status["stage"] = "completed"
            logger.info(f"Ingestion completed successfully [run_id: {run_id}]")
            
        except Exception as e:
            # Handle failure
            run_status["status"] = "FAILED"
            run_status["error"] = str(e)
            run_status["traceback"] = traceback.format_exc()
            
            logger.error(f"Ingestion failed [run_id: {run_id}]: {e}", exc_info=True)
            
            # Update ingestion run as failed
            if ingestion_run:
                try:
                    self.repo.update_ingestion_run(
                        session=session,
                        run_db_id=ingestion_run.id,
                        status='failed',
                        completed_at=datetime.utcnow(),
                        error_message=str(e)
                    )
                    session.commit()
                except Exception as update_err:
                    logger.error(f"Failed to update ingestion run status: {update_err}")
            
            # Rollback transaction
            session.rollback()
            
            # Send error notification
            if settings.EMAIL_ENABLED and settings.RECIPIENT_EMAILS:
                emailer_service.send_error_notification(
                    to_emails=settings.RECIPIENT_EMAILS,
                    error_message=str(e),
                    run_id=run_id
                )
        
        finally:
            run_status["end_time"] = datetime.utcnow().isoformat()
            self.last_run_status = run_status
            self.current_run_id = None
            session.close()
        
        return run_status
    
    def _insert_normalized_data(
        self,
        session,
        normalized_data: dict,
        ingestion_run_db_id: int
    ) -> dict:
        """
        Insert normalized data into database using repo.py methods.
        
        Args:
            session: Database session
            normalized_data: Normalized data from normalize_service
            ingestion_run_db_id: ID of current ingestion run
            
        Returns:
            Dictionary with counts of inserted records
        """
        inserted_counts = {
            'participants': 0,
            'conversations': 0,
            'messages': 0,
            'conversation_participants': 0
        }
        
        # Step 1: Upsert participants
        logger.info("Inserting participants...")
        participant_map = {}  # linkedin_id -> db_id
        
        for participant_data in normalized_data.get('participants', []):
            participant = self.repo.upsert_participant(
                session=session,
                linkedin_id=participant_data['linkedin_id'],
                full_name=participant_data['full_name'],
                profile_url=participant_data.get('profile_url'),
                email=participant_data.get('email'),
                headline=participant_data.get('headline')
            )
            participant_map[participant.linkedin_id] = participant.id
            inserted_counts['participants'] += 1
        
        logger.info(f"Inserted {inserted_counts['participants']} participants")
        
        # Step 2: Upsert conversations
        logger.info("Inserting conversations...")
        conversation_map = {}  # conversation_id -> db_id
        
        for conv_data in normalized_data.get('conversations', []):
            conversation = self.repo.upsert_conversation(
                session=session,
                conversation_id=conv_data['conversation_id'],
                conversation_title=conv_data.get('conversation_title'),
                is_group_chat=conv_data.get('is_group_chat', False),
                first_message_at=conv_data.get('first_message_at'),
                last_message_at=conv_data.get('last_message_at')
            )
            conversation_map[conversation.conversation_id] = conversation.id
            inserted_counts['conversations'] += 1
        
        logger.info(f"Inserted {inserted_counts['conversations']} conversations")
        
        # Step 3: Link conversation participants (junction table)
        logger.info("Linking conversation participants...")
        
        for junction_data in normalized_data.get('conversation_participants', []):
            conv_id = junction_data['conversation_id']
            participant_linkedin_id = junction_data['participant_linkedin_id']
            
            # Get database IDs
            conv_db_id = conversation_map.get(conv_id)
            participant_db_id = participant_map.get(participant_linkedin_id)
            
            if conv_db_id and participant_db_id:
                self.repo.upsert_conversation_participant(
                    session=session,
                    conversation_db_id=conv_db_id,
                    participant_db_id=participant_db_id,
                    joined_at=junction_data.get('joined_at'),
                    left_at=junction_data.get('left_at')
                )
                inserted_counts['conversation_participants'] += 1
            else:
                logger.warning(f"Skipping junction link - missing IDs: "
                             f"conv={conv_id}, participant={participant_linkedin_id}")
        
        logger.info(f"Linked {inserted_counts['conversation_participants']} conversation-participant pairs")
        
        # Step 4: Upsert messages
        logger.info("Inserting messages...")
        
        for msg_data in normalized_data.get('messages', []):
            # Get database IDs from maps
            conv_db_id = conversation_map.get(msg_data['conversation_id'])
            sender_db_id = participant_map.get(msg_data['sender_linkedin_id'])
            
            if not conv_db_id:
                logger.warning(f"Skipping message {msg_data['message_id']} - "
                             f"conversation not found: {msg_data['conversation_id']}")
                continue
            
            if not sender_db_id:
                logger.warning(f"Skipping message {msg_data['message_id']} - "
                             f"sender not found: {msg_data['sender_linkedin_id']}")
                continue
            
            # Insert message
            message = self.repo.upsert_message(
                session=session,
                message_id=msg_data['message_id'],
                conversation_db_id=conv_db_id,
                sender_db_id=sender_db_id,
                content=msg_data.get('content'),
                sent_at=msg_data['sent_at']
            )
            
            # Track message ingestion
            self.repo.track_message_ingestion(
                session=session,
                message_db_id=message.id,
                ingestion_run_db_id=ingestion_run_db_id,
                source_raw_hash=self.repo.compute_hash(msg_data)
            )
            
            inserted_counts['messages'] += 1
            
            # Log progress every 100 messages
            if inserted_counts['messages'] % 100 == 0:
                logger.debug(f"Inserted {inserted_counts['messages']} messages...")
        
        logger.info(f"Inserted {inserted_counts['messages']} messages")
        
        return inserted_counts
    
    def _reconcile_data(
        self,
        session,
        normalized_data: dict,
        inserted_counts: dict,
        run_id: str
    ) -> dict:
        """
        Reconcile normalized data against database state.
        
        Args:
            session: Database session
            normalized_data: Original normalized data
            inserted_counts: Counts of what was inserted
            run_id: Current run ID
            
        Returns:
            Reconciliation report dictionary
        """
        report = {
            "status": "SUCCESS",
            "run_id": run_id,
            "timestamp": datetime.utcnow().isoformat(),
            "checks": {}
        }
        
        # Get database totals
        db_totals = self.repo.get_database_summary(session)
        
        # Check participants
        expected_participants = len(normalized_data.get('participants', []))
        if inserted_counts['participants'] == expected_participants:
            report["checks"]["participants"] = "PASS"
        else:
            report["checks"]["participants"] = "FAIL"
            report["status"] = "WARNING"
        
        # Check conversations
        expected_conversations = len(normalized_data.get('conversations', []))
        if inserted_counts['conversations'] == expected_conversations:
            report["checks"]["conversations"] = "PASS"
        else:
            report["checks"]["conversations"] = "FAIL"
            report["status"] = "WARNING"
        
        # Check messages
        expected_messages = len(normalized_data.get('messages', []))
        if inserted_counts['messages'] == expected_messages:
            report["checks"]["messages"] = "PASS"
        else:
            report["checks"]["messages"] = "FAIL"
            report["status"] = "FAIL"
        
        report["expected"] = {
            "participants": expected_participants,
            "conversations": expected_conversations,
            "messages": expected_messages
        }
        
        report["inserted"] = inserted_counts
        report["db_totals"] = db_totals
        
        return report
    
    def get_last_run_status(self) -> Optional[dict]:
        """Get status of last completed run."""
        return self.last_run_status
    
    def is_running(self) -> bool:
        """Check if a job is currently running."""
        return self.current_run_id is not None


class HealthCheckJob:
    """Periodic health check job."""
    
    def __init__(self):
        """Initialize health check job."""
        self.repo = repository
    
    def run_health_check(self) -> dict:
        """
        Run health checks on system components.
        
        Returns:
            Health status dictionary
        """
        logger.debug("Running health check...")
        
        health_status = {
            "timestamp": datetime.utcnow().isoformat(),
            "status": "HEALTHY",
            "components": {}
        }
        
        # Check database connectivity
        try:
            session = self.repo.get_session()
            db_summary = self.repo.get_database_summary(session)
            session.close()
            
            health_status["components"]["database"] = "OK"
            health_status["database_summary"] = db_summary
        except Exception as e:
            health_status["components"]["database"] = f"ERROR: {str(e)}"
            health_status["status"] = "UNHEALTHY"
            logger.error(f"Database health check failed: {e}")
        
        # Check storage directories
        try:
            stats = storage_service.get_storage_stats()
            health_status["components"]["storage"] = "OK"
            health_status["storage_stats"] = stats
        except Exception as e:
            health_status["components"]["storage"] = f"ERROR: {str(e)}"
            health_status["status"] = "UNHEALTHY"
            logger.error(f"Storage health check failed: {e}")
        
        # Check email configuration (if enabled)
        if settings.EMAIL_ENABLED:
            try:
                if emailer_service.enabled:
                    health_status["components"]["email"] = "CONFIGURED"
                else:
                    health_status["components"]["email"] = "DISABLED"
            except Exception as e:
                health_status["components"]["email"] = f"ERROR: {str(e)}"
                logger.error(f"Email health check failed: {e}")
        else:
            health_status["components"]["email"] = "DISABLED"
        
        logger.debug(f"Health check completed: {health_status['status']}")
        return health_status


# Singleton instances
ingestion_job = IngestionJob()
health_check_job = HealthCheckJob()


# Scheduled job functions (called by APScheduler)
def scheduled_ingestion():
    """Scheduled ingestion job wrapper."""
    logger.info("Triggered scheduled ingestion")
    try:
        result = ingestion_job.run_ingestion()
        return result
    except Exception as e:
        logger.error(f"Scheduled ingestion failed: {e}", exc_info=True)
        raise


def scheduled_health_check():
    """Scheduled health check job wrapper."""
    try:
        result = health_check_job.run_health_check()
        return result
    except Exception as e:
        logger.error(f"Scheduled health check failed: {e}", exc_info=True)
        raise