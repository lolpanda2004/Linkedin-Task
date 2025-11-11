# app/db/repo.py
"""
Repository layer for database operations
Handles all inserts/upserts with idempotency guarantees
NO parsing logic - only database I/O operations
"""

from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from sqlalchemy import create_engine, select, update, and_, func
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import IntegrityError
from dateutil import parser
import hashlib
import json
import logging

from app.db.models import (
    Base, Participant, Conversation, Message, ConversationParticipant,
    IngestionRun, MessageIngestionTracking, MessageAttachment, MessageReaction,
    SchemaVersion
)

logger = logging.getLogger(__name__)


class DatabaseRepository:
    """
    Single responsibility: Database write operations with idempotency
    All methods handle upsert logic to prevent duplicates
    """
    
    def __init__(self, db_url: str = "sqlite:///./linkedin_messages.db"):
        """
        Initialize database connection and create tables
        
        Args:
            db_url: SQLAlchemy database URL
                Examples:
                - SQLite: "sqlite:///./data/linkedin_messages.db"
                - PostgreSQL: "postgresql://user:pass@localhost/linkedin_db"
        """
        self.db_url = db_url
        self.engine = create_engine(
            db_url,
            connect_args={"check_same_thread": False} if "sqlite" in db_url else {},
            echo=False,  # Set to True for SQL debugging
            pool_pre_ping=True  # Verify connections before using
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        # Create all tables if they don't exist
        Base.metadata.create_all(bind=self.engine)
        logger.info(f"Database initialized: {db_url}")
    
    def get_session(self) -> Session:
        """
        Get a new database session
        
        Usage:
            with repo.get_session() as session:
                try:
                    # operations
                    session.commit()
                except Exception:
                    session.rollback()
                    raise
        """
        return self.SessionLocal()
    
    def close(self):
        """Close database engine and connections"""
        self.engine.dispose()
        logger.info("Database connections closed")
    
    # ========================================================================
    # PARTICIPANT OPERATIONS
    # ========================================================================
    
    def upsert_participant(
        self, 
        session: Session,
        linkedin_id: str,
        full_name: str,
        profile_url: Optional[str] = None,
        email: Optional[str] = None,
        headline: Optional[str] = None
    ) -> Participant:
        """
        Insert or update participant (idempotent)
        Uses linkedin_id as unique key
        
        Args:
            session: Active database session
            linkedin_id: LinkedIn unique identifier (required)
            full_name: Participant's full name (required)
            profile_url: LinkedIn profile URL (optional)
            email: Email address (optional)
            headline: Professional headline (optional)
        
        Returns:
            Participant object (new or existing)
        """
        # Check if participant exists
        existing = session.query(Participant).filter(
            Participant.linkedin_id == linkedin_id
        ).first()
        
        if existing:
            # Update existing record (only non-null values)
            updated = False
            if full_name and existing.full_name != full_name:
                existing.full_name = full_name
                updated = True
            if profile_url and existing.profile_url != profile_url:
                existing.profile_url = profile_url
                updated = True
            if email and existing.email != email:
                existing.email = email
                updated = True
            if headline and existing.headline != headline:
                existing.headline = headline
                updated = True
            
            if updated:
                existing.updated_at = datetime.utcnow()
                session.flush()
                logger.debug(f"Updated participant: {linkedin_id}")
            
            return existing
        else:
            # Insert new record
            new_participant = Participant(
                linkedin_id=linkedin_id,
                full_name=full_name,
                profile_url=profile_url,
                email=email,
                headline=headline
            )
            session.add(new_participant)
            session.flush()
            logger.debug(f"Created participant: {linkedin_id}")
            return new_participant
    
    def get_participant_by_linkedin_id(self, session: Session, linkedin_id: str) -> Optional[Participant]:
        """
        Retrieve participant by LinkedIn ID
        
        Returns:
            Participant object or None if not found
        """
        return session.query(Participant).filter(
            Participant.linkedin_id == linkedin_id
        ).first()
    
    def get_all_participants(self, session: Session, limit: Optional[int] = None) -> List[Participant]:
        """Get all participants, optionally limited"""
        query = session.query(Participant).order_by(Participant.full_name)
        if limit:
            query = query.limit(limit)
        return query.all()
    
    # ========================================================================
    # CONVERSATION OPERATIONS
    # ========================================================================
    
    def upsert_conversation(
        self,
        session: Session,
        conversation_id: str,
        conversation_title: Optional[str] = None,
        is_group_chat: bool = False,
        first_message_at: Optional[datetime] = None,
        last_message_at: Optional[datetime] = None
    ) -> Conversation:
        """
        Insert or update conversation (idempotent)
        Uses conversation_id as unique key
        
        Args:
            session: Active database session
            conversation_id: Unique conversation identifier (required)
            conversation_title: Conversation/group name (optional)
            is_group_chat: True if more than 2 participants (default False)
            first_message_at: Timestamp of first message (optional)
            last_message_at: Timestamp of last message (optional)
        
        Returns:
            Conversation object (new or existing)
        """
        existing = session.query(Conversation).filter(
            Conversation.conversation_id == conversation_id
        ).first()
        
        if existing:
            # Update if new data provided
            updated = False
            
            if conversation_title and existing.conversation_title != conversation_title:
                existing.conversation_title = conversation_title
                updated = True
            
            if existing.is_group_chat != is_group_chat:
                existing.is_group_chat = is_group_chat
                updated = True
            
            # Update timestamps (keep earliest first, latest last)
            if first_message_at:
                if existing.first_message_at is None or first_message_at < existing.first_message_at:
                    existing.first_message_at = first_message_at
                    updated = True
            
            if last_message_at:
                if existing.last_message_at is None or last_message_at > existing.last_message_at:
                    existing.last_message_at = last_message_at
                    updated = True
            
            if updated:
                existing.updated_at = datetime.utcnow()
                session.flush()
                logger.debug(f"Updated conversation: {conversation_id}")
            
            return existing
        else:
            # Insert new conversation
            new_conversation = Conversation(
                conversation_id=conversation_id,
                conversation_title=conversation_title,
                is_group_chat=is_group_chat,
                first_message_at=first_message_at,
                last_message_at=last_message_at
            )
            session.add(new_conversation)
            session.flush()
            logger.debug(f"Created conversation: {conversation_id}")
            return new_conversation
    
    def get_conversation_by_conversation_id(
        self, 
        session: Session, 
        conversation_id: str
    ) -> Optional[Conversation]:
        """Retrieve conversation by conversation_id"""
        return session.query(Conversation).filter(
            Conversation.conversation_id == conversation_id
        ).first()
    
    def update_conversation_timestamps(
        self,
        session: Session,
        conversation_db_id: int,
        message_sent_at: datetime
    ):
        """
        Update conversation's first/last message timestamps
        Called automatically when inserting messages
        
        Args:
            conversation_db_id: Database ID of conversation
            message_sent_at: Timestamp of message being inserted
        """
        conv = session.query(Conversation).filter(Conversation.id == conversation_db_id).first()
        if conv:
            updated = False
            
            if conv.first_message_at is None or message_sent_at < conv.first_message_at:
                conv.first_message_at = message_sent_at
                updated = True
            
            if conv.last_message_at is None or message_sent_at > conv.last_message_at:
                conv.last_message_at = message_sent_at
                updated = True
            
            if updated:
                session.flush()
    
    def get_all_conversations(self, session: Session, limit: Optional[int] = None) -> List[Conversation]:
        """Get all conversations, optionally limited"""
        query = session.query(Conversation).order_by(Conversation.last_message_at.desc())
        if limit:
            query = query.limit(limit)
        return query.all()
    
    # ========================================================================
    # MESSAGE OPERATIONS
    # ========================================================================
    
    def upsert_message(
        self,
        session: Session,
        message_id: str,
        conversation_db_id: int,
        sender_db_id: int,
        content: Optional[str],
        sent_at: datetime
    ) -> Message:
        """
        Insert or update message (idempotent)
        Also updates conversation timestamps
        Uses message_id as unique key
        
        Args:
            session: Active database session
            message_id: Unique message identifier (required)
            conversation_db_id: Database ID of conversation (FK) (required)
            sender_db_id: Database ID of sender participant (FK) (required)
            content: Message text content (optional, can be empty)
            sent_at: Timestamp when message was sent (required)
        
        Returns:
            Message object (new or existing)
        """
        existing = session.query(Message).filter(
            Message.message_id == message_id
        ).first()
        
        if existing:
            # Update existing message
            updated = False
            
            if content is not None and existing.content != content:
                existing.content = content
                updated = True
            
            if existing.conversation_id != conversation_db_id:
                existing.conversation_id = conversation_db_id
                updated = True
            
            if existing.sender_id != sender_db_id:
                existing.sender_id = sender_db_id
                updated = True
            
            if existing.sent_at != sent_at:
                existing.sent_at = sent_at
                updated = True
            if isinstance(sent_at, str):
                sent_at = parser.parse(sent_at)

            if updated:
                existing.updated_at = datetime.utcnow()
                session.flush()
                logger.debug(f"Updated message: {message_id}")
            
            return existing
        else:
            # Insert new message
            new_message = Message(
                message_id=message_id,
                conversation_id=conversation_db_id,
                sender_id=sender_db_id,
                content=content,
                sent_at=sent_at
            )
            session.add(new_message)
            session.flush()
            
            # Update conversation timestamps
            self.update_conversation_timestamps(session, conversation_db_id, sent_at)
            
            logger.debug(f"Created message: {message_id}")
            return new_message
    
    def get_message_by_message_id(self, session: Session, message_id: str) -> Optional[Message]:
        """Retrieve message by message_id"""
        return session.query(Message).filter(Message.message_id == message_id).first()
    
    def get_messages_by_conversation(
        self, 
        session: Session, 
        conversation_db_id: int,
        limit: Optional[int] = None
    ) -> List[Message]:
        """Get all messages in a conversation, ordered by sent_at"""
        query = session.query(Message).filter(
            Message.conversation_id == conversation_db_id
        ).order_by(Message.sent_at)
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    # ========================================================================
    # CONVERSATION PARTICIPANT OPERATIONS
    # ========================================================================
    
    def upsert_conversation_participant(
        self,
        session: Session,
        conversation_db_id: int,
        participant_db_id: int,
        joined_at: Optional[datetime] = None,
        left_at: Optional[datetime] = None
    ) -> ConversationParticipant:
        """
        Link participant to conversation (idempotent)
        Creates many-to-many relationship
        
        Args:
            session: Active database session
            conversation_db_id: Database ID of conversation (required)
            participant_db_id: Database ID of participant (required)
            joined_at: When participant joined (optional)
            left_at: When participant left (optional)
        
        Returns:
            ConversationParticipant object
        """
        existing = session.query(ConversationParticipant).filter(
            and_(
                ConversationParticipant.conversation_id == conversation_db_id,
                ConversationParticipant.participant_id == participant_db_id
            )
        ).first()
        
        if existing:
            # Update timestamps if provided
            updated = False
            
            if joined_at and (existing.joined_at is None or joined_at < existing.joined_at):
                existing.joined_at = joined_at
                updated = True
            
            if left_at and (existing.left_at is None or left_at > existing.left_at):
                existing.left_at = left_at
                updated = True
            
            if updated:
                session.flush()
            
            return existing
        else:
            # Create new link
            new_link = ConversationParticipant(
                conversation_id=conversation_db_id,
                participant_id=participant_db_id,
                joined_at=joined_at,
                left_at=left_at
            )
            session.add(new_link)
            session.flush()
            logger.debug(f"Linked participant {participant_db_id} to conversation {conversation_db_id}")
            return new_link
    
    def get_conversation_participants(
        self, 
        session: Session, 
        conversation_db_id: int
    ) -> List[Participant]:
        """Get all participants in a conversation"""
        return session.query(Participant).join(ConversationParticipant).filter(
            ConversationParticipant.conversation_id == conversation_db_id
        ).all()
    
    # ========================================================================
    # INGESTION RUN OPERATIONS
    # ========================================================================
    
    def create_ingestion_run(
        self,
        session: Session,
        run_id: str,
        source_zip_path: str,
        source_zip_hash: str,
        started_at: datetime
    ) -> IngestionRun:
        """
        Create new ingestion run record
        Marks the start of an ETL job
        
        Args:
            session: Active database session
            run_id: Unique run identifier (UUID recommended)
            source_zip_path: Path to source ZIP file
            source_zip_hash: SHA256 hash of ZIP file
            started_at: When ingestion started
        
        Returns:
            IngestionRun object
        """
        new_run = IngestionRun(
            run_id=run_id,
            source_zip_path=source_zip_path,
            source_zip_hash=source_zip_hash,
            started_at=started_at,
            status='running'
        )
        session.add(new_run)
        session.flush()
        logger.info(f"Created ingestion run: {run_id}")
        return new_run
    
    def update_ingestion_run(
        self,
        session: Session,
        run_db_id: int,
        status: str,
        completed_at: Optional[datetime] = None,
        error_message: Optional[str] = None,
        stats: Optional[Dict[str, int]] = None
    ):
        """
        Update ingestion run status and statistics
        Called when run completes (success or failure)
        
        Args:
            session: Active database session
            run_db_id: Database ID of ingestion run
            status: 'running', 'success', or 'failed'
            completed_at: When run completed
            error_message: Error details if failed
            stats: Dict with counts:
                - messages_found
                - messages_inserted
                - conversations_found
                - conversations_inserted
                - participants_found
                - participants_inserted
        """
        run = session.query(IngestionRun).filter(IngestionRun.id == run_db_id).first()
        if not run:
            logger.error(f"Ingestion run {run_db_id} not found")
            return
        
        run.status = status
        run.completed_at = completed_at
        run.error_message = error_message
        
        if stats:
            run.total_messages_found = stats.get('messages_found', 0)
            run.total_messages_inserted = stats.get('messages_inserted', 0)
            run.total_conversations_found = stats.get('conversations_found', 0)
            run.total_conversations_inserted = stats.get('conversations_inserted', 0)
            run.total_participants_found = stats.get('participants_found', 0)
            run.total_participants_inserted = stats.get('participants_inserted', 0)
        
        session.flush()
        logger.info(f"Updated ingestion run {run_db_id}: status={status}")
    
    def get_latest_ingestion_run(self, session: Session) -> Optional[IngestionRun]:
        """Get most recent ingestion run"""
        return session.query(IngestionRun).order_by(
            IngestionRun.started_at.desc()
        ).first()
    
    def get_ingestion_run_by_id(self, session: Session, run_id: str) -> Optional[IngestionRun]:
        """Get ingestion run by run_id"""
        return session.query(IngestionRun).filter(IngestionRun.run_id == run_id).first()
    
    def check_zip_already_ingested(self, session: Session, zip_hash: str) -> bool:
        """
        Check if a ZIP file has already been successfully ingested
        Prevents duplicate processing
        
        Args:
            zip_hash: SHA256 hash of ZIP file
        
        Returns:
            True if already ingested successfully, False otherwise
        """
        existing = session.query(IngestionRun).filter(
            and_(
                IngestionRun.source_zip_hash == zip_hash,
                IngestionRun.status == 'success'
            )
        ).first()
        return existing is not None
    
    # ========================================================================
    # MESSAGE INGESTION TRACKING
    # ========================================================================
    
    def track_message_ingestion(
        self,
        session: Session,
        message_db_id: int,
        ingestion_run_db_id: int,
        source_raw_hash: Optional[str] = None
    ) -> MessageIngestionTracking:
        """
        Link message to ingestion run for reconciliation
        Enables tracing which messages came from which ingestion
        
        Args:
            session: Active database session
            message_db_id: Database ID of message
            ingestion_run_db_id: Database ID of ingestion run
            source_raw_hash: Hash of raw source data (optional)
        
        Returns:
            MessageIngestionTracking object
        """
        # Check if already tracked
        existing = session.query(MessageIngestionTracking).filter(
            and_(
                MessageIngestionTracking.message_id == message_db_id,
                MessageIngestionTracking.ingestion_run_id == ingestion_run_db_id
            )
        ).first()
        
        if existing:
            return existing
        
        new_tracking = MessageIngestionTracking(
            message_id=message_db_id,
            ingestion_run_id=ingestion_run_db_id,
            source_raw_hash=source_raw_hash
        )
        session.add(new_tracking)
        session.flush()
        return new_tracking
    
    # ========================================================================
    # ATTACHMENT & REACTION OPERATIONS
    # ========================================================================
    
    def upsert_message_attachment(
        self,
        session: Session,
        message_db_id: int,
        attachment_type: Optional[str] = None,
        file_name: Optional[str] = None,
        file_path: Optional[str] = None,
        file_url: Optional[str] = None,
        file_size_bytes: Optional[int] = None,
        mime_type: Optional[str] = None
    ) -> MessageAttachment:
        """
        Insert message attachment
        
        Args:
            session: Active database session
            message_db_id: Database ID of message
            attachment_type: 'image', 'document', 'link', 'video'
            file_name: Original filename
            file_path: Local storage path
            file_url: Original URL
            file_size_bytes: File size in bytes
            mime_type: MIME type
        
        Returns:
            MessageAttachment object
        """
        new_attachment = MessageAttachment(
            message_id=message_db_id,
            attachment_type=attachment_type,
            file_name=file_name,
            file_path=file_path,
            file_url=file_url,
            file_size_bytes=file_size_bytes,
            mime_type=mime_type
        )
        session.add(new_attachment)
        session.flush()
        logger.debug(f"Created attachment for message {message_db_id}")
        return new_attachment
    
    def upsert_message_reaction(
        self,
        session: Session,
        message_db_id: int,
        participant_db_id: int,
        reaction_type: str,
        reacted_at: Optional[datetime] = None
    ) -> MessageReaction:
        """
        Insert or update message reaction (idempotent)
        
        Args:
            session: Active database session
            message_db_id: Database ID of message
            participant_db_id: Database ID of participant who reacted
            reaction_type: 'like', 'love', 'insightful', etc.
            reacted_at: When reaction was added
        
        Returns:
            MessageReaction object
        """
        existing = session.query(MessageReaction).filter(
            and_(
                MessageReaction.message_id == message_db_id,
                MessageReaction.participant_id == participant_db_id,
                MessageReaction.reaction_type == reaction_type
            )
        ).first()
        
        if existing:
            if reacted_at:
                existing.reacted_at = reacted_at
                session.flush()
            return existing
        
        new_reaction = MessageReaction(
            message_id=message_db_id,
            participant_id=participant_db_id,
            reaction_type=reaction_type,
            reacted_at=reacted_at
        )
        session.add(new_reaction)
        session.flush()
        logger.debug(f"Created reaction for message {message_db_id}")
        return new_reaction
    
    # ========================================================================
    # RECONCILIATION QUERIES
    # ========================================================================
    
    def get_message_count(self, session: Session) -> int:
        """Total messages in database"""
        return session.query(func.count(Message.id)).scalar()
    
    def get_conversation_count(self, session: Session) -> int:
        """Total conversations in database"""
        return session.query(func.count(Conversation.id)).scalar()
    
    def get_participant_count(self, session: Session) -> int:
        """Total participants in database"""
        return session.query(func.count(Participant.id)).scalar()
    
    def get_messages_by_run(self, session: Session, run_db_id: int) -> List[Message]:
        """
        Get all messages ingested in a specific run
        
        Args:
            run_db_id: Database ID of ingestion run
        
        Returns:
            List of Message objects
        """
        return session.query(Message).join(MessageIngestionTracking).filter(
            MessageIngestionTracking.ingestion_run_id == run_db_id
        ).all()
    
    def get_ingestion_run_stats(self, session: Session, run_db_id: int) -> Dict[str, Any]:
        """
        Get detailed statistics for an ingestion run
        
        Args:
            run_db_id: Database ID of ingestion run
        
        Returns:
            Dict with run statistics and metadata
        """
        run = session.query(IngestionRun).filter(IngestionRun.id == run_db_id).first()
        if not run:
            return {}
        
        return {
            'run_id': run.run_id,
            'status': run.status,
            'started_at': run.started_at.isoformat() if run.started_at else None,
            'completed_at': run.completed_at.isoformat() if run.completed_at else None,
            'duration_seconds': (
                (run.completed_at - run.started_at).total_seconds()
                if run.completed_at and run.started_at else None
            ),
            'source_zip': run.source_zip_path,
            'source_hash': run.source_zip_hash,
            'messages_found': run.total_messages_found,
            'messages_inserted': run.total_messages_inserted,
            'conversations_found': run.total_conversations_found,
            'conversations_inserted': run.total_conversations_inserted,
            'participants_found': run.total_participants_found,
            'participants_inserted': run.total_participants_inserted,
            'error': run.error_message
        }
    
    def get_all_ingestion_runs(
        self, 
        session: Session, 
        limit: Optional[int] = 10
    ) -> List[IngestionRun]:
        """Get recent ingestion runs"""
        query = session.query(IngestionRun).order_by(IngestionRun.started_at.desc())
        if limit:
            query = query.limit(limit)
        return query.all()
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    @staticmethod
    def compute_hash(data: Any) -> str:
        """
        Compute SHA256 hash of data (for reconciliation)
        
        Args:
            data: Any serializable object (dict, list, str, etc.)
        
        Returns:
            Hex string of SHA256 hash
        """
        if isinstance(data, (dict, list)):
            data_str = json.dumps(data, sort_keys=True, default=str)
        else:
            data_str = str(data)
        
        return hashlib.sha256(data_str.encode('utf-8')).hexdigest()
    
    @staticmethod
    def compute_file_hash(file_path: str) -> str:
        """
        Compute SHA256 hash of a file
        
        Args:
            file_path: Path to file
        
        Returns:
            Hex string of SHA256 hash
        """
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            # Read in 64kb chunks
            for byte_block in iter(lambda: f.read(65536), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def get_database_summary(self, session: Session) -> Dict[str, Any]:
        """
        Get overall database statistics
        
        Returns:
            Dict with counts and metadata
        """
        return {
            'total_participants': self.get_participant_count(session),
            'total_conversations': self.get_conversation_count(session),
            'total_messages': self.get_message_count(session),
            'latest_message': session.query(func.max(Message.sent_at)).scalar(),
            'earliest_message': session.query(func.min(Message.sent_at)).scalar(),
            'total_ingestion_runs': session.query(func.count(IngestionRun.id)).scalar(),
            'successful_runs': session.query(func.count(IngestionRun.id)).filter(
                IngestionRun.status == 'success'
            ).scalar()
        }