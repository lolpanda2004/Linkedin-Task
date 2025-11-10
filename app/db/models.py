# app/db/models.py
"""
SQLAlchemy ORM models matching schema.sql
Maps database tables to Python objects for type-safe operations
"""

from datetime import datetime
from typing import Optional, List
from sqlalchemy import (
    Boolean, Column, Integer, String, Text, DateTime, ForeignKey, 
    UniqueConstraint, Index, event
)
from sqlalchemy.orm import declarative_base, relationship, Session
from sqlalchemy.sql import func

Base = declarative_base()


# ============================================================================
# CORE ENTITIES
# ============================================================================

class Participant(Base):
    """LinkedIn user/contact - stores unique participants across all conversations"""
    __tablename__ = "participants"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    linkedin_id = Column(String, unique=True, nullable=False, index=True)
    full_name = Column(String, nullable=False, index=True)
    profile_url = Column(String, nullable=True)
    email = Column(String, nullable=True)
    headline = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    sent_messages = relationship("Message", back_populates="sender", cascade="all, delete-orphan")
    conversations = relationship("ConversationParticipant", back_populates="participant", cascade="all, delete-orphan")
    reactions = relationship("MessageReaction", back_populates="participant", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Participant(id={self.id}, name='{self.full_name}', linkedin_id='{self.linkedin_id}')>"


class Conversation(Base):
    """Message thread/conversation - can be 1:1 or group chat"""
    __tablename__ = "conversations"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    conversation_id = Column(String, unique=True, nullable=False, index=True)
    conversation_title = Column(String, nullable=True)
    is_group_chat = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    first_message_at = Column(DateTime, nullable=True)
    last_message_at = Column(DateTime, nullable=True, index=True)
    
    # Relationships
    messages = relationship("Message", back_populates="conversation", cascade="all, delete-orphan")
    participants = relationship("ConversationParticipant", back_populates="conversation", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Conversation(id={self.id}, conversation_id='{self.conversation_id}', group={self.is_group_chat})>"


class Message(Base):
    """Individual message within a conversation"""
    __tablename__ = "messages"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(String, unique=True, nullable=False, index=True)
    conversation_id = Column(Integer, ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False, index=True)
    sender_id = Column(Integer, ForeignKey("participants.id", ondelete="CASCADE"), nullable=False, index=True)
    content = Column(Text, nullable=True)
    sent_at = Column(DateTime, nullable=False, index=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    conversation = relationship("Conversation", back_populates="messages")
    sender = relationship("Participant", back_populates="sent_messages")
    attachments = relationship("MessageAttachment", back_populates="message", cascade="all, delete-orphan")
    reactions = relationship("MessageReaction", back_populates="message", cascade="all, delete-orphan")
    ingestion_tracking = relationship("MessageIngestionTracking", back_populates="message", cascade="all, delete-orphan")
    
    def __repr__(self):
        preview = self.content[:50] if self.content else ""
        return f"<Message(id={self.id}, message_id='{self.message_id}', preview='{preview}...')>"


class ConversationParticipant(Base):
    """Junction table linking conversations and participants (many-to-many)"""
    __tablename__ = "conversation_participants"
    __table_args__ = (
        UniqueConstraint('conversation_id', 'participant_id', name='uq_conv_participant'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    conversation_id = Column(Integer, ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False, index=True)
    participant_id = Column(Integer, ForeignKey("participants.id", ondelete="CASCADE"), nullable=False, index=True)
    joined_at = Column(DateTime, nullable=True)
    left_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    
    # Relationships
    conversation = relationship("Conversation", back_populates="participants")
    participant = relationship("Participant", back_populates="conversations")
    
    def __repr__(self):
        return f"<ConversationParticipant(conv_id={self.conversation_id}, participant_id={self.participant_id})>"


# ============================================================================
# METADATA & TRACKING
# ============================================================================

class IngestionRun(Base):
    """Tracks each ETL job execution for auditing and reconciliation"""
    __tablename__ = "ingestion_runs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, unique=True, nullable=False, index=True)
    source_zip_path = Column(String, nullable=False)
    source_zip_hash = Column(String, nullable=False)
    started_at = Column(DateTime, nullable=False, index=True)
    completed_at = Column(DateTime, nullable=True)
    status = Column(String, nullable=False, index=True)  # 'running', 'success', 'failed'
    total_messages_found = Column(Integer, default=0)
    total_messages_inserted = Column(Integer, default=0)
    total_conversations_found = Column(Integer, default=0)
    total_conversations_inserted = Column(Integer, default=0)
    total_participants_found = Column(Integer, default=0)
    total_participants_inserted = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    
    # Relationships
    message_tracking = relationship("MessageIngestionTracking", back_populates="ingestion_run", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<IngestionRun(run_id='{self.run_id}', status='{self.status}', started={self.started_at})>"


class MessageIngestionTracking(Base):
    """Links messages to specific ingestion runs for reconciliation"""
    __tablename__ = "message_ingestion_tracking"
    __table_args__ = (
        UniqueConstraint('message_id', 'ingestion_run_id', name='uq_msg_ingestion'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(Integer, ForeignKey("messages.id", ondelete="CASCADE"), nullable=False, index=True)
    ingestion_run_id = Column(Integer, ForeignKey("ingestion_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    source_raw_hash = Column(String, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    
    # Relationships
    message = relationship("Message", back_populates="ingestion_tracking")
    ingestion_run = relationship("IngestionRun", back_populates="message_tracking")
    
    def __repr__(self):
        return f"<MessageIngestionTracking(msg_id={self.message_id}, run_id={self.ingestion_run_id})>"


# ============================================================================
# OPTIONAL: ATTACHMENTS & REACTIONS
# ============================================================================

class MessageAttachment(Base):
    """Files, images, links attached to messages"""
    __tablename__ = "message_attachments"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(Integer, ForeignKey("messages.id", ondelete="CASCADE"), nullable=False, index=True)
    attachment_type = Column(String, nullable=True)  # 'image', 'document', 'link', 'video'
    file_name = Column(String, nullable=True)
    file_path = Column(String, nullable=True)
    file_url = Column(String, nullable=True)
    file_size_bytes = Column(Integer, nullable=True)
    mime_type = Column(String, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    
    # Relationships
    message = relationship("Message", back_populates="attachments")
    
    def __repr__(self):
        return f"<MessageAttachment(id={self.id}, type='{self.attachment_type}', file='{self.file_name}')>"


class MessageReaction(Base):
    """Reactions/likes on messages"""
    __tablename__ = "message_reactions"
    __table_args__ = (
        UniqueConstraint('message_id', 'participant_id', 'reaction_type', name='uq_msg_reaction'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(Integer, ForeignKey("messages.id", ondelete="CASCADE"), nullable=False, index=True)
    participant_id = Column(Integer, ForeignKey("participants.id", ondelete="CASCADE"), nullable=False, index=True)
    reaction_type = Column(String, nullable=False)  # 'like', 'love', 'insightful', etc.
    reacted_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    
    # Relationships
    message = relationship("Message", back_populates="reactions")
    participant = relationship("Participant", back_populates="reactions")
    
    def __repr__(self):
        return f"<MessageReaction(msg_id={self.message_id}, participant_id={self.participant_id}, type='{self.reaction_type}')>"


class SchemaVersion(Base):
    """Tracks database schema migrations"""
    __tablename__ = "schema_version"
    
    version = Column(Integer, primary_key=True)
    applied_at = Column(DateTime, default=func.now(), nullable=False)
    description = Column(Text, nullable=True)
    
    def __repr__(self):
        return f"<SchemaVersion(version={self.version}, applied={self.applied_at})>"


# ============================================================================
# SQLALCHEMY EVENTS (for triggers not handled by onupdate)
# ============================================================================

@event.listens_for(Message, 'after_insert')
def update_conversation_timestamps(mapper, connection, target):
    """
    Trigger equivalent: Update conversation's first/last message timestamps
    when new message is inserted
    """
    # Note: This is handled in repo.py to avoid session conflicts
    pass