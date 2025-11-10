-- app/db/schema.sql
-- LinkedIn Message Ingestion Database Schema
-- Normalized structure for storing LinkedIn messages, conversations, and participants

-- ============================================================================
-- CORE ENTITIES
-- ============================================================================

-- Participants table: stores unique LinkedIn users
CREATE TABLE IF NOT EXISTS participants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    linkedin_id TEXT UNIQUE NOT NULL,           -- LinkedIn's internal user ID (if available)
    full_name TEXT NOT NULL,
    profile_url TEXT,                            -- LinkedIn profile URL
    email TEXT,                                  -- Email if available in export
    headline TEXT,                               -- Professional headline
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_participants_linkedin_id ON participants(linkedin_id);
CREATE INDEX idx_participants_full_name ON participants(full_name);

-- Conversations table: stores unique message threads
CREATE TABLE IF NOT EXISTS conversations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id TEXT UNIQUE NOT NULL,        -- LinkedIn's conversation ID
    conversation_title TEXT,                     -- Group chat name or derived title
    is_group_chat BOOLEAN DEFAULT 0,             -- TRUE if more than 2 participants
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    first_message_at TIMESTAMP,                  -- Timestamp of first message in thread
    last_message_at TIMESTAMP                    -- Timestamp of most recent message
);

CREATE INDEX idx_conversations_conversation_id ON conversations(conversation_id);
CREATE INDEX idx_conversations_last_message_at ON conversations(last_message_at);

-- Messages table: stores individual messages
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT UNIQUE NOT NULL,             -- Unique message identifier
    conversation_id INTEGER NOT NULL,            -- FK to conversations
    sender_id INTEGER NOT NULL,                  -- FK to participants
    content TEXT,                                -- Message body text
    sent_at TIMESTAMP NOT NULL,                  -- When message was sent
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE,
    FOREIGN KEY (sender_id) REFERENCES participants(id) ON DELETE CASCADE
);

CREATE INDEX idx_messages_message_id ON messages(message_id);
CREATE INDEX idx_messages_conversation_id ON messages(conversation_id);
CREATE INDEX idx_messages_sender_id ON messages(sender_id);
CREATE INDEX idx_messages_sent_at ON messages(sent_at);

-- Conversation participants junction table (many-to-many)
CREATE TABLE IF NOT EXISTS conversation_participants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id INTEGER NOT NULL,
    participant_id INTEGER NOT NULL,
    joined_at TIMESTAMP,                         -- When they joined (for group chats)
    left_at TIMESTAMP,                           -- When they left (if applicable)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE,
    FOREIGN KEY (participant_id) REFERENCES participants(id) ON DELETE CASCADE,
    UNIQUE(conversation_id, participant_id)      -- Prevent duplicate entries
);

CREATE INDEX idx_conv_participants_conversation ON conversation_participants(conversation_id);
CREATE INDEX idx_conv_participants_participant ON conversation_participants(participant_id);

-- ============================================================================
-- METADATA & TRACKING
-- ============================================================================

-- Ingestion runs table: tracks each ingestion job
CREATE TABLE IF NOT EXISTS ingestion_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT UNIQUE NOT NULL,                 -- UUID for this ingestion
    source_zip_path TEXT NOT NULL,               -- Path to source ZIP file
    source_zip_hash TEXT NOT NULL,               -- SHA256 hash of source ZIP
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    status TEXT NOT NULL,                        -- 'running', 'success', 'failed'
    total_messages_found INTEGER DEFAULT 0,
    total_messages_inserted INTEGER DEFAULT 0,
    total_conversations_found INTEGER DEFAULT 0,
    total_conversations_inserted INTEGER DEFAULT 0,
    total_participants_found INTEGER DEFAULT 0,
    total_participants_inserted INTEGER DEFAULT 0,
    error_message TEXT,                          -- Error details if failed
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ingestion_runs_run_id ON ingestion_runs(run_id);
CREATE INDEX idx_ingestion_runs_started_at ON ingestion_runs(started_at);
CREATE INDEX idx_ingestion_runs_status ON ingestion_runs(status);

-- Message ingestion tracking: links messages to ingestion runs
CREATE TABLE IF NOT EXISTS message_ingestion_tracking (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL,
    ingestion_run_id INTEGER NOT NULL,
    source_raw_hash TEXT,                        -- Hash of raw source data for this message
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE,
    FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id) ON DELETE CASCADE,
    UNIQUE(message_id, ingestion_run_id)         -- Each message tracked once per run
);

CREATE INDEX idx_msg_tracking_message ON message_ingestion_tracking(message_id);
CREATE INDEX idx_msg_tracking_run ON message_ingestion_tracking(ingestion_run_id);

-- ============================================================================
-- OPTIONAL: ATTACHMENTS & REACTIONS (if LinkedIn export includes these)
-- ============================================================================

-- Message attachments table
CREATE TABLE IF NOT EXISTS message_attachments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL,
    attachment_type TEXT,                        -- 'image', 'document', 'link', 'video'
    file_name TEXT,
    file_path TEXT,                              -- Path to stored file (if downloaded)
    file_url TEXT,                               -- Original URL
    file_size_bytes INTEGER,
    mime_type TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE
);

CREATE INDEX idx_attachments_message ON message_attachments(message_id);

-- Message reactions table (if LinkedIn includes reaction data)
CREATE TABLE IF NOT EXISTS message_reactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL,
    participant_id INTEGER NOT NULL,
    reaction_type TEXT NOT NULL,                 -- 'like', 'love', 'insightful', etc.
    reacted_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE,
    FOREIGN KEY (participant_id) REFERENCES participants(id) ON DELETE CASCADE,
    UNIQUE(message_id, participant_id, reaction_type)
);

CREATE INDEX idx_reactions_message ON message_reactions(message_id);
CREATE INDEX idx_reactions_participant ON message_reactions(participant_id);

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- View: Messages with sender details
CREATE VIEW IF NOT EXISTS vw_messages_with_sender AS
SELECT 
    m.id,
    m.message_id,
    m.conversation_id,
    m.content,
    m.sent_at,
    p.full_name AS sender_name,
    p.linkedin_id AS sender_linkedin_id,
    p.profile_url AS sender_profile_url
FROM messages m
JOIN participants p ON m.sender_id = p.id;

-- View: Conversation summary with participant count
CREATE VIEW IF NOT EXISTS vw_conversation_summary AS
SELECT 
    c.id,
    c.conversation_id,
    c.conversation_title,
    c.is_group_chat,
    c.first_message_at,
    c.last_message_at,
    COUNT(DISTINCT cp.participant_id) AS participant_count,
    COUNT(DISTINCT m.id) AS message_count
FROM conversations c
LEFT JOIN conversation_participants cp ON c.id = cp.conversation_id
LEFT JOIN messages m ON c.id = m.conversation_id
GROUP BY c.id, c.conversation_id, c.conversation_title, c.is_group_chat, 
         c.first_message_at, c.last_message_at;

-- View: Latest ingestion run stats
CREATE VIEW IF NOT EXISTS vw_latest_ingestion_stats AS
SELECT 
    run_id,
    started_at,
    completed_at,
    status,
    total_messages_found,
    total_messages_inserted,
    total_conversations_found,
    total_conversations_inserted,
    total_participants_found,
    total_participants_inserted,
    ROUND((JULIANDAY(completed_at) - JULIANDAY(started_at)) * 86400, 2) AS duration_seconds
FROM ingestion_runs
ORDER BY started_at DESC
LIMIT 1;

-- ============================================================================
-- TRIGGERS FOR AUTOMATIC TIMESTAMP UPDATES
-- ============================================================================

-- Update participants.updated_at on row update
CREATE TRIGGER IF NOT EXISTS trg_participants_updated_at
AFTER UPDATE ON participants
FOR EACH ROW
BEGIN
    UPDATE participants SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Update conversations.updated_at on row update
CREATE TRIGGER IF NOT EXISTS trg_conversations_updated_at
AFTER UPDATE ON conversations
FOR EACH ROW
BEGIN
    UPDATE conversations SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Update messages.updated_at on row update
CREATE TRIGGER IF NOT EXISTS trg_messages_updated_at
AFTER UPDATE ON messages
FOR EACH ROW
BEGIN
    UPDATE messages SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Update conversation last_message_at when new message inserted
CREATE TRIGGER IF NOT EXISTS trg_update_conversation_last_message
AFTER INSERT ON messages
FOR EACH ROW
BEGIN
    UPDATE conversations 
    SET last_message_at = NEW.sent_at,
        first_message_at = COALESCE(first_message_at, NEW.sent_at)
    WHERE id = NEW.conversation_id;
END;

-- ============================================================================
-- SCHEMA VERSION TRACKING
-- ============================================================================

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);

-- Insert initial version
INSERT OR IGNORE INTO schema_version (version, description) 
VALUES (1, 'Initial schema: participants, conversations, messages, tracking tables');