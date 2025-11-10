# app/services/normalize.py
"""
Normalization Service
Converts raw connector output → normalized DB-ready rows
Handles data cleaning, deduplication, validation, and transformation

Input: Raw dict from connector (data_export.py)
Output: Clean, validated data ready for repo.py insert operations
"""

from typing import Dict, Any, List, Optional, Tuple, Set
from datetime import datetime
import logging
import re
from collections import defaultdict

logger = logging.getLogger(__name__)


class NormalizationService:
    """
    Transforms raw LinkedIn export data into normalized database rows
    Single responsibility: Data transformation and validation
    NO database operations (that's repo.py's job)
    """
    
    def __init__(self):
        """Initialize normalization service"""
        self.stats = {
            'participants_processed': 0,
            'participants_skipped': 0,
            'conversations_processed': 0,
            'conversations_skipped': 0,
            'messages_processed': 0,
            'messages_skipped': 0,
            'validation_errors': 0
        }
    
    def normalize_raw_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main entry point: normalize all raw data
        
        Args:
            raw_data: Dict from connector with keys:
                - participants: List[Dict]
                - conversations: List[Dict]
                - messages: List[Dict]
                - metadata: Dict
        
        Returns:
            Dict with normalized data:
            {
                'participants': List[Dict],  # Clean, deduplicated
                'conversations': List[Dict],  # With proper flags
                'messages': List[Dict],       # Validated, cleaned
                'conversation_participants': List[Dict],  # Junction table data
                'metadata': Dict,
                'stats': Dict
            }
        """
        logger.info("Starting normalization process...")
        
        # Reset stats
        self.stats = {
            'participants_processed': 0,
            'participants_skipped': 0,
            'conversations_processed': 0,
            'conversations_skipped': 0,
            'messages_processed': 0,
            'messages_skipped': 0,
            'validation_errors': 0
        }
        
        # Step 1: Normalize and deduplicate participants
        normalized_participants = self._normalize_participants(
            raw_data.get('participants', [])
        )
        
        # Step 2: Normalize conversations
        normalized_conversations = self._normalize_conversations(
            raw_data.get('conversations', [])
        )
        
        # Step 3: Normalize messages (depends on participants and conversations)
        normalized_messages = self._normalize_messages(
            raw_data.get('messages', []),
            normalized_participants,
            normalized_conversations
        )
        
        # Step 4: Build conversation-participant junction table data
        conversation_participants = self._build_conversation_participants(
            normalized_conversations,
            normalized_participants
        )
        
        logger.info(f"Normalization complete. Stats: {self.stats}")
        
        return {
            'participants': normalized_participants,
            'conversations': normalized_conversations,
            'messages': normalized_messages,
            'conversation_participants': conversation_participants,
            'metadata': raw_data.get('metadata', {}),
            'stats': self.stats.copy()
        }
    
    def _normalize_participants(self, raw_participants: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Clean and deduplicate participants
        
        Returns:
            List of normalized participant dicts ready for DB insertion
        """
        logger.info(f"Normalizing {len(raw_participants)} participants...")
        
        # Use linkedin_id as deduplication key
        participants_map: Dict[str, Dict[str, Any]] = {}
        
        for raw_p in raw_participants:
            try:
                linkedin_id = raw_p.get('linkedin_id', '').strip()
                full_name = raw_p.get('full_name', '').strip()
                
                # Validation
                if not linkedin_id:
                    logger.warning(f"Skipping participant with missing linkedin_id: {raw_p}")
                    self.stats['validation_errors'] += 1
                    self.stats['participants_skipped'] += 1
                    continue
                
                if not full_name:
                    logger.warning(f"Skipping participant with missing name: {linkedin_id}")
                    self.stats['validation_errors'] += 1
                    self.stats['participants_skipped'] += 1
                    continue
                
                # Clean and normalize
                normalized = {
                    'linkedin_id': linkedin_id,
                    'full_name': self._clean_name(full_name),
                    'profile_url': self._clean_url(raw_p.get('profile_url')),
                    'email': self._clean_email(raw_p.get('email')),
                    'headline': self._clean_text(raw_p.get('headline'))
                }
                
                # Deduplication: merge data if linkedin_id already exists
                if linkedin_id in participants_map:
                    existing = participants_map[linkedin_id]
                    # Keep non-null values (prefer new data if available)
                    normalized = {
                        'linkedin_id': linkedin_id,
                        'full_name': normalized['full_name'] or existing['full_name'],
                        'profile_url': normalized['profile_url'] or existing['profile_url'],
                        'email': normalized['email'] or existing['email'],
                        'headline': normalized['headline'] or existing['headline']
                    }
                    logger.debug(f"Merged duplicate participant: {linkedin_id}")
                
                participants_map[linkedin_id] = normalized
                self.stats['participants_processed'] += 1
            
            except Exception as e:
                logger.error(f"Error normalizing participant: {raw_p}. Error: {str(e)}")
                self.stats['validation_errors'] += 1
                self.stats['participants_skipped'] += 1
                continue
        
        result = list(participants_map.values())
        logger.info(f"Normalized {len(result)} unique participants "
                   f"(processed: {self.stats['participants_processed']}, "
                   f"skipped: {self.stats['participants_skipped']})")
        return result
    
    def _normalize_conversations(self, raw_conversations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Clean and validate conversations
        
        Returns:
            List of normalized conversation dicts
        """
        logger.info(f"Normalizing {len(raw_conversations)} conversations...")
        
        conversations_map: Dict[str, Dict[str, Any]] = {}
        
        for raw_c in raw_conversations:
            try:
                conversation_id = raw_c.get('conversation_id', '').strip()
                
                if not conversation_id:
                    logger.warning(f"Skipping conversation with no ID: {raw_c}")
                    self.stats['validation_errors'] += 1
                    self.stats['conversations_skipped'] += 1
                    continue
                
                # Validate participant IDs
                participant_ids = raw_c.get('participant_linkedin_ids', [])
                if not isinstance(participant_ids, list):
                    logger.warning(f"Invalid participant_ids for conversation {conversation_id}: {participant_ids}")
                    participant_ids = []
                
                # Clean participant IDs
                participant_ids = [str(pid).strip() for pid in participant_ids if pid]
                
                if not participant_ids:
                    logger.warning(f"Skipping conversation with no participants: {conversation_id}")
                    self.stats['validation_errors'] += 1
                    self.stats['conversations_skipped'] += 1
                    continue
                
                # Determine if group chat (more than 2 unique participants)
                unique_participants = set(participant_ids)
                is_group_chat = len(unique_participants) > 2
                
                # Get conversation title
                conversation_title = raw_c.get('conversation_title')
                
                # For 1:1 chats without title, we can derive it from participant names later
                # For now, just clean what we have
                conversation_title = self._clean_text(conversation_title)
                
                normalized = {
                    'conversation_id': conversation_id,
                    'conversation_title': conversation_title,
                    'is_group_chat': is_group_chat,
                    'participant_linkedin_ids': list(unique_participants),  # Keep for junction table
                    'first_message_at': None,  # Will be computed from messages
                    'last_message_at': None    # Will be computed from messages
                }
                
                # Deduplication
                if conversation_id in conversations_map:
                    existing = conversations_map[conversation_id]
                    # Merge participant lists
                    all_participants = set(existing['participant_linkedin_ids']) | unique_participants
                    normalized['participant_linkedin_ids'] = list(all_participants)
                    normalized['is_group_chat'] = len(all_participants) > 2
                    # Keep existing title if new one is None
                    normalized['conversation_title'] = conversation_title or existing['conversation_title']
                    logger.debug(f"Merged duplicate conversation: {conversation_id}")
                
                conversations_map[conversation_id] = normalized
                self.stats['conversations_processed'] += 1
            
            except Exception as e:
                logger.error(f"Error normalizing conversation: {raw_c}. Error: {str(e)}")
                self.stats['validation_errors'] += 1
                self.stats['conversations_skipped'] += 1
                continue
        
        result = list(conversations_map.values())
        logger.info(f"Normalized {len(result)} unique conversations "
                   f"(processed: {self.stats['conversations_processed']}, "
                   f"skipped: {self.stats['conversations_skipped']})")
        return result
    
    def _normalize_messages(
        self,
        raw_messages: List[Dict[str, Any]],
        normalized_participants: List[Dict[str, Any]],
        normalized_conversations: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Clean, validate, and enrich messages
        
        Args:
            raw_messages: Raw message list from connector
            normalized_participants: Already normalized participants
            normalized_conversations: Already normalized conversations
        
        Returns:
            List of normalized message dicts
        """
        logger.info(f"Normalizing {len(raw_messages)} messages...")
        
        # Build lookup sets for validation
        valid_participant_ids = {p['linkedin_id'] for p in normalized_participants}
        valid_conversation_ids = {c['conversation_id'] for c in normalized_conversations}
        
        # Track conversation timestamps for updating
        conversation_timestamps: Dict[str, List[datetime]] = defaultdict(list)
        
        messages_map: Dict[str, Dict[str, Any]] = {}
        
        for raw_m in raw_messages:
            try:
                message_id = raw_m.get('message_id', '').strip()
                conversation_id = raw_m.get('conversation_id', '').strip()
                sender_linkedin_id = raw_m.get('sender_linkedin_id', '').strip()
                content = raw_m.get('content', '')
                sent_at = raw_m.get('sent_at')
                
                # Validation: message_id
                if not message_id:
                    logger.warning(f"Skipping message with no message_id")
                    self.stats['validation_errors'] += 1
                    self.stats['messages_skipped'] += 1
                    continue
                
                # Validation: conversation_id
                if not conversation_id:
                    logger.warning(f"Skipping message {message_id} with no conversation_id")
                    self.stats['validation_errors'] += 1
                    self.stats['messages_skipped'] += 1
                    continue
                
                if conversation_id not in valid_conversation_ids:
                    logger.warning(f"Skipping message {message_id} with invalid conversation_id: {conversation_id}")
                    self.stats['validation_errors'] += 1
                    self.stats['messages_skipped'] += 1
                    continue
                
                # Validation: sender_linkedin_id
                if not sender_linkedin_id:
                    logger.warning(f"Skipping message {message_id} with no sender")
                    self.stats['validation_errors'] += 1
                    self.stats['messages_skipped'] += 1
                    continue
                
                if sender_linkedin_id not in valid_participant_ids:
                    logger.warning(f"Skipping message {message_id} with invalid sender: {sender_linkedin_id}")
                    self.stats['validation_errors'] += 1
                    self.stats['messages_skipped'] += 1
                    continue
                
                # Validation: sent_at
                if not isinstance(sent_at, datetime):
                    logger.warning(f"Skipping message {message_id} with invalid sent_at: {sent_at}")
                    self.stats['validation_errors'] += 1
                    self.stats['messages_skipped'] += 1
                    continue
                
                # Normalize content (can be empty)
                normalized_content = self._clean_message_content(content) if content else None
                
                # Build normalized message
                normalized = {
                    'message_id': message_id,
                    'conversation_id': conversation_id,
                    'sender_linkedin_id': sender_linkedin_id,
                    'content': normalized_content,
                    'sent_at': sent_at,
                    'attachments': raw_m.get('attachments', [])  # Pass through for now
                }
                
                # Track timestamps for conversation
                conversation_timestamps[conversation_id].append(sent_at)
                
                # Deduplication
                if message_id in messages_map:
                    logger.debug(f"Duplicate message_id found: {message_id}, keeping first occurrence")
                else:
                    messages_map[message_id] = normalized
                    self.stats['messages_processed'] += 1
            
            except Exception as e:
                logger.error(f"Error normalizing message: {raw_m}. Error: {str(e)}")
                self.stats['validation_errors'] += 1
                self.stats['messages_skipped'] += 1
                continue
        
        # Update conversation timestamps
        self._update_conversation_timestamps(
            normalized_conversations,
            conversation_timestamps
        )
        
        result = list(messages_map.values())
        logger.info(f"Normalized {len(result)} unique messages "
                   f"(processed: {self.stats['messages_processed']}, "
                   f"skipped: {self.stats['messages_skipped']})")
        return result
    
    def _build_conversation_participants(
        self,
        normalized_conversations: List[Dict[str, Any]],
        normalized_participants: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Build junction table data linking conversations to participants
        
        Returns:
            List of dicts for conversation_participants table
        """
        logger.info("Building conversation-participant junction data...")
        
        valid_participant_ids = {p['linkedin_id'] for p in normalized_participants}
        junction_data = []
        
        for conv in normalized_conversations:
            conversation_id = conv['conversation_id']
            participant_ids = conv.get('participant_linkedin_ids', [])
            
            for participant_id in participant_ids:
                # Validate participant exists
                if participant_id not in valid_participant_ids:
                    logger.warning(f"Skipping invalid participant {participant_id} "
                                 f"in conversation {conversation_id}")
                    continue
                
                junction_data.append({
                    'conversation_id': conversation_id,
                    'participant_linkedin_id': participant_id,
                    'joined_at': None,  # Could set to first message timestamp
                    'left_at': None
                })
        
        logger.info(f"Built {len(junction_data)} conversation-participant links")
        return junction_data
    
    def _update_conversation_timestamps(
        self,
        conversations: List[Dict[str, Any]],
        timestamps_map: Dict[str, List[datetime]]
    ):
        """
        Update conversation first/last message timestamps based on actual messages
        Mutates conversations list in-place
        
        Args:
            conversations: List of normalized conversations
            timestamps_map: Map of conversation_id -> list of message timestamps
        """
        for conv in conversations:
            conv_id = conv['conversation_id']
            timestamps = timestamps_map.get(conv_id, [])
            
            if timestamps:
                conv['first_message_at'] = min(timestamps)
                conv['last_message_at'] = max(timestamps)
                logger.debug(f"Updated timestamps for conversation {conv_id}: "
                           f"{len(timestamps)} messages")
    
    # ========================================================================
    # CLEANING/VALIDATION HELPERS
    # ========================================================================
    
    def _clean_name(self, name: Optional[str]) -> Optional[str]:
        """Clean and normalize person names"""
        if not name:
            return None
        
        # Remove extra whitespace
        name = ' '.join(name.split())
        
        # Remove special characters (keep letters, spaces, hyphens, apostrophes, periods)
        name = re.sub(r"[^\w\s\-'.]", '', name)
        
        # Title case (handles names like "McDonald" correctly)
        name = name.title()
        
        # Remove trailing/leading dots
        name = name.strip('.')
        
        return name if name else None
    
    def _clean_text(self, text: Optional[str]) -> Optional[str]:
        """Clean general text fields"""
        if not text:
            return None
        
        # Strip whitespace
        text = text.strip()
        
        # Remove null bytes and control characters
        text = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]', '', text)
        
        # Normalize whitespace
        text = ' '.join(text.split())
        
        return text if text else None
    
    def _clean_message_content(self, content: Optional[str]) -> Optional[str]:
        """Clean message content while preserving formatting"""
        if not content:
            return None
        
        # Remove null bytes and control characters (except newlines, tabs)
        content = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]', '', content)
        
        # Normalize excessive newlines (max 2 consecutive)
        content = re.sub(r'\n{3,}', '\n\n', content)
        
        # Normalize excessive spaces (but preserve single spaces)
        content = re.sub(r' {2,}', ' ', content)
        
        # Strip leading/trailing whitespace
        content = content.strip()
        
        return content if content else None
    
    def _clean_url(self, url: Optional[str]) -> Optional[str]:
        """Validate and clean URLs"""
        if not url:
            return None
        
        url = url.strip()
        
        # Basic URL validation pattern
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain
            r'localhost|'  # localhost
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # or IP
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE
        )
        
        if url_pattern.match(url):
            return url
        
        # Try adding https:// if missing
        if not url.startswith(('http://', 'https://')):
            test_url = f"https://{url}"
            if url_pattern.match(test_url):
                return test_url
        
        logger.debug(f"Invalid URL: {url}")
        return None
    
    def _clean_email(self, email: Optional[str]) -> Optional[str]:
        """Validate and clean email addresses"""
        if not email:
            return None
        
        email = email.strip().lower()
        
        # Basic email validation
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        
        if email_pattern.match(email):
            return email
        
        logger.debug(f"Invalid email: {email}")
        return None
    
    def get_stats(self) -> Dict[str, int]:
        """Get normalization statistics"""
        return self.stats.copy()
    
    def reset_stats(self):
        """Reset statistics counters"""
        self.stats = {
            'participants_processed': 0,
            'participants_skipped': 0,
            'conversations_processed': 0,
            'conversations_skipped': 0,
            'messages_processed': 0,
            'messages_skipped': 0,
            'validation_errors': 0
        }