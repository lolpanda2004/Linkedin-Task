"""
app/services/normalize.py

Normalizes raw extracted data into clean, standardized database records.
Handles data cleaning, type conversion, deduplication, and validation.
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import hashlib
import re
from collections import defaultdict

logger = logging.getLogger(__name__)


class NormalizeService:
    """Service for normalizing raw data into database-ready records."""
    
    def __init__(self):
        """Initialize normalization service."""
        self.date_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S UTC",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%d",
            "%d %b %Y",
            "%b %d, %Y",
            "%m/%d/%Y",
            "%d/%m/%Y",
        ]
        
        self.stats = {
            'participants_processed': 0,
            'participants_skipped': 0,
            'conversations_processed': 0,
            'conversations_skipped': 0,
            'messages_processed': 0,
            'messages_skipped': 0,
            'validation_errors': 0
        }
    
    def normalize_all(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize all tables in raw data.
        
        Args:
            raw_data: Dictionary with either:
                - Table-based structure: {'participants': [...], 'conversations': [...]}
                - Or nested structure from connector with metadata
            
        Returns:
            Dictionary with normalized data including participants, conversations,
            messages, conversation_participants junction table, metadata, and stats
        """
        logger.info("Starting normalization of all tables...")
        
        # Reset stats
        self._reset_stats()
        
        # Check if this is connector format (has metadata) or table format
        is_connector_format = 'metadata' in raw_data or all(
            key in raw_data for key in ['participants', 'conversations', 'messages']
        )
        
        if is_connector_format:
            return self._normalize_connector_format(raw_data)
        else:
            return self._normalize_table_format(raw_data)
    
    def _normalize_connector_format(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize data from connector format (LinkedIn export).
        
        Returns normalized data with junction table and stats.
        """
        # Step 1: Normalize and deduplicate participants
        normalized_participants = self.normalize_participants(
            raw_data.get('participants', [])
        )
        
        # Step 2: Normalize conversations
        normalized_conversations = self.normalize_conversations(
            raw_data.get('conversations', [])
        )
        
        # Step 3: Normalize messages (depends on participants and conversations)
        normalized_messages = self.normalize_messages(
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
            'connections': raw_data.get('connections', []),
            'profile': raw_data.get('profile', []),
            'reactions': raw_data.get('reactions', []),
            'metadata': raw_data.get('metadata', {}),
            'stats': self.stats.copy()
        }
    
    def _normalize_table_format(self, raw_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Normalize data in table format (generic multi-table structure).
        
        Returns normalized tables without junction table or stats.
        """
        normalized_data = {}
        
        for table_name, raw_records in raw_data.items():
            logger.debug(f"Normalizing table: {table_name} ({len(raw_records)} records)")
            
            # Route to appropriate normalizer
            if table_name == 'participants':
                normalized_data[table_name] = self.normalize_participants(raw_records)
            elif table_name == 'conversations':
                normalized_data[table_name] = self.normalize_conversations(raw_records)
            elif table_name == 'messages':
                normalized_data[table_name] = self.normalize_messages(raw_records)
            elif table_name == 'connections':
                normalized_data[table_name] = self.normalize_connections(raw_records)
            elif table_name == 'profile':
                normalized_data[table_name] = self.normalize_profile(raw_records)
            elif table_name == 'reactions':
                normalized_data[table_name] = self.normalize_reactions(raw_records)
            else:
                # Generic normalization for unknown tables
                normalized_data[table_name] = self.normalize_generic(raw_records)
            
            logger.info(f"Normalized {table_name}: {len(normalized_data[table_name])} records")
        
        return normalized_data
    
    def normalize_participants(
        self, 
        raw_records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Normalize participants table.
        
        Handles both LinkedIn export format (linkedin_id) and generic format (participant_id).
        Performs deduplication and validation.
        """
        logger.info(f"Normalizing {len(raw_records)} participants...")
        
        participants_map: Dict[str, Dict[str, Any]] = {}
        
        for raw_p in raw_records:
            try:
                # Support both linkedin_id and participant_id/id
                linkedin_id = (
                    raw_p.get('linkedin_id') or 
                    raw_p.get('participant_id') or 
                    raw_p.get('id')
                )
                
                if linkedin_id:
                    linkedin_id = str(linkedin_id).strip()
                
                full_name = self._get_field(raw_p, ['full_name', 'name', 'display_name'])
                
                # Validation
                if not linkedin_id:
                    logger.warning(f"Skipping participant with missing ID: {raw_p}")
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
                    'email': self._clean_email(raw_p.get('email') or raw_p.get('email_address')),
                    'headline': self._clean_text(raw_p.get('headline')),
                    'first_seen': self._parse_date(raw_p.get('first_seen') or raw_p.get('created_at')),
                    'created_at': datetime.utcnow().isoformat()
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
                        'headline': normalized['headline'] or existing['headline'],
                        'first_seen': normalized['first_seen'] or existing['first_seen'],
                        'created_at': existing['created_at']  # Keep original
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
    
    def normalize_conversations(
        self, 
        raw_records: List[Dict[str, Any]],
        standalone: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Normalize conversations table.
        
        Args:
            raw_records: List of raw conversation records
            standalone: If True, uses simple format. If False, uses LinkedIn export format.
        """
        logger.info(f"Normalizing {len(raw_records)} conversations...")
        
        conversations_map: Dict[str, Dict[str, Any]] = {}
        
        for raw_c in raw_records:
            try:
                conversation_id = str(self._get_field(raw_c, ['conversation_id', 'id']) or '').strip()
                
                if not conversation_id:
                    logger.warning(f"Skipping conversation with no ID: {raw_c}")
                    self.stats['validation_errors'] += 1
                    self.stats['conversations_skipped'] += 1
                    continue
                
                # Get participant IDs (handle both formats)
                participant_ids = raw_c.get('participant_linkedin_ids') or raw_c.get('participant_ids')
                
                if isinstance(participant_ids, str):
                    participant_ids = [p.strip() for p in participant_ids.split(',') if p.strip()]
                elif isinstance(participant_ids, list):
                    participant_ids = [str(p).strip() for p in participant_ids if p]
                else:
                    participant_ids = []
                
                if not participant_ids:
                    logger.warning(f"Skipping conversation with no participants: {conversation_id}")
                    self.stats['validation_errors'] += 1
                    self.stats['conversations_skipped'] += 1
                    continue
                
                # Determine if group chat
                unique_participants = list(set(participant_ids))
                is_group_chat = len(unique_participants) > 2
                
                # Get conversation title
                conversation_title = self._get_field(
                    raw_c, 
                    ['conversation_title', 'subject', 'title']
                )
                conversation_title = self._clean_text(conversation_title)
                
                # Get timestamps
                created_at = self._parse_date(
                    self._get_field(raw_c, ['created_at', 'first_message_at', 'date', 'start_date'])
                )
                
                normalized = {
                    'conversation_id': conversation_id,
                    'conversation_title': conversation_title,
                    'is_group_chat': is_group_chat,
                    'participant_linkedin_ids': unique_participants,
                    'first_message_at': created_at,
                    'last_message_at': self._parse_date(raw_c.get('last_message_at')),
                    'message_count': self._to_int(raw_c.get('message_count')) or 0,
                    'created_at': created_at or datetime.utcnow().isoformat(),
                    'updated_at': datetime.utcnow().isoformat()
                }
                
                # Deduplication
                if conversation_id in conversations_map:
                    existing = conversations_map[conversation_id]
                    # Merge participant lists
                    all_participants = list(set(existing['participant_linkedin_ids']) | set(unique_participants))
                    normalized['participant_linkedin_ids'] = all_participants
                    normalized['is_group_chat'] = len(all_participants) > 2
                    normalized['conversation_title'] = conversation_title or existing['conversation_title']
                    normalized['created_at'] = existing['created_at']  # Keep original
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
    
    def normalize_messages(
        self,
        raw_records: List[Dict[str, Any]],
        normalized_participants: Optional[List[Dict[str, Any]]] = None,
        normalized_conversations: Optional[List[Dict[str, Any]]] = None
    ) -> List[Dict[str, Any]]:
        """
        Normalize messages table.
        
        Args:
            raw_records: Raw message records
            normalized_participants: For validation (optional)
            normalized_conversations: For validation and timestamp updates (optional)
        """
        logger.info(f"Normalizing {len(raw_records)} messages...")
        
        # Build validation sets if provided
        valid_participant_ids = None
        valid_conversation_ids = None
        conversation_timestamps: Dict[str, List[datetime]] = defaultdict(list)
        
        if normalized_participants:
            valid_participant_ids = {p['linkedin_id'] for p in normalized_participants}
        
        if normalized_conversations:
            valid_conversation_ids = {c['conversation_id'] for c in normalized_conversations}
        
        messages_map: Dict[str, Dict[str, Any]] = {}
        
        for raw_m in raw_records:
            try:
                message_id = str(self._get_field(raw_m, ['message_id', 'id']) or '').strip()
                conversation_id = str(self._get_field(raw_m, ['conversation_id', 'thread_id']) or '').strip()
                sender_id = str(self._get_field(raw_m, ['sender_linkedin_id', 'sender_id', 'from_id']) or '').strip()
                sender_name = self._get_field(raw_m, ['sender_name', 'from', 'sender'])
                content = self._get_field(raw_m, ['content', 'message', 'body', 'text'])
                sent_at = raw_m.get('sent_at')
                folder = self._get_field(raw_m, ['folder'])
                
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
                
                if valid_conversation_ids and conversation_id not in valid_conversation_ids:
                    logger.warning(f"Skipping message {message_id} with invalid conversation_id: {conversation_id}")
                    self.stats['validation_errors'] += 1
                    self.stats['messages_skipped'] += 1
                    continue
                
                # Validation: sender
                if not sender_id:
                    logger.warning(f"Skipping message {message_id} with no sender")
                    self.stats['validation_errors'] += 1
                    self.stats['messages_skipped'] += 1
                    continue
                
                if valid_participant_ids and sender_id not in valid_participant_ids:
                    logger.warning(f"Skipping message {message_id} with invalid sender: {sender_id}")
                    self.stats['validation_errors'] += 1
                    self.stats['messages_skipped'] += 1
                    continue
                
                # Parse timestamp
                if isinstance(sent_at, datetime):
                    parsed_sent_at = sent_at
                else:
                    parsed_sent_at = self._parse_date_to_datetime(sent_at)
                    if not parsed_sent_at:
                        logger.warning(f"Skipping message {message_id} with invalid sent_at: {sent_at}")
                        self.stats['validation_errors'] += 1
                        self.stats['messages_skipped'] += 1
                        continue
                
                # Normalize content
                normalized_content = self._clean_message_content(content) if content else None
                
                normalized = {
                    'message_id': message_id,
                    'conversation_id': conversation_id,
                    'sender_linkedin_id': sender_id,
                    'sender_name': self._clean_name(sender_name),
                    'content': normalized_content,
                    'sent_at': parsed_sent_at if isinstance(parsed_sent_at, datetime) else parsed_sent_at,
                    'folder': self._clean_string(folder),
                    'attachments': raw_m.get('attachments', []),
                    'created_at': datetime.utcnow().isoformat()
                }
                
                # Track timestamps for conversation updates
                if normalized_conversations and isinstance(parsed_sent_at, datetime):
                    conversation_timestamps[conversation_id].append(parsed_sent_at)
                
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
        
        # Update conversation timestamps if applicable
        if normalized_conversations and conversation_timestamps:
            self._update_conversation_timestamps(
                normalized_conversations,
                conversation_timestamps
            )
        
        result = list(messages_map.values())
        logger.info(f"Normalized {len(result)} unique messages "
                   f"(processed: {self.stats['messages_processed']}, "
                   f"skipped: {self.stats['messages_skipped']})")
        return result
    
    def normalize_connections(self, raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normalize connections table (LinkedIn connections).
        """
        normalized = []
        seen = set()
        
        for idx, record in enumerate(raw_records, start=1):
            first_name = self._get_field(record, ['First Name', 'first_name', 'firstName'])
            last_name = self._get_field(record, ['Last Name', 'last_name', 'lastName'])
            email = self._get_field(record, ['Email Address', 'email', 'Email'])
            company = self._get_field(record, ['Company', 'company', 'organization'])
            position = self._get_field(record, ['Position', 'position', 'title'])
            connected_on = self._get_field(record, ['Connected On', 'connected_on', 'connection_date'])
            
            # Create unique key for deduplication
            full_name = f"{first_name} {last_name}".strip()
            dedup_key = f"{full_name}:{email}".lower()
            
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            
            normalized.append({
                'connection_id': idx,
                'first_name': self._clean_string(first_name),
                'last_name': self._clean_string(last_name),
                'full_name': self._clean_string(full_name),
                'email': self._clean_email(email),
                'company': self._clean_string(company),
                'position': self._clean_string(position),
                'connected_on': self._parse_date(connected_on),
                'created_at': datetime.utcnow().isoformat()
            })
        
        return normalized
    
    def normalize_profile(self, raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normalize profile table (LinkedIn profile - usually single record).
        """
        if not raw_records:
            return []
        
        record = raw_records[0]
        
        return [{
            'profile_id': 1,
            'first_name': self._clean_string(self._get_field(record, ['First Name', 'first_name'])),
            'last_name': self._clean_string(self._get_field(record, ['Last Name', 'last_name'])),
            'headline': self._clean_text(self._get_field(record, ['Headline', 'headline'])),
            'summary': self._clean_text(self._get_field(record, ['Summary', 'summary', 'about'])),
            'industry': self._clean_string(self._get_field(record, ['Industry', 'industry'])),
            'location': self._clean_string(self._get_field(record, ['Location', 'location', 'geo_location'])),
            'created_at': datetime.utcnow().isoformat()
        }]
    
    def normalize_reactions(self, raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normalize reactions table (LinkedIn reactions).
        """
        normalized = []
        
        for idx, record in enumerate(raw_records, start=1):
            reaction_type = self._get_field(record, ['Type', 'reaction_type', 'type'])
            date = self._get_field(record, ['Date', 'date', 'created_at'])
            link = self._get_field(record, ['Link', 'url', 'link'])
            
            normalized.append({
                'reaction_id': idx,
                'reaction_type': self._clean_string(reaction_type),
                'reacted_at': self._parse_date(date),
                'link': self._clean_string(link),
                'created_at': datetime.utcnow().isoformat()
            })
        
        return normalized
    
    def normalize_generic(self, raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generic normalization for unknown table types.
        """
        normalized = []
        common_date_fields = ['date', 'created_at', 'updated_at', 'timestamp', 'time']
        
        for record in raw_records:
            clean_record = {}
            
            for key, value in record.items():
                if isinstance(value, str):
                    clean_value = self._clean_string(value)
                    
                    # Try to parse dates
                    if any(date_field in key.lower() for date_field in common_date_fields):
                        parsed_date = self._parse_date(value)
                        clean_record[key] = parsed_date if parsed_date else clean_value
                    else:
                        clean_record[key] = clean_value
                else:
                    clean_record[key] = value
            
            normalized.append(clean_record)
        
        return normalized
    
    def _build_conversation_participants(
        self,
        normalized_conversations: List[Dict[str, Any]],
        normalized_participants: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Build junction table data linking conversations to participants.
        """
        logger.info("Building conversation-participant junction data...")
        
        valid_participant_ids = {p['linkedin_id'] for p in normalized_participants}
        junction_data = []
        
        for conv in normalized_conversations:
            conversation_id = conv['conversation_id']
            participant_ids = conv.get('participant_linkedin_ids', [])
            
            for participant_id in participant_ids:
                if participant_id not in valid_participant_ids:
                    logger.warning(f"Skipping invalid participant {participant_id} "
                                 f"in conversation {conversation_id}")
                    continue
                
                junction_data.append({
                    'conversation_id': conversation_id,
                    'participant_linkedin_id': participant_id,
                    'joined_at': conv.get('first_message_at'),
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
        Update conversation first/last message timestamps.
        Mutates conversations list in-place.
        """
        for conv in conversations:
            conv_id = conv['conversation_id']
            timestamps = timestamps_map.get(conv_id, [])
            
            if timestamps:
                conv['first_message_at'] = min(timestamps)
                conv['last_message_at'] = max(timestamps)
                logger.debug(f"Updated timestamps for conversation {conv_id}: "
                           f"{len(timestamps)} messages")
    
    # ==================== UTILITY METHODS ====================
    
    def _get_field(self, record: Dict[str, Any], field_names: List[str]) -> Any:
        """Get field value by trying multiple possible field names."""
        for field_name in field_names:
            if field_name in record:
                return record[field_name]
        return None
    
    def _clean_string(self, value: Any) -> Optional[str]:
        """Clean and normalize string values."""
        if value is None:
            return None
        
        if not isinstance(value, str):
            value = str(value)
        
        value = value.strip()
        value = re.sub(r'\s+', ' ', value)
        
        return value if value else None
    
    def _clean_name(self, name: Optional[str]) -> Optional[str]:
        """Clean and normalize person names."""
        if not name:
            return None
        
        name = ' '.join(name.split())
        name = re.sub(r"[^\w\s\-'.]", '', name)
        name = name.title()
        name = name.strip('.')
        
        return name if name else None
    
    def _clean_text(self, text: Optional[str]) -> Optional[str]:
        """Clean general text fields."""
        if not text:
            return None
        
        text = text.strip()
        text = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]', '', text)
        text = ' '.join(text.split())
        
        return text if text else None
    
    def _clean_message_content(self, content: Optional[str]) -> Optional[str]:
        """Clean message content while preserving formatting."""
        if not content:
            return None
        
        content = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]', '', content)
        content = re.sub(r'\n{3,}', '\n\n', content)
        content = re.sub(r' {2,}', ' ', content)
        content = content.strip()
        
        return content if content else None
    
    def _clean_url(self, url: Optional[str]) -> Optional[str]:
        """Validate and clean URLs."""
        if not url:
            return None
        
        url = url.strip()
        
        url_pattern = re.compile(
            r'^https?://'
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'
            r'localhost|'
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
            r'(?::\d+)?'
            r'(?:/?|[/?]\S+)$', re.IGNORECASE
        )
        
        if url_pattern.match(url):
            return url
        
        if not url.startswith(('http://', 'https://')):
            test_url = f"https://{url}"
            if url_pattern.match(test_url):
                return test_url
        
        logger.debug(f"Invalid URL: {url}")
        return None
    
    def _clean_email(self, email: Any) -> Optional[str]:
        """Clean and validate email addresses."""
        email = self._clean_string(email)
        
        if not email:
            return None
        
        email = email.lower()
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        
        if email_pattern.match(email):
            return email
        
        logger.debug(f"Invalid email: {email}")
        return None
    
    def _to_int(self, value: Any) -> Optional[int]:
        """Convert value to integer."""
        if value is None:
            return None
        
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    
    def _parse_date(self, value: Any) -> Optional[str]:
        """
        Parse date string into ISO format string.
        """
        if value is None:
            return None
        
        if isinstance(value, datetime):
            return value.isoformat()
        
        if not isinstance(value, str):
            value = str(value)
        
        value = value.strip()
        
        if not value:
            return None
        
        for date_format in self.date_formats:
            try:
                dt = datetime.strptime(value, date_format)
                return dt.isoformat()
            except ValueError:
                continue
        
        logger.debug(f"Could not parse date: {value}")
        return value
    
    def _parse_date_to_datetime(self, value: Any) -> Optional[datetime]:
        """
        Parse date string into datetime object.
        """
        if value is None:
            return None
        
        if isinstance(value, datetime):
            return value
        
        if not isinstance(value, str):
            value = str(value)
        
        value = value.strip()
        
        if not value:
            return None
        
        for date_format in self.date_formats:
            try:
                return datetime.strptime(value, date_format)
            except ValueError:
                continue
        
        logger.debug(f"Could not parse date to datetime: {value}")
        return None
    
    def _generate_hash(self, *values) -> str:
        """Generate MD5 hash from values for deduplication."""
        combined = '|'.join(str(v) for v in values if v is not None)
        return hashlib.md5(combined.encode()).hexdigest()
    
    def get_stats(self) -> Dict[str, int]:
        """Get normalization statistics."""
        return self.stats.copy()
    
    def _reset_stats(self):
        """Reset statistics counters."""
        self.stats = {
            'participants_processed': 0,
            'participants_skipped': 0,
            'conversations_processed': 0,
            'conversations_skipped': 0,
            'messages_processed': 0,
            'messages_skipped': 0,
            'validation_errors': 0
        }


# Singleton instance
normalize_service = NormalizeService()