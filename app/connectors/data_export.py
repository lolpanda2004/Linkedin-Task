"""
app/connectors/data_export.py

Connector for parsing LinkedIn "Download your data" export ZIP files.
Extracts and parses CSV files from the export into standardized dictionaries.
"""

import csv
import zipfile
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging
import io

from app.connectors.base import BaseConnector

logger = logging.getLogger(__name__)


class DataExportConnector(BaseConnector):
    """
    Connector for LinkedIn data export ZIP files.
    
    Handles extraction and parsing of CSV/JSON files from LinkedIn's
    "Download your data" export format.
    """
    
    # Expected file mappings in LinkedIn export
    FILE_MAPPINGS = {
        'messages': 'messages.csv',
        'connections': 'Connections.csv',
        'profile': 'Profile.csv',
        'reactions': 'Reactions.csv',
        'invitations': 'Invitations.csv',
        'contacts': 'Contacts.csv',
        'registrations': 'Registration.csv',
    }
    
    def __init__(self):
        """Initialize the data export connector."""
        self.temp_extract_dir = None
    
    def extract(self, source_path: Path) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract and parse LinkedIn export ZIP file.
        
        Args:
            source_path: Path to LinkedIn export ZIP file
            
        Returns:
            Dictionary mapping table names to lists of raw record dictionaries
            
        Raises:
            FileNotFoundError: If ZIP file doesn't exist
            zipfile.BadZipFile: If file is not a valid ZIP
            ValueError: If ZIP doesn't contain expected LinkedIn data
        """
        if not source_path.exists():
            raise FileNotFoundError(f"ZIP file not found: {source_path}")
        
        logger.info(f"Extracting LinkedIn export from: {source_path.name}")
        
        raw_data = {}
        
        try:
            with zipfile.ZipFile(source_path, 'r') as zip_ref:
                # List all files in ZIP for debugging
                file_list = zip_ref.namelist()
                logger.debug(f"Files in ZIP: {len(file_list)}")
                
                # Extract each expected file type
                for data_type, filename in self.FILE_MAPPINGS.items():
                    records = self._extract_file_from_zip(zip_ref, filename, file_list)
                    if records:
                        raw_data[data_type] = records
                        logger.info(f"Extracted {len(records)} records from {filename}")
                    else:
                        logger.warning(f"No data found for {filename}")
                
                # Special handling for messages (may have conversation structure)
                if 'messages' in raw_data:
                    raw_data = self._parse_messages(raw_data)
                
                if not raw_data:
                    raise ValueError("No valid LinkedIn data found in ZIP file")
                
                logger.info(f"Successfully extracted {sum(len(r) for r in raw_data.values())} total records")
                
                return raw_data
                
        except zipfile.BadZipFile as e:
            logger.error(f"Invalid ZIP file: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to extract LinkedIn export: {e}", exc_info=True)
            raise
    
    def _extract_file_from_zip(
        self,
        zip_ref: zipfile.ZipFile,
        target_filename: str,
        file_list: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Extract and parse a specific file from the ZIP.
        
        Args:
            zip_ref: Open ZipFile object
            target_filename: Name of file to extract (e.g., 'messages.csv')
            file_list: List of all files in ZIP
            
        Returns:
            List of record dictionaries
        """
        # Find file in ZIP (case-insensitive, handles nested directories)
        matching_file = None
        for file_path in file_list:
            file_name = Path(file_path).name
            if file_name.lower() == target_filename.lower():
                matching_file = file_path
                break
        
        if not matching_file:
            logger.debug(f"File not found in ZIP: {target_filename}")
            return []
        
        try:
            # Read file content
            with zip_ref.open(matching_file) as file:
                content = file.read()
                
                # Determine file type and parse accordingly
                if matching_file.lower().endswith('.csv'):
                    return self._parse_csv(content)
                elif matching_file.lower().endswith('.json'):
                    return self._parse_json(content)
                else:
                    logger.warning(f"Unknown file type: {matching_file}")
                    return []
                    
        except Exception as e:
            logger.error(f"Failed to parse {matching_file}: {e}")
            return []
    
    def _parse_csv(self, content: bytes) -> List[Dict[str, Any]]:
        """
        Parse CSV content into list of dictionaries.
        
        Args:
            content: Raw CSV file content as bytes
            
        Returns:
            List of record dictionaries
        """
        try:
            # Decode content (try UTF-8, fallback to latin-1)
            try:
                text_content = content.decode('utf-8')
            except UnicodeDecodeError:
                text_content = content.decode('latin-1')
            
            # Parse CSV
            csv_file = io.StringIO(text_content)
            reader = csv.DictReader(csv_file)
            
            records = []
            for row in reader:
                # Clean up row: strip whitespace from keys and values
                cleaned_row = {
                    key.strip(): value.strip() if isinstance(value, str) else value
                    for key, value in row.items()
                }
                records.append(cleaned_row)
            
            return records
            
        except Exception as e:
            logger.error(f"Failed to parse CSV: {e}")
            return []
    
    def _parse_json(self, content: bytes) -> List[Dict[str, Any]]:
        """
        Parse JSON content into list of dictionaries.
        
        Args:
            content: Raw JSON file content as bytes
            
        Returns:
            List of record dictionaries
        """
        try:
            text_content = content.decode('utf-8')
            data = json.loads(text_content)
            
            # Handle different JSON structures
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                # If it's a dict with a list inside, try to find it
                for value in data.values():
                    if isinstance(value, list):
                        return value
                # If it's a single record, wrap in list
                return [data]
            else:
                logger.warning(f"Unexpected JSON structure: {type(data)}")
                return []
                
        except Exception as e:
            logger.error(f"Failed to parse JSON: {e}")
            return []
    
    def _parse_messages(self, raw_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Parse messages into conversations, participants, and messages tables.
        
        LinkedIn messages.csv typically has structure:
        - FROM (sender name/email)
        - TO (recipient names)
        - DATE
        - SUBJECT (conversation subject)
        - CONTENT (message content)
        - FOLDER (INBOX/SENT)
        
        This transforms it into:
        - participants: unique people in conversations
        - conversations: conversation threads
        - messages: individual messages
        
        Args:
            raw_data: Dictionary with 'messages' key containing raw message records
            
        Returns:
            Updated raw_data with participants, conversations, and messages
        """
        if 'messages' not in raw_data:
            return raw_data
        
        messages = raw_data['messages']
        
        # Track unique participants and conversations
        participants_map = {}  # email/name -> participant dict
        conversations_map = {}  # conversation_id -> conversation dict
        parsed_messages = []
        
        participant_id_counter = 1
        conversation_id_counter = 1
        message_id_counter = 1
        
        for msg in messages:
            # Extract fields (LinkedIn CSV field names may vary)
            from_field = msg.get('FROM', msg.get('From', msg.get('SENDER', '')))
            to_field = msg.get('TO', msg.get('To', msg.get('RECIPIENTS', '')))
            date_field = msg.get('DATE', msg.get('Date', msg.get('SENT AT', '')))
            subject_field = msg.get('SUBJECT', msg.get('Subject', msg.get('CONVERSATION TITLE', '')))
            content_field = msg.get('CONTENT', msg.get('Content', msg.get('MESSAGE', '')))
            folder = msg.get('FOLDER', msg.get('Folder', 'UNKNOWN'))
            
            # Parse participants
            sender = self._extract_participant(from_field)
            recipients = self._extract_recipients(to_field)
            
            # Add sender to participants
            if sender and sender not in participants_map:
                participants_map[sender] = {
                    'participant_id': participant_id_counter,
                    'name': sender,
                    'email': self._extract_email(sender),
                    'first_seen': date_field
                }
                participant_id_counter += 1
            
            # Add recipients to participants
            for recipient in recipients:
                if recipient and recipient not in participants_map:
                    participants_map[recipient] = {
                        'participant_id': participant_id_counter,
                        'name': recipient,
                        'email': self._extract_email(recipient),
                        'first_seen': date_field
                    }
                    participant_id_counter += 1
            
            # Create conversation identifier (based on subject + participants)
            all_participants = sorted([sender] + recipients)
            conversation_key = f"{subject_field}:{':'.join(all_participants)}"
            
            if conversation_key not in conversations_map:
                conversations_map[conversation_key] = {
                    'conversation_id': conversation_id_counter,
                    'subject': subject_field or '(No Subject)',
                    'participant_ids': [participants_map[p]['participant_id'] for p in all_participants if p in participants_map],
                    'created_at': date_field,
                    'message_count': 0
                }
                conversation_id_counter += 1
            
            # Increment message count
            conversations_map[conversation_key]['message_count'] += 1
            
            # Create message record
            parsed_messages.append({
                'message_id': message_id_counter,
                'conversation_id': conversations_map[conversation_key]['conversation_id'],
                'sender_id': participants_map.get(sender, {}).get('participant_id'),
                'sender_name': sender,
                'content': content_field,
                'sent_at': date_field,
                'folder': folder
            })
            message_id_counter += 1
        
        # Update raw_data with parsed structures
        raw_data['participants'] = list(participants_map.values())
        raw_data['conversations'] = list(conversations_map.values())
        raw_data['messages'] = parsed_messages
        
        logger.info(f"Parsed messages into {len(participants_map)} participants, "
                   f"{len(conversations_map)} conversations, "
                   f"{len(parsed_messages)} messages")
        
        return raw_data
    
    def _extract_participant(self, participant_field: str) -> str:
        """
        Extract participant name/identifier from field.
        
        Args:
            participant_field: Raw participant string
            
        Returns:
            Clean participant identifier
        """
        if not participant_field:
            return ""
        
        # Remove email brackets if present: "John Doe <john@example.com>" -> "John Doe"
        if '<' in participant_field and '>' in participant_field:
            return participant_field.split('<')[0].strip()
        
        return participant_field.strip()
    
    def _extract_recipients(self, recipients_field: str) -> List[str]:
        """
        Extract list of recipients from field.
        
        Recipients may be comma-separated or semicolon-separated.
        
        Args:
            recipients_field: Raw recipients string
            
        Returns:
            List of recipient identifiers
        """
        if not recipients_field:
            return []
        
        # Split by comma or semicolon
        separators = [',', ';']
        for sep in separators:
            if sep in recipients_field:
                recipients = [self._extract_participant(r.strip()) for r in recipients_field.split(sep)]
                return [r for r in recipients if r]
        
        # Single recipient
        return [self._extract_participant(recipients_field)]
    
    def _extract_email(self, participant_string: str) -> Optional[str]:
        """
        Extract email address from participant string.
        
        Args:
            participant_string: String that may contain email
            
        Returns:
            Email address if found, None otherwise
        """
        if not participant_string:
            return None
        
        # Check for email in brackets: "Name <email@example.com>"
        if '<' in participant_string and '>' in participant_string:
            email = participant_string.split('<')[1].split('>')[0].strip()
            return email if '@' in email else None
        
        # Check if the whole string is an email
        if '@' in participant_string and ' ' not in participant_string:
            return participant_string.strip()
        
        return None
    
    def validate(self, data: Dict[str, List[Dict[str, Any]]]) -> bool:
        """
        Validate extracted data structure.
        
        Args:
            data: Extracted data dictionary
            
        Returns:
            True if valid, False otherwise
        """
        if not data:
            logger.error("No data extracted")
            return False
        
        # Check for at least one expected table
        expected_tables = set(self.FILE_MAPPINGS.keys())
        actual_tables = set(data.keys())
        
        if not actual_tables.intersection(expected_tables):
            logger.error(f"No expected tables found. Got: {actual_tables}")
            return False
        
        # Validate each table has records
        for table_name, records in data.items():
            if not isinstance(records, list):
                logger.error(f"Table {table_name} is not a list")
                return False
            
            if not records:
                logger.warning(f"Table {table_name} is empty")
            
            # Validate first record structure
            if records and not isinstance(records[0], dict):
                logger.error(f"Table {table_name} records are not dictionaries")
                return False
        
        logger.info("Data validation passed")
        return True
    
    def get_metadata(self, source_path: Path) -> Dict[str, Any]:
        """
        Get metadata about the LinkedIn export without full extraction.
        
        Args:
            source_path: Path to LinkedIn export ZIP
            
        Returns:
            Metadata dictionary
        """
        metadata = {
            'file_name': source_path.name,
            'file_size_mb': source_path.stat().st_size / (1024 * 1024),
            'files_in_zip': [],
            'detected_tables': []
        }
        
        try:
            with zipfile.ZipFile(source_path, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                metadata['files_in_zip'] = file_list
                metadata['file_count'] = len(file_list)
                
                # Detect which expected files are present
                for data_type, filename in self.FILE_MAPPINGS.items():
                    for file_path in file_list:
                        if Path(file_path).name.lower() == filename.lower():
                            metadata['detected_tables'].append(data_type)
                            break
        
        except Exception as e:
            logger.error(f"Failed to get metadata: {e}")
            metadata['error'] = str(e)
        
        return metadata