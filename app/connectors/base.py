"""
app/connectors/base.py

Base connector interface for data sources.
Defines the contract that all data connectors must implement.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Any


class BaseConnector(ABC):
    """
    Abstract base class for data source connectors.
    
    All connectors must implement the extract() method which returns
    data in a standardized dictionary format.
    
    Standard output format:
    {
        "table_name": [
            {"field1": "value1", "field2": "value2", ...},
            {"field1": "value3", "field2": "value4", ...},
            ...
        ],
        ...
    }
    """
    
    @abstractmethod
    def extract(self, source_path: Path) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract data from source and return standardized dictionary.
        
        Args:
            source_path: Path to data source (file, directory, etc.)
            
        Returns:
            Dictionary mapping table names to lists of record dictionaries.
            Each record dictionary contains field_name -> value mappings.
            
        Raises:
            FileNotFoundError: If source doesn't exist
            ValueError: If source is invalid or malformed
            Exception: For other extraction errors
            
        Example return value:
            {
                "participants": [
                    {"participant_id": 1, "name": "John Doe", "email": "john@example.com"},
                    {"participant_id": 2, "name": "Jane Smith", "email": "jane@example.com"}
                ],
                "messages": [
                    {
                        "message_id": 1,
                        "conversation_id": 1,
                        "sender_id": 1,
                        "content": "Hello!",
                        "sent_at": "2024-01-01T10:00:00Z"
                    }
                ]
            }
        """
        pass
    
    def validate(self, data: Dict[str, List[Dict[str, Any]]]) -> bool:
        """
        Validate extracted data structure (optional override).
        
        Args:
            data: Extracted data dictionary
            
        Returns:
            True if valid, False otherwise
        """
        # Default validation: check data is non-empty dict of lists
        if not isinstance(data, dict):
            return False
        
        if not data:
            return False
        
        for table_name, records in data.items():
            if not isinstance(records, list):
                return False
        
        return True
    
    def get_metadata(self, source_path: Path) -> Dict[str, Any]:
        """
        Get metadata about the data source (optional override).
        
        Args:
            source_path: Path to data source
            
        Returns:
            Metadata dictionary
        """
        return {
            "source_path": str(source_path),
            "exists": source_path.exists()
        }


class ConnectorRegistry:
    """
    Registry for managing multiple data source connectors.
    
    Allows registration and retrieval of connectors by name.
    """
    
    def __init__(self):
        """Initialize empty connector registry."""
        self._connectors: Dict[str, BaseConnector] = {}
    
    def register(self, name: str, connector: BaseConnector) -> None:
        """
        Register a connector.
        
        Args:
            name: Unique connector name
            connector: Connector instance
        """
        self._connectors[name] = connector
    
    def get(self, name: str) -> BaseConnector:
        """
        Get a registered connector by name.
        
        Args:
            name: Connector name
            
        Returns:
            Connector instance
            
        Raises:
            KeyError: If connector not registered
        """
        if name not in self._connectors:
            raise KeyError(f"Connector '{name}' not registered")
        return self._connectors[name]
    
    def list_connectors(self) -> List[str]:
        """
        List all registered connector names.
        
        Returns:
            List of connector names
        """
        return list(self._connectors.keys())
    
    def has_connector(self, name: str) -> bool:
        """
        Check if connector is registered.
        
        Args:
            name: Connector name
            
        Returns:
            True if registered, False otherwise
        """
        return name in self._connectors


# Global connector registry instance
connector_registry = ConnectorRegistry()