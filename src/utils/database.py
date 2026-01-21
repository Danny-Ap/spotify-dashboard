"""
MongoDB connection management for the Spotify data pipeline.

Provides a context manager for safe connection handling and
centralized database access.
"""

import os
import logging
from typing import Optional
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from dotenv import load_dotenv

from .config import DATABASE_NAME

load_dotenv()
logger = logging.getLogger(__name__)


class MongoDBConnection:
    """
    MongoDB connection manager with context manager support.

    Usage:
        with MongoDBConnection() as db:
            collection = db.get_collection("songs_master")
            collection.find_one(...)

    Or manual connection:
        db = MongoDBConnection()
        if db.connect():
            collection = db.get_collection("songs_master")
            ...
            db.close()
    """

    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize MongoDB connection.

        Args:
            connection_string: MongoDB connection string. If not provided,
                              reads from MONGODB_CONNECTION_STRING env var.
        """
        self.connection_string = connection_string or os.getenv('MONGODB_CONNECTION_STRING')
        self.client: Optional[MongoClient] = None
        self.db: Optional[Database] = None
        self._is_connected = False

    def connect(self) -> bool:
        """
        Establish connection to MongoDB Atlas.

        Returns:
            True if connection successful, False otherwise.
        """
        if self._is_connected:
            return True

        if not self.connection_string:
            logger.error("MongoDB connection string not found in environment variables")
            return False

        try:
            logger.info("Connecting to MongoDB Atlas...")
            self.client = MongoClient(self.connection_string)

            # Test the connection
            self.client.admin.command('ping')
            self.db = self.client[DATABASE_NAME]
            self._is_connected = True

            logger.info(f"Connected to MongoDB Atlas (database: {DATABASE_NAME})")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            self._is_connected = False
            return False

    def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
            self._is_connected = False
            logger.info("MongoDB connection closed")

    def __enter__(self) -> 'MongoDBConnection':
        """Context manager entry."""
        if not self.connect():
            raise ConnectionError("Failed to connect to MongoDB")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False  # Don't suppress exceptions

    def get_collection(self, collection_name: str) -> Collection:
        """
        Get a collection from the database.

        Args:
            collection_name: Name of the collection to retrieve.

        Returns:
            MongoDB Collection object.

        Raises:
            RuntimeError: If not connected to database.
        """
        if not self._is_connected or self.db is None:
            raise RuntimeError("Database not connected. Call connect() first or use context manager.")
        return self.db[collection_name]

    @property
    def is_connected(self) -> bool:
        """Check if currently connected to database."""
        return self._is_connected

    def get_collection_stats(self, collection_name: str) -> dict:
        """
        Get statistics for a collection.

        Args:
            collection_name: Name of the collection.

        Returns:
            Dictionary with count and other stats.
        """
        collection = self.get_collection(collection_name)
        return {
            "name": collection_name,
            "count": collection.count_documents({}),
        }
