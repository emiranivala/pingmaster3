import asyncio
import logging
import random
import time
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class ConnectionManager:
    """Manages connections to prevent too many concurrent requests to the same resource."""
    
    # Dictionary to store locks for different connections
    _connection_locks: Dict[str, asyncio.Lock] = {}
    
    # Dictionary to store message cache
    _message_cache: Dict[str, Dict[str, Any]] = {}
    
    # Dictionary to store last edited message content
    _last_edit_content: Dict[str, str] = {}
    
    # Cache expiration time (in seconds)
    CACHE_EXPIRY = 3600  # 1 hour
    
    # Maximum number of concurrent connections per chat
    MAX_CONCURRENT_CONNECTIONS = 2  # Reduced from 3 to avoid hitting limits
    
    # Rate limiting settings
    _request_timestamps: Dict[str, List[datetime]] = {}
    MAX_REQUESTS_PER_MINUTE = 15  # Reduced from 20 to be more conservative
    
    # Connection pool management
    _active_connections: Dict[str, int] = {}
    _last_connection_time: Dict[str, float] = {}
    MIN_CONNECTION_INTERVAL = 1.0  # Minimum time between connections in seconds
    
    @classmethod
    async def with_connection_lock(cls, connection_id: str, coro):
        """Execute a coroutine with a connection lock to prevent too many concurrent requests."""
        # Check rate limits first
        await cls._check_rate_limit(connection_id)
        
        # Check connection interval to avoid rapid connections
        await cls._check_connection_interval(connection_id)
        
        # Get or create a lock for this connection
        if connection_id not in cls._connection_locks:
            cls._connection_locks[connection_id] = asyncio.Lock()
        
        # Track active connections
        cls._increment_active_connections(connection_id)
        
        try:
            # Acquire the lock and execute the coroutine
            async with cls._connection_locks[connection_id]:
                # Add a small delay to space out requests
                await asyncio.sleep(0.5)
                return await coro
        finally:
            # Always decrement active connections count
            cls._decrement_active_connections(connection_id)
    
    @classmethod
    async def _check_rate_limit(cls, connection_id: str):
        """Check if the rate limit has been exceeded for this connection."""
        now = datetime.now()
        minute_ago = now - timedelta(minutes=1)
        
        # Initialize timestamps list if not exists
        if connection_id not in cls._request_timestamps:
            cls._request_timestamps[connection_id] = []
        
        # Remove timestamps older than 1 minute
        cls._request_timestamps[connection_id] = [
            ts for ts in cls._request_timestamps[connection_id] if ts > minute_ago
        ]
        
        # Check if rate limit exceeded
        if len(cls._request_timestamps[connection_id]) >= cls.MAX_REQUESTS_PER_MINUTE:
            # Calculate wait time based on oldest request
            wait_time = (cls._request_timestamps[connection_id][0] + timedelta(minutes=1) - now).total_seconds()
            
            # Ensure wait time is positive
            wait_time = max(wait_time, 1.0)
            
            # Add jitter to avoid thundering herd problem
            jitter = wait_time * 0.1  # 10% jitter
            wait_time += random.uniform(0, jitter)
            
            logger.warning(f"Rate limit exceeded for {connection_id}. Waiting {wait_time:.2f} seconds.")
            await asyncio.sleep(wait_time + 1)  # Add 1 second buffer
            
            # Recursive check after waiting to ensure we're under the limit
            return await cls._check_rate_limit(connection_id)
        
        # Add current timestamp
        cls._request_timestamps[connection_id].append(now)
    
    @classmethod
    async def _check_connection_interval(cls, connection_id: str):
        """Ensure minimum time between connections to the same resource."""
        current_time = time.time()
        
        if connection_id in cls._last_connection_time:
            elapsed = current_time - cls._last_connection_time[connection_id]
            if elapsed < cls.MIN_CONNECTION_INTERVAL:
                wait_time = cls.MIN_CONNECTION_INTERVAL - elapsed
                logger.debug(f"Spacing connections for {connection_id}. Waiting {wait_time:.2f} seconds.")
                await asyncio.sleep(wait_time)
        
        # Update last connection time
        cls._last_connection_time[connection_id] = time.time()
    
    @classmethod
    def _increment_active_connections(cls, connection_id: str):
        """Increment the count of active connections for a resource."""
        if connection_id not in cls._active_connections:
            cls._active_connections[connection_id] = 0
        
        cls._active_connections[connection_id] += 1
        logger.debug(f"Active connections for {connection_id}: {cls._active_connections[connection_id]}")
    
    @classmethod
    def _decrement_active_connections(cls, connection_id: str):
        """Decrement the count of active connections for a resource."""
        if connection_id in cls._active_connections and cls._active_connections[connection_id] > 0:
            cls._active_connections[connection_id] -= 1
            logger.debug(f"Active connections for {connection_id}: {cls._active_connections[connection_id]}")

    
    @classmethod
    async def cache_message(cls, cache_key: str, message):
        """Cache a message for future use."""
        cls._message_cache[cache_key] = {
            'message': message,
            'expires_at': datetime.now() + timedelta(seconds=cls.CACHE_EXPIRY)
        }
    
    @classmethod
    async def get_cached_message(cls, cache_key: str) -> Optional[Any]:
        """Get a cached message if it exists and hasn't expired."""
        if cache_key in cls._message_cache:
            cache_entry = cls._message_cache[cache_key]
            if datetime.now() < cache_entry['expires_at']:
                return cache_entry['message']
            else:
                # Remove expired cache entry
                del cls._message_cache[cache_key]
        return None
    
    @classmethod
    def clear_cache(cls):
        """Clear the message cache."""
        cls._message_cache.clear()
    
    @classmethod
    def clear_expired_cache(cls):
        """Clear expired cache entries."""
        now = datetime.now()
        keys_to_delete = [
            key for key, entry in cls._message_cache.items() 
            if now > entry['expires_at']
        ]
        for key in keys_to_delete:
            del cls._message_cache[key]
            
    @classmethod
    async def safe_edit_message_text(cls, client, chat_id, message_id, new_text):
        """Safely edit a message's text, avoiding MESSAGE_NOT_MODIFIED errors.
        
        Args:
            client: The Pyrogram client to use
            chat_id: The chat ID where the message is located
            message_id: The message ID to edit
            new_text: The new text for the message
            
        Returns:
            The edited message or None if no edit was needed
        """
        # Create a unique key for this message
        edit_key = f"{chat_id}_{message_id}"
        
        # Check if we've edited this message before
        if edit_key in cls._last_edit_content and cls._last_edit_content[edit_key] == new_text:
            logger.debug(f"Skipping edit for message {message_id} in chat {chat_id}: content unchanged")
            return None
        
        # Store the new content and perform the edit
        cls._last_edit_content[edit_key] = new_text
        
        try:
            return await client.edit_message_text(chat_id, message_id, new_text)
        except Exception as e:
            logger.error(f"Error editing message: {str(e)}")
            raise