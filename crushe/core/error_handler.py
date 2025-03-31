import asyncio
import logging
import random
import time
from functools import wraps
from typing import Callable, Any, Optional, Dict, Tuple

from pyrogram.errors import FloodWait, RPCError, BadRequest, Unauthorized, Forbidden, MessageNotModified
from pyrogram.errors.exceptions.flood_420 import FloodWait
from requests.exceptions import ConnectionError, Timeout, RequestException

logger = logging.getLogger(__name__)

# Configure more detailed logging for error tracking
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s/%(asctime)s] %(name)s: %(message)s',
)

# Set specific loggers to higher levels to reduce noise
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("pyrogram.session").setLevel(logging.ERROR)
logging.getLogger("pyrogram.connection").setLevel(logging.ERROR)
logging.getLogger("pyrogram.client").setLevel(logging.ERROR)
logging.getLogger("asyncio").setLevel(logging.WARNING)


async def exponential_backoff(attempt: int, base_delay: float = 1.0, max_delay: float = 60.0) -> float:
    """
    Calculate delay with exponential backoff and jitter for retries.
    
    Args:
        attempt: The current attempt number (starting from 1)
        base_delay: The base delay in seconds
        max_delay: Maximum delay in seconds
        
    Returns:
        The calculated delay in seconds
    """
    # Calculate exponential backoff
    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
    # Add jitter (±25%)
    jitter = delay * 0.25
    delay = delay + random.uniform(-jitter, jitter)
    logger.debug(f"Calculated backoff delay: {delay:.2f}s for attempt {attempt}")
    return delay


# Track rate limits per chat to avoid hitting limits
_rate_limit_tracker: Dict[int, Tuple[float, int]] = {}

def should_rate_limit(chat_id: int, max_requests: int = 20, time_window: float = 60.0) -> bool:
    """
    Check if operations for a specific chat should be rate limited.
    
    Args:
        chat_id: The chat ID to check
        max_requests: Maximum number of requests allowed in the time window
        time_window: Time window in seconds
        
    Returns:
        True if rate limit should be applied, False otherwise
    """
    current_time = time.time()
    
    if chat_id not in _rate_limit_tracker:
        _rate_limit_tracker[chat_id] = (current_time, 1)
        return False
    
    last_time, count = _rate_limit_tracker[chat_id]
    
    # Reset counter if time window has passed
    if current_time - last_time > time_window:
        _rate_limit_tracker[chat_id] = (current_time, 1)
        return False
    
    # Update counter
    if count >= max_requests:
        logger.warning(f"Rate limit reached for chat {chat_id}: {count} requests in {time_window}s")
        return True
    
    _rate_limit_tracker[chat_id] = (last_time, count + 1)
    return False


def retry_with_backoff(max_retries: int = 5, initial_delay: float = 1.0, max_delay: float = 60.0):
    """
    Decorator to retry functions with exponential backoff when FloodWait or connection errors occur.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            last_error = None
            
            # Check if first argument is a message with chat_id (for rate limiting)
            chat_id = None
            if args and hasattr(args[0], 'chat') and hasattr(args[0].chat, 'id'):
                chat_id = args[0].chat.id
                if should_rate_limit(chat_id):
                    # Add small delay to avoid hitting rate limits
                    await asyncio.sleep(2.0)
            
            while True:
                try:
                    return await func(*args, **kwargs)
                except FloodWait as e:
                    # Handle Telegram's FloodWait explicitly
                    wait_time = e.value if hasattr(e, 'value') else e.x
                    logger.warning(f"FloodWait error in {func.__name__}: waiting for {wait_time} seconds")
                    # Add a small buffer to the wait time
                    await asyncio.sleep(wait_time + 1)
                    # Don't count FloodWait against retry limit as it's a server instruction
                    continue
                except (ConnectionError, Timeout, RequestException, TimeoutError, asyncio.TimeoutError) as e:
                    retries += 1
                    last_error = e
                    if retries > max_retries:
                        logger.error(f"Maximum retries ({max_retries}) exceeded in {func.__name__}: {str(e)}")
                        raise
                    
                    delay = await exponential_backoff(retries, initial_delay, max_delay)
                    logger.info(f"Connection error in {func.__name__}: {str(e)}. Retrying in {delay:.2f} seconds (attempt {retries}/{max_retries})")
                    await asyncio.sleep(delay)
                except MessageNotModified:
                    # This is not an error - the message content was the same
                    logger.info(f"Message not modified in {func.__name__}: Content unchanged")
                    return await func(*args, **kwargs)  # Return the original result
                except (BadRequest, Unauthorized, Forbidden) as e:
                    # Don't retry client errors
                    logger.error(f"Client error in {func.__name__}: {str(e)}")
                    raise
                except RPCError as e:
                    retries += 1
                    last_error = e
                    if retries > max_retries:
                        logger.error(f"Maximum retries ({max_retries}) exceeded in {func.__name__}: {str(e)}")
                        raise
                    
                    delay = await exponential_backoff(retries, initial_delay, max_delay)
                    logger.info(f"RPC error in {func.__name__}: {str(e)}. Retrying in {delay:.2f} seconds (attempt {retries}/{max_retries})")
                    await asyncio.sleep(delay)
                except Exception as e:
                    # Log unexpected errors but don't retry
                    logger.error(f"Unexpected error in {func.__name__}: {str(e)}")
                    raise
        return wrapper
    return decorator


async def safe_execute(func: Callable, *args, **kwargs) -> Optional[Any]:
    """
    Safely execute a function with error handling and automatic retries.
    
    Args:
        func: The function to execute
        *args: Arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the function or None if an error occurred
    """
    max_retries = kwargs.pop('max_retries', 3) if 'max_retries' in kwargs else 3
    current_retry = 0
    
    # Check if first argument is a message with chat_id (for rate limiting)
    chat_id = None
    if args and hasattr(args[0], 'chat') and hasattr(args[0].chat, 'id'):
        chat_id = args[0].chat.id
        if should_rate_limit(chat_id):
            # Add small delay to avoid hitting rate limits
            await asyncio.sleep(2.0)
    
    while current_retry <= max_retries:
        try:
            return await func(*args, **kwargs)
        except FloodWait as e:
            wait_time = e.value if hasattr(e, 'value') else e.x
            logger.warning(f"FloodWait error in {func.__name__}: waiting for {wait_time + 1} seconds")
            await asyncio.sleep(wait_time + 1)  # Add 1 second buffer
            # Don't count FloodWait against retry limit
            continue
        except (ConnectionError, Timeout, RequestException, TimeoutError, asyncio.TimeoutError) as e:
            current_retry += 1
            if current_retry > max_retries:
                logger.error(f"Maximum retries ({max_retries}) exceeded in {func.__name__}: {str(e)}")
                return None
                
            delay = await exponential_backoff(current_retry, 1.0, 30.0)
            logger.info(f"Connection error in {func.__name__}: {str(e)}. Retrying in {delay:.2f} seconds (attempt {current_retry}/{max_retries})")
            await asyncio.sleep(delay)
        except MessageNotModified:
            # This is not an error - the message content was the same
            logger.info(f"Message not modified in {func.__name__}: Content unchanged")
            return await func(*args, **kwargs)  # Return the original result
        except (BadRequest, Unauthorized, Forbidden) as e:
            # Don't retry client errors
            logger.error(f"Client error in {func.__name__}: {str(e)}")
            return None
        except RPCError as e:
            current_retry += 1
            if current_retry > max_retries:
                logger.error(f"Maximum retries ({max_retries}) exceeded in {func.__name__}: {str(e)}")
                return None
                
            delay = await exponential_backoff(current_retry, 1.0, 30.0)
            logger.info(f"RPC error in {func.__name__}: {str(e)}. Retrying in {delay:.2f} seconds (attempt {current_retry}/{max_retries})")
            await asyncio.sleep(delay)
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {str(e)}")
            return None
    
    return None


async def run_with_lock(lock: asyncio.Lock, func: Callable, *args, **kwargs) -> Any:
    """
    Run a function with a lock to prevent concurrent execution.
    
    Args:
        lock: The lock to acquire
        func: The function to execute
        *args: Arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the function
    """
    async with lock:
        return await func(*args, **kwargs)


async def run_with_timeout(func: Callable, timeout: float, *args, **kwargs) -> Any:
    """
    Run a function with a timeout.
    
    Args:
        func: The function to execute
        timeout: Timeout in seconds
        *args: Arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the function
        
    Raises:
        asyncio.TimeoutError: If the function takes longer than the timeout
    """
    return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)