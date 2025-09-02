import logging
import os
import requests
import json
import time
from typing import Optional, Dict, Any
from collections import defaultdict

# Rate limiting for Discord notifications
_discord_rate_limiter = defaultdict(lambda: {"last_sent": 0, "count": 0})

def setup_logging():
    """Setup logging configuration"""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    
    # Clear any existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level))
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level),
        handlers=[console_handler],
        force=True
    )
    
    # Test that logging is working
    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured with level: {log_level}")

def send_discord_notification(message: str, webhook_url: Optional[str] = None):
    """Send notification to Discord webhook for debugging"""
    webhook_url = webhook_url or os.getenv("DISCORD_WEBHOOK")
    
    if not webhook_url:
        logging.warning("No Discord webhook URL configured")
        return
    
    try:
        payload = {
            "content": f"BDQ Service: {message}"
        }
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to send Discord notification: {e}")

def log_and_notify(level: str, message: str, exc_info: bool = False):
    """Log message and optionally send Discord notification"""
    logger = logging.getLogger(__name__)
    
    if level.upper() == "ERROR":
        send_discord_notification(f"ERROR: {message}")
        logger.error(message, exc_info=exc_info)
    elif level.upper() == "WARNING":
        send_discord_notification(f"WARNING: {message}")
        logger.warning(message)
    elif level.upper() == "INFO":
        send_discord_notification(f"INFO: {message}")
        logger.info(message)
    else:
        send_discord_notification(f"DEBUG: {message}")
        logger.debug(message)

def send_structured_discord_notification(data: Dict[str, Any], webhook_url: Optional[str] = None):
    """Send structured notification to Discord webhook for debugging"""
    webhook_url = webhook_url or os.getenv("DISCORD_WEBHOOK")
    
    if not webhook_url:
        logging.warning("No Discord webhook URL configured")
        return
    
    try:
        # Format as JSON code block for better readability
        json_content = json.dumps(data, indent=2)
        payload = {
            "content": f"```json\n{json_content}\n```"
        }
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to send structured Discord notification: {e}")

def send_bdq_error_notification(
    request_id: str,
    test_id: str,
    java_class: str,
    java_method: str,
    tuple_sample: Dict[str, str],
    parameters: Dict[str, str],
    exception_info: Dict[str, str],
    attempt: int = 1,
    webhook_url: Optional[str] = None
):
    """Send structured BDQ error notification with rate limiting"""
    
    # Create rate limiting key
    rate_key = f"{test_id}:{exception_info.get('type', '')}:{exception_info.get('message', '')}"
    current_time = time.time()
    
    # Check rate limiting (at most one alert per unique error per minute)
    limiter = _discord_rate_limiter[rate_key]
    if current_time - limiter["last_sent"] < 60:  # 60 seconds
        limiter["count"] += 1
        return  # Rate limited, don't send
    
    # Reset or update rate limiter
    suppressed_count = limiter["count"]
    limiter["last_sent"] = current_time
    limiter["count"] = 0
    
    # Build structured error data
    error_data = {
        "kind": "bdq_error",
        "requestId": request_id,
        "testId": test_id,
        "javaClass": java_class,
        "javaMethod": java_method,
        "tupleSample": tuple_sample,
        "parameters": parameters,
        "exception": exception_info,
        "server": {
            "version": "bdq-jvm-server 1.0.0",
            "libs": {
                "geo_ref_qc": "2.1.2-SNAPSHOT",
                "event_date_qc": "3.1.1-SNAPSHOT",
                "sci_name_qc": "1.2.1-SNAPSHOT",
                "rec_occur_qc": "1.1.1-SNAPSHOT"
            }
        },
        "attempt": attempt
    }
    
    if suppressed_count > 0:
        error_data["suppressedCount"] = suppressed_count
    
    send_structured_discord_notification(error_data, webhook_url)

def send_bdq_progress_notification(
    test_id: str,
    processed: int,
    total: int,
    success: int,
    fail: int,
    elapsed_sec: int,
    webhook_url: Optional[str] = None
):
    """Send structured BDQ progress notification"""
    
    progress_data = {
        "kind": "bdq_progress",
        "testId": test_id,
        "processed": processed,
        "total": total,
        "success": success,
        "fail": fail,
        "elapsedSec": elapsed_sec
    }
    
    send_structured_discord_notification(progress_data, webhook_url)
