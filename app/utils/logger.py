import logging
import os
import requests
from typing import Optional

def setup_logging():
    """Setup logging configuration"""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    
    # Clear any existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Create a console handler that explicitly writes to stdout
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
    print(f"STDOUT: Logging configured with level: {log_level}")  # Also print to ensure it shows up

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
