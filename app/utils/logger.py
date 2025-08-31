import logging
import os
import requests
from typing import Optional

def setup_logging():
    """Setup logging configuration"""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
        ]
    )

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
        logger.error(message, exc_info=exc_info)
        send_discord_notification(f"ERROR: {message}")
    elif level.upper() == "WARNING":
        logger.warning(message)
    elif level.upper() == "INFO":
        logger.info(message)
    else:
        logger.debug(message)
