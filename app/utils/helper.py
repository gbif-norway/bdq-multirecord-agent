
from typing import List, Dict, Optional, Any
import pandas as pd
import logging
import requests
import os
from pydantic import BaseModel

# Minimal root logger config so Cloud Run captures logs
_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _LEVEL, logging.INFO),
    handlers=[logging.StreamHandler()],
    force=True,
)


def log(message: str, level: str = "INFO"):
    """Simple logging function that logs to console and Discord"""
    logger = logging.getLogger(__name__)
    
    # Log to console using standard Python logging
    if level.upper() == "DEBUG":
        logger.debug(message)
    elif level.upper() == "INFO":
        logger.info(message)
    elif level.upper() == "WARNING":
        logger.warning(message)
    elif level.upper() == "ERROR":
        logger.error(f"🚨 {message}")
    else:
        logger.info(message)
    
    # Send to Discord
    webhook_url = os.getenv("DISCORD_WEBHOOK")
    if webhook_url:
        try:
            requests.post(webhook_url, json={"content": message}, timeout=10)
        except Exception:
            pass  # Don't let Discord failures break logging

