"""
Logging Configuration Utilities
"""

import logging.config
import os
from pathlib import Path

def setup_logging():
    """Setup application logging"""
    # Import here to avoid circular dependency
    from ..config.settings import LOGGING_CONFIG, LOGS_DIR
    
    # Ensure logs directory exists
    LOGS_DIR.mkdir(exist_ok=True)
    
    # Configure logging
    logging.config.dictConfig(LOGGING_CONFIG)
    
    logger = logging.getLogger(__name__)
    logger.info("Logging configured successfully")

def get_logger(name):
    """Get logger with specified name"""
    return logging.getLogger(name)