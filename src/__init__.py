from .utils.logger import get_logger

import logging

logger = get_logger(
    name=__name__,
    console_level=logging.INFO,      # Log INFO and above to console
    file_path="logs/app.log",        # Log output to a file
    file_level=logging.DEBUG         # File logs at DEBUG level and above
)
