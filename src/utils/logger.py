import logging
import os
from logging import Logger

def get_logger(
    name: str = __name__,
    console_level: int = logging.INFO,
    file_path: str = None,
    file_level: int = logging.DEBUG
) -> Logger:
    """
    Returns a logger with a specified name.
    
    :param name: Name of the logger (typically __name__).
    :param console_level: Log level for console output (INFO by default).
    :param file_path: If provided, logs will be written to this file in addition to console.
    :param file_level: Log level for file handler (DEBUG by default).
    :return: Configured Logger object.
    """

    logger = logging.getLogger(name)

    # If logger already has handlers, return it to avoid adding duplicates
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)  # Master level; individual handlers can filter further

    # --- Console Handler ---
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # --- File Handler (optional) ---
    if file_path is not None:
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        file_handler = logging.FileHandler(file_path)
        file_handler.setLevel(file_level)
        file_formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger







# Example usage in other modules:
# logger.debug("Debug info: usually for diagnosing issues.")
# logger.info("Info message: normal operation details.")
# logger.warning("Warning: something unexpected but not fatal.")
# logger.error("Error: a serious issue occurred.")
# logger.critical("Critical: program may be unable to continue.")