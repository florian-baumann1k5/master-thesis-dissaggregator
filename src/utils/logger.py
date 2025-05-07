import logging
import os
from logging import Logger # Explicit import for clarity

# --- Define formats centrally for consistency ---
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(filename)s(l.%(lineno)d): %(funcName)s() - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def get_logger(
    name: str = __name__,
    console_level: int = logging.INFO,
    file_path: str = None,
    file_level: int = logging.DEBUG
) -> Logger:
    """
    Returns a logger configured to show source location (file, line, function).

    :param name: Name of the logger (typically __name__).
    :param console_level: Log level for console output (INFO by default).
    :param file_path: If provided, logs will be written to this file in addition
                      to console.
    :param file_level: Log level for file handler (DEBUG by default).
    :return: Configured Logger object.
    """

    logger = logging.getLogger(name)

    # Prevent adding duplicate handlers if called multiple times for the same logger
    if logger.handlers:
        # Optionally check if the configuration is the same or update it
        # For simplicity, we'll just return the existing logger here.
        # If you need dynamic reconfiguration, that's more complex.
        return logger

    # Set the master level - handlers can filter messages finer than this,
    # but not coarser. DEBUG captures everything.
    logger.setLevel(logging.DEBUG)

    # --- Console Handler ---
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    # Use the centrally defined format
    console_formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # --- File Handler (optional) ---
    if file_path is not None:
        try:
            # Ensure directory exists before creating the handler
            log_dir = os.path.dirname(file_path)
            if log_dir: # Avoid trying to create directories if path is just a filename
                os.makedirs(log_dir, exist_ok=True)

            file_handler = logging.FileHandler(file_path, encoding='utf-8') # Good practice to specify encoding
            file_handler.setLevel(file_level)
            # Use the centrally defined format
            file_formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        except OSError as e:
            # Log an error if the file handler couldn't be set up
            # Use a temporary basic config in case the logger setup itself failed
            logging.basicConfig()
            logging.error(f"Failed to create log file handler for {file_path}: {e}", exc_info=True)
            # Re-raise or handle appropriately depending on application needs
            # raise # Or maybe just continue without file logging

    # Prevent messages from propagating to the root logger if handlers are added
    # This is often desired to avoid duplicate messages if the root logger is
    # also configured (e.g., by a library or framework).
    logger.propagate = False

    return logger







# Example usage in other modules:
# logger.debug("Debug info: usually for diagnosing issues.")
# logger.info("Info message: normal operation details.")
# logger.warning("Warning: something unexpected but not fatal.")
# logger.error("Error: a serious issue occurred.")
# logger.critical("Critical: program may be unable to continue.")