
import logging
import sys

def setup_logging(log_level="INFO", log_file="pipeline.log"):
    """
    Configures logging for the ETL pipeline.
    """
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

def get_logger(name):
    """
    Returns a logger with the specified name.
    """
    return logging.getLogger(name)
