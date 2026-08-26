import logging
import sys

def setup_logging(level=logging.INFO):
    """Set up the default root logger configuration."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

def get_logger(name):
    """Retrieve a logger with the specified name."""
    return logging.getLogger(name)
