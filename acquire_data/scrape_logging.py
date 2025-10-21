import logging
import json

def setup_logger(name="scrapers_logger", log_file="scrapers.log", level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)

    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # Add handler to logger
    if not logger.handlers:
        logger.addHandler(file_handler)

    return logger


def update_last_updates(last_updates_file, field, data):
    # Load the last updates JSON file
    try:
        with open(last_updates_file, "r") as f:
            last_updates = json.load(f)
    except FileNotFoundError:
        last_updates = {}

    # Update the interests section
    last_updates[field] = data

    # Save the updated last updates JSON file
    with open(last_updates_file, "w") as f:
        json.dump(last_updates, f, indent=4)