# Engine/logger.py

import logging

logger = logging.getLogger("panelitix")
logger.setLevel(logging.DEBUG)  # or INFO in prod

formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

if not logger.handlers:  # Prevent duplicate handlers on reload
    logger.addHandler(console_handler)
