import os
from logger import logger

def get_supabase_headers():
    token = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not token:
        logger.warning("SUPABASE_SERVICE_ROLE_KEY not found in environment variables.")
    else:
        logger.debug("Supabase service role key loaded successfully.")
    return {
        "apikey": token,
        "Authorization": f"Bearer {token}",
        "Content-Type": "text/plain",
    }
