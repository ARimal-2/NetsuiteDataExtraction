import logging
from datetime import datetime, timezone

# -----------------------------
# ID Extraction
# -----------------------------
def extract_ids(resource_data, logger: logging.Logger) -> list:
    """
    Extract `id` from a list of dicts and return as a list of IDs.

    Args:
        resource_data (list): List of dictionaries returned from API.
        logger (logging.Logger): Logger instance.

    Returns:
        List of IDs.
    """
    try:
        id_list = [item["id"] for item in resource_data if isinstance(item, dict) and "id" in item]
        logger.info(f"Extracted {len(id_list)} IDs")
        return id_list
    except Exception as e:
        logger.error(f"Error extracting IDs: {e}")
        return []

# -----------------------------
# Convert datetime to NetSuite display format
# -----------------------------
def to_netsuite_display(dt_or_str, logger: logging.Logger) -> str:
    """
    Convert a datetime or ISO-8601 string to NetSuite display format: MM/DD/YYYY hh:mm AM/PM.

    Assumes input is UTC if no timezone is present.

    Args:
        dt_or_str (datetime | str): Datetime object or ISO string.
        logger (logging.Logger): Logger instance.

    Returns:
        str: Formatted datetime string for NetSuite.
    """
    try:
        if isinstance(dt_or_str, datetime):
            dt = dt_or_str
        else:
            s = str(dt_or_str).strip()
            # Handle UTC 'Z' suffix
            if s.endswith("Z"):
                s = s[:-1]
                dt = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
            else:
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)

        # Keep UTC; adjust here if conversion to local tz is needed
        dt_utc = dt.astimezone(timezone.utc)
        formatted = dt_utc.strftime("%m/%d/%Y %I:%M %p")
        logger.info(f"Converted timestamp to NetSuite format: {formatted}")
        return formatted
    except Exception as e:
        logger.error(f"Failed to convert datetime '{dt_or_str}' to NetSuite format: {e}")
        return ""

# -----------------------------
# JSON Validation
# -----------------------------
def validate_json(data, logger: logging.Logger) -> dict:
    """
    Validate JSON data from API response.

    Args:
        data: The data to validate.
        logger (logging.Logger): Logger instance.

    Returns:
        dict: Valid JSON data, or empty dict if invalid.
    """
    if isinstance(data, dict):
        return data

    logger.error(f"Invalid JSON structure: {str(data)[:200]}")
    return {}
