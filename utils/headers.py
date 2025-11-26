from dotenv import load_dotenv
import os
from oauthlib.oauth1 import Client

# Load environment variables
load_dotenv()

def get_netsuite_headers(url, method="GET"):
    """
    Generate signed OAuth1 headers for aiohttp NetSuite API requests.
    """
    required_vars = [
        "CONSUMER_KEY",
        "CONSUMER_SECRET",
        "TOKEN_ID",
        "TOKEN_SECRET",
        "ACCOUNT_ID"
    ]
    missing_vars = [var for var in required_vars if os.getenv(var) is None]
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

    client = Client(
        client_key=os.getenv("CONSUMER_KEY"),
        client_secret=os.getenv("CONSUMER_SECRET"),
        resource_owner_key=os.getenv("TOKEN_ID"),
        resource_owner_secret=os.getenv("TOKEN_SECRET"),
        signature_method="HMAC-SHA256",
        realm=os.getenv("ACCOUNT_ID")
    )

    uri, headers, body = client.sign(
        url,
        http_method=method,
        headers={"Content-Type": "application/json"}
    )

    return headers
