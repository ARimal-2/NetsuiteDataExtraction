from dotenv import load_dotenv
import os
from oauthlib.oauth1 import Client

load_dotenv()

def get_netsuite_headers(url: str, method: str = "GET") -> dict:
    """
    Generate OAuth1 headers for NetSuite REST API.
    Nonce and timestamp are regenerated on every call.
    """
    required_vars = [
        "CONSUMER_KEY",
        "CONSUMER_SECRET",
        "TOKEN_ID",
        "TOKEN_SECRET",
        "ACCOUNT_ID",
    ]

    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        raise EnvironmentError(
            f"Missing NetSuite environment variables: {', '.join(missing)}"
        )

    client = Client(
        client_key=os.getenv("CONSUMER_KEY"),
        client_secret=os.getenv("CONSUMER_SECRET"),
        resource_owner_key=os.getenv("TOKEN_ID"),
        resource_owner_secret=os.getenv("TOKEN_SECRET"),
        signature_method="HMAC-SHA256",
        realm=os.getenv("ACCOUNT_ID"),
    )

    _, headers, _ = client.sign(
        url,
        http_method=method,
        headers={"Content-Type": "application/json"},
    )

    return headers
