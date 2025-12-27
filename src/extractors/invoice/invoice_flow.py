from utils.netsuite_flow import generate_flow
from src.extractors.invoice.list_invoice import list_invoice_details
from src.extractors.invoice.list_invoice_id import list_invoice_id

def invoice_flow(redis_client):
    fetch_ids, decide_path, fetch_details, no_ids = generate_flow(
        resource_key="Invoice",
        redis_client=redis_client,
        id_extractor=list_invoice_id,
        detail_extractor=list_invoice_details,
        batch_size=3000,  
    )

    decide_path(fetch_ids) >> [fetch_details, no_ids]

    return fetch_ids
