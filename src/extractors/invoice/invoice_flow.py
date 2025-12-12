from utils.netsuite_flow import generate_flow

from src.extractors.invoice.list_invoice import list_invoice_details
from src.extractors.invoice.list_invoice_id import list_invoice_id
def invoice_flow(redis_client):
    invoice_ids, invoice_decide, invoice_details, invoice_no = generate_flow(
        resource_key="Invoice",
        redis_client=redis_client,
        id_extractor=list_invoice_id,
        detail_extractor=list_invoice_details,
        
    )
    invoice_decide(invoice_ids) >> [invoice_details, invoice_no]
    return invoice_ids
