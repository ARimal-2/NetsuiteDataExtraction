from utils.netsuite_flow import generate_flow

from src.extractors.vendorBill.list_vendorBill_id import list_vendorBill_id
from src.extractors.vendorBill.list_vendorBill import list_vendorBill_details

def vendorBill_flow(redis_client):
    vendorBill_ids, vendorBill_decide, vendorBill_details, vendorBill_no = generate_flow(
        resource_key="vendorBill",
        redis_client=redis_client,
        id_extractor=list_vendorBill_id,
        detail_extractor=list_vendorBill_details,
        
    )
    vendorBill_decide(vendorBill_ids) >> [vendorBill_details, vendorBill_no]
    return vendorBill_ids
