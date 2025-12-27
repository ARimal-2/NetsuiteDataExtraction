from utils.netsuite_flow import generate_flow

from src.extractors.vendor.list_vendor_id import list_vendor_id
from src.extractors.vendor.list_vendor import list_vendor_details

def vendor_flow(redis_client):
    vendor_ids, vendor_decide, vendor_details, vendor_no = generate_flow(
        resource_key="vendor",
        redis_client=redis_client,
        id_extractor=list_vendor_id,
        detail_extractor=list_vendor_details,
        batch_size=3000
    )
    vendor_decide(vendor_ids) >> [vendor_details, vendor_no]
    return vendor_ids
