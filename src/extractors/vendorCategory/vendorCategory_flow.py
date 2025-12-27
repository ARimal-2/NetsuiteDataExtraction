from utils.netsuite_flow import generate_flow

from src.extractors.vendorCategory.list_vendorCategory_id import list_vendorCategory_id
from src.extractors.vendorCategory.list_vendorCategory import list_vendorCategory_details

def vendorCategory_flow(redis_client):
    vendorCategory_ids, vendorCategory_decide, vendorCategory_details, vendorCategory_no = generate_flow(
        resource_key="vendorCategory",
        redis_client=redis_client,
        id_extractor=list_vendorCategory_id,
        detail_extractor=list_vendorCategory_details,
        batch_size=3000
    )
    vendorCategory_decide(vendorCategory_ids) >> [vendorCategory_details, vendorCategory_no]
    return vendorCategory_ids
