from utils.netsuite_flow import generate_flow
from src.extractors.ItemFulfillment.list_item_fulfillment_id import list_item_fulfillment_id
from src.extractors.ItemFulfillment.list_item_fulfillment import list_item_fulfillment_details

def itemFulfillment_flow(redis_client):
    itmFul_ids, itmFul_decide, itmFul_details, itmFul_no = generate_flow(
        resource_key="itemFulfillment",
        redis_client=redis_client,
        id_extractor=list_item_fulfillment_id,
        detail_extractor=list_item_fulfillment_details,
        
    )
    itmFul_decide(itmFul_ids) >> [itmFul_details, itmFul_no]
    return itmFul_ids
