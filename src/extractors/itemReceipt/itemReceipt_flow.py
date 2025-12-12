from utils.netsuite_flow import generate_flow

from src.extractors.itemReceipt.list_itemReceipt import list_itemReceipt_details
from src.extractors.itemReceipt.list_itemReceipt_id import list_itemReceipt_id
def itemReceipt_flow(redis_client):
    itemReceipt_ids, itemReceipt_decide, itemReceipt_details, itemReceipt_no = generate_flow(
        resource_key="itemReceipt",
        redis_client=redis_client,
        id_extractor=list_itemReceipt_id,
        detail_extractor=list_itemReceipt_details,
        
    )
    itemReceipt_decide(itemReceipt_ids) >> [itemReceipt_details, itemReceipt_no]
    return itemReceipt_ids
