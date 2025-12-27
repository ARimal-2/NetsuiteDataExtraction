from utils.netsuite_flow import generate_flow
from src.extractors.InventoryItem.list_inventory_item_id import list_inventory_item_id
from src.extractors.InventoryItem.list_inventory_items import list_inventory_items

def InventoryItem_flow(redis_client):
    invenItem_ids, invenItem_decide, invenItem_details, invenItem_no = generate_flow(
        resource_key="InventoryItem",
        redis_client=redis_client,
        id_extractor=list_inventory_item_id,
        detail_extractor=list_inventory_items,
        batch_size=3000
    )
    invenItem_decide(invenItem_ids) >> [invenItem_details, invenItem_no]
    return invenItem_ids
