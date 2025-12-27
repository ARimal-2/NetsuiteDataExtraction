from utils.netsuite_flow import generate_flow
from src.extractors.inventoryCount.list_inventory_count_id import list_inventory_count_id
from src.extractors.inventoryCount.list_inventory_count import list_inventory_count

def inventory_count_flow(redis_client):
    invCount_ids, invCount_decide, invCount_details, invCount_no = generate_flow(
        resource_key="InventoryCount",
        redis_client=redis_client,
        id_extractor=list_inventory_count_id,
        detail_extractor=list_inventory_count,
        batch_size=3000
    )
    invCount_decide(invCount_ids) >> [invCount_details, invCount_no]
    return invCount_ids
