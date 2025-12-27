from utils.netsuite_flow import generate_flow
from src.extractors.InventoryNumber.list_inventory_details import list_inventory_details
from src.extractors.InventoryNumber.list_inventory_number import list_inventory_id

def inventoryNumber_flow(redis_client):
    ivenNum_ids, ivenNum_decide, ivenNum_details, ivenNum_no = generate_flow(
        resource_key="inventoryNumber",
        redis_client=redis_client,
        id_extractor=list_inventory_id,
        detail_extractor=list_inventory_details,
        batch_size=3000
    )
    ivenNum_decide(ivenNum_ids) >> [ivenNum_details, ivenNum_no]
    return ivenNum_ids
