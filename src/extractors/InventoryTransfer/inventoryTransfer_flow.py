from utils.netsuite_flow import generate_flow
from src.extractors.InventoryTransfer.list_inventory_transfer_id import list_inventory_transfer_id
from src.extractors.InventoryTransfer.list_inventory_transfer_details import list_inventory_transfer_details

def inventoryTransfer_flow(redis_client):
    ivenTrans_ids, ivenTrans_decide, ivenTrans_details, ivenTrans_no = generate_flow(
        resource_key="InventoryTransfer",
        redis_client=redis_client,
        id_extractor=list_inventory_transfer_id,
        detail_extractor=list_inventory_transfer_details,
        batch_size=3000
    )
    ivenTrans_decide(ivenTrans_ids) >> [ivenTrans_details, ivenTrans_no]
    return ivenTrans_ids
