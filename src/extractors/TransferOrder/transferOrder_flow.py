
from utils.netsuite_flow import generate_flow

from src.extractors.TransferOrder.list_transferOrder_id import list_transferOrder_id
from src.extractors.TransferOrder.list_transferOrder import list_transferOrder_details

def transferOrder_flow(redis_client):
    transferOrder_ids, transferOrder_decide, transferOrder_details, transferOrder_no = generate_flow(
        resource_key="transferOrder",
        redis_client=redis_client,
        id_extractor=list_transferOrder_id,
        detail_extractor=list_transferOrder_details,
        
    )
    transferOrder_decide(transferOrder_ids) >> [transferOrder_details, transferOrder_no]
    return transferOrder_ids
