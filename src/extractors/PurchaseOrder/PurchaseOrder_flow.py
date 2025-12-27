from utils.netsuite_flow import generate_flow

from src.extractors.PurchaseOrder.list_purchase_order_details import list_purchase_order
from src.extractors.PurchaseOrder.list_purchase_order_id import list_purchase_order_id

def PurchaseOrder_flow(redis_client):
    PO_ids, PO_decide, PO_details, PO_no = generate_flow(
        resource_key="PurchaseOrder",
        redis_client=redis_client,
        id_extractor=list_purchase_order_id,
        detail_extractor=list_purchase_order,
        batch_size=3000
    )
    PO_decide(PO_ids) >> [PO_details, PO_no]
    return PO_ids
