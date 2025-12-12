from utils.netsuite_flow import generate_flow
from src.extractors.InterCompanyTransferOrder.list_intercompany_Transfer_Order_id import list_intercompanyTransferOrder_id
from src.extractors.InterCompanyTransferOrder.list_intercompanyTransferOrder_details import list_intercompanyTransferOrder_details

def interCompanyTransferOrder_flow(redis_client):
    ICTO_ids, ICTO_decide, ICTO_details, ICTO_no = generate_flow(
        resource_key="interCompanyTransferOrder",
        redis_client=redis_client,
        id_extractor=list_intercompanyTransferOrder_id,
        detail_extractor=list_intercompanyTransferOrder_details,
        
    )
    ICTO_decide(ICTO_ids) >> [ICTO_details, ICTO_no]
    return ICTO_ids
