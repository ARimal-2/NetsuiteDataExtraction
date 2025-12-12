from utils.netsuite_flow import generate_flow

from src.extractors.SalesOrder.sales_order_details import list_sales_order
from src.extractors.SalesOrder.sales_order_ids import list_sales_order_ids

def SalesOrder_flow(redis_client):
    SO_ids, SO_decide, SO_details, SO_no = generate_flow(
        resource_key="SalesOrder",
        redis_client=redis_client,
        id_extractor=list_sales_order_ids,
        detail_extractor=list_sales_order,
        
    )
    SO_decide(SO_ids) >> [SO_details, SO_no]
    return SO_ids
