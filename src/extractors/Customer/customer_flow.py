from utils.netsuite_flow import generate_flow
from src.extractors.Customer.list_customers_ids import list_customers_ids
from src.extractors.Customer.list_customer import customers_list

def customer_flow(redis_client):
    cust_ids, cust_decide, cust_details, cust_no = generate_flow(
        resource_key="customer",
        redis_client=redis_client,
        id_extractor=list_customers_ids,
        detail_extractor=customers_list,
       
    )
    cust_decide(cust_ids) >> [cust_details, cust_no]
    return cust_ids
