from utils.netsuite_flow import generate_flow
from src.extractors.account.list_account_id import list_account_id
from src.extractors.account.list_account_details import list_account_details

def account_flow(redis_client):
    acc_ids, acc_decide, acc_details, acc_no = generate_flow(
        resource_key="account",
        redis_client=redis_client,
        id_extractor=list_account_id,
        detail_extractor=list_account_details,
        batch_size=3000
        
    )
    acc_decide(acc_ids) >> [acc_details, acc_no]
    return acc_ids
