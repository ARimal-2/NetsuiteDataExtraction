from utils.netsuite_flow import generate_flow

from src.extractors.Subsidiary.list_subsidiary_id import list_subsidiary_id
from src.extractors.Subsidiary.list_subsidiary import list_subsidiary_details

def subsidiary_flow(redis_client):
    subsidiary_ids, subsidiary_decide, subsidiary_details, subsidiary_no = generate_flow(
        resource_key="subsidiary",
        redis_client=redis_client,
        id_extractor=list_subsidiary_id,
        detail_extractor=list_subsidiary_details,
        
    )
    subsidiary_decide(subsidiary_ids) >> [subsidiary_details, subsidiary_no]
    return subsidiary_ids
