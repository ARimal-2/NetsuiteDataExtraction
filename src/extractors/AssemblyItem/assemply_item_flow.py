from utils.netsuite_flow import generate_flow
from src.extractors.AssemblyItem.list_assembly_item_id import list_assembly_items_id
from src.extractors.AssemblyItem.list_assembly_items import list_assembly_items

def assembly_item_flow(redis_client):
    aItem_ids, aItem_decide, aItem_details, aItem_no = generate_flow(
        resource_key="assembly_item",
        redis_client=redis_client,
        id_extractor=list_assembly_items_id,
        detail_extractor=list_assembly_items,
        batch_size=3000
    )
    aItem_decide(aItem_ids) >> [aItem_details, aItem_no]
    return aItem_ids
