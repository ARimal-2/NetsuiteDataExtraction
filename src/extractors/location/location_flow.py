from utils.netsuite_flow import generate_flow

from src.extractors.location.list_location import list_location_details
from src.extractors.location.list_location_id import list_location_id
def location_flow(redis_client):
    location_ids, location_decide, location_details, location_no = generate_flow(
        resource_key="location",
        redis_client=redis_client,
        id_extractor=list_location_id,
        detail_extractor=list_location_details,
        
    )
    location_decide(location_ids) >> [location_details, location_no]
    return location_ids
