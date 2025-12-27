from utils.netsuite_flow import generate_flow
from src.extractors.department.list_department import list_department_details
from src.extractors.department.list_department_id import list_department_id

def department_flow(redis_client):
    dept_ids, dept_decide, dept_details, dept_no = generate_flow(
        resource_key="deptount",
        redis_client=redis_client,
        id_extractor=list_department_id,
        detail_extractor=list_department_details,
        batch_size=3000
    )
    dept_decide(dept_ids) >> [dept_details, dept_no]
    return dept_ids
