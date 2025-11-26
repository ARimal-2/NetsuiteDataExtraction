import os
import ibm_boto3
from ibm_botocore.client import Config
from dotenv import load_dotenv

load_dotenv()

def connect_to_cos():
    api_key = os.getenv("API_KEY")
    service_instance_id = os.getenv("SERVICE_INSTANCE_ID")
    endpoint_url = os.getenv("IBM_COS_ENDPOINT")
    bucket_name = os.getenv("BUCKET_NAME")

    if not all([api_key, service_instance_id, endpoint_url, bucket_name]):
        raise EnvironmentError("One or more IBM COS environment variables are missing.")

    # Create COS client using IAM API key
    cos = ibm_boto3.client(
        "s3",
        ibm_api_key_id=api_key,
        ibm_service_instance_id=service_instance_id,
        config=Config(signature_version="oauth"),
        endpoint_url=endpoint_url
    )
    return cos, bucket_name
