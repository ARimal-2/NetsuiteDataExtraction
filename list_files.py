import ibm_boto3
from ibm_botocore.client import Config
from dotenv import load_dotenv

load_dotenv()

# IBM COS credentials
ACCESS_KEY = "6de1127078464ff6bb1c15935efd95c6"
SECRET_KEY = "0cf9f4f337ebf747934c71d2f7e4c24e6cbefd4e64faa847"
ENDPOINT_URL = "https://s3.us-south.cloud-object-storage.appdomain.cloud"
BUCKET_NAME = "sample-testing-data"

# Connect to COS
cos = ibm_boto3.client(
    "s3",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    endpoint_url=ENDPOINT_URL,
    config=Config(signature_version="s3v4")
)

# List all objects in the bucket
try:
    response = cos.list_objects_v2(Bucket=BUCKET_NAME)
    if "Contents" in response:
        print("Files/folders in bucket:")
        for obj in response["Contents"]:
            print(obj["Key"])
    else:
        print("Bucket is empty")

except Exception as e:
    print(f"Error listing bucket contents: {e}")
