import boto3
import os
import kagglehub
from botocore.config import Config
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET, MINIO_BUCKET

def check_bucket(minio,bucket):
    names = []
    for b in minio.list_buckets().get("Buckets", []):
        names.append(b["Name"])
    if bucket not in names:
        minio.create_bucket(Bucket=bucket)    
def downloadDb(link):
    return kagglehub.dataset_download(link)
    
def loadDataIntoMinIO (path_file):
    with open(path_file, "r") as f:
        data_dir = f.read().strip()
        
    minio_client = boto3.client(
        's3',
        endpoint_url = MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET, 
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    check_bucket(minio_client,MINIO_BUCKET)
    
    for root,dirs,files in os.walk(data_dir):
        for fname in files:
            raw_fpath = os.path.join(root,fname)
            minio_path = os.path.relpath(raw_fpath,data_dir)
            with open (raw_fpath,'rb')as f:
                minio_client.upload_fileobj(f,MINIO_BUCKET,f"raw/{minio_path}")
                print(f'Da up: {minio_path}')
    print('Up thanh cong')
if __name__== "__main__":
    link = "olistbr/brazilian-ecommerce"
    data_dir = downloadDb(link)
    print("Downloaded to:", data_dir)
    with open("path.txt", "w", encoding="utf-8") as g:
        g.write(data_dir)
    loadDataIntoMinIO("path.txt")