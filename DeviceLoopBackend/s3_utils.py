# s3_utils.py
import boto3
from flask import current_app

def s3_client():
    return boto3.client("s3", region_name=current_app.config["AWS_REGION"])

def presign_put(key: str, content_type: str):
    return s3_client().generate_presigned_url(
        "put_object",
        Params={
            "Bucket": current_app.config["S3_UPLOADS_BUCKET"],
            "Key": key,
            "ContentType": content_type,
        },
        ExpiresIn=current_app.config["S3_PRESIGN_EXPIRE"],
    )

def presign_get(key: str):
    return s3_client().generate_presigned_url(
        "get_object",
        Params={
            "Bucket": current_app.config["S3_UPLOADS_BUCKET"],
            "Key": key,
        },
        ExpiresIn=current_app.config["S3_PRESIGN_EXPIRE"],
    )
