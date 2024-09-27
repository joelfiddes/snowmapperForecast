import logging

import boto3
from botocore.exceptions import ClientError

SNOW_MODEL = "joel-snow-model"
SNOW_MODEL_BUCKET = "snow-model-data-source"
PARAMETERS = ["HS", "SWE", "ROF", "HS24"]


def get_file_path(date: str, parameter: str, forecast: bool = False):
    if not forecast:
        return f"{SNOW_MODEL}/{parameter}/{date[:4]}/{date[:6]}/{parameter}_{date}.nc"
    return f"{SNOW_MODEL}/forecast/{parameter}/{date[:4]}/{date[:6]}/{parameter}_{date}.nc"


def upload_snow_model_to_s3(
    file: str,
    date: str,
    parameter: str,
    bucket_name=SNOW_MODEL_BUCKET,
    forecast: bool = False,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
) -> bool:
    """
    Upload a snow model file to an S3 bucket
    :param file: File to upload
    :param date: Date of the file in the format YYYYMMDD
    :param parameter: Parameter of the file, either HS or SWE
    :param bucket_name: Bucket to upload to
    :param aws_access_key_id: AWS access key
    :param aws_secret_access_key: AWS secret access key
    :param forecast: If the file is a forecast file
    """
    if parameter not in PARAMETERS:
        raise ValueError(f"Parameter {parameter} not in {PARAMETERS}!")
    file_path = get_file_path(date, parameter, forecast=forecast)
    return upload_file(
        file,
        bucket_name=bucket_name,
        object_name=file_path,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )


def upload_file(
    file_name: str,
    bucket_name: str,
    object_name=None,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket_name: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :param aws_access_key_id: AWS access key
    :param aws_secret_access_key: AWS secret access key
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    try:
        _ = s3_client.upload_file(file_name, bucket_name, object_name)
    except ClientError as e:
        logging.getLogger().error(e)
        return False
    return True
