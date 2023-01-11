from google.cloud import storage
import logging
import os

CACHE_CONTROL = os.getenv("cache_control")


def need_update(bucket_name, blob_name):
    """
    Checks if Cache-Control metadata needs to be updated or not.
    :param bucket_name: Bucket Name.
    :param blob_name: File Name.
    :return: True/False (Boolean)
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(blob_name)

    if blob.cache_control is None:
        return True
    else:
        return False


def update_cache_control(bucket, obj):
    """
    Updates the Cache-Control metadata of the object.
    :param bucket: Bucket Name
    :param obj: Object Name
    :return: None.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket)
    blob = bucket.get_blob(obj)
    blob.cache_control = CACHE_CONTROL
    blob.patch()
    logging.info("Cache-Control updated for: {}/{}".format(obj, bucket))


def run(event, context):
    """
    Main function of the solution. Orchestrates other function.
    """
    bucket_name = event['bucket']
    file_name = event['name']
    logging.info("Processing Cache-Control check for: {}/{}".format(file_name, bucket_name))
    if need_update(bucket_name, file_name):
        update_cache_control(bucket_name, file_name)
    else:
        print("Cache-Control already updated.")
