# https://google-cloud-python.readthedocs.io/en/stable/storage/client.html

import logging, time, tempfile
from datetime import datetime, timezone
from google.cloud import storage

from cloud_common.cc.google import env_vars

# Storage client for Google Cloud
storage_client = storage.Client(env_vars.cloud_project_id)

DEBIAN_PACKAGE_BUCKET = 'openag-v1-debian-packages'
IMAGE_BUCKET = 'openag-v1-images'

URL_TEMPLATE = 'https://console.cloud.google.com/storage/browser/{}?project=openag-v1'

#------------------------------------------------------------------------------
def get_latest_debian_package_from_storage():
    return "deprecated" # no longer using debian packages


#------------------------------------------------------------------------------
def get_latest_backup_from_storage():
    try:
        buckets = list(storage_client.list_buckets(prefix='openag-v1-backup-'))
        return buckets[-1].name
    except:
        logging.error('no backup buckets.')
    return None # no data


#------------------------------------------------------------------------------
def get_images_URL_from_storage():
    return URL_TEMPLATE.format(IMAGE_BUCKET)


#------------------------------------------------------------------------------
def delete_files_over_two_hours_old(bucket_name):
    # Remove any files in the uploads bucket that are over 2 hours old
    now = datetime.now(timezone.utc) # use same TZ as storage
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    for blob in blobs:
        time_created = blob.time_created # datetime or None
        delta = now - time_created
        if delta.total_seconds() >= 2 * 60 * 60:
            try:
                blob.delete()
            except:
                pass 
            logging.info(f'storage.delete_files_over_two_hours_old: '
                    f'Removing stale file={blob.path}')


#------------------------------------------------------------------------------
# https://google-cloud-python.readthedocs.io/en/stable/storage/buckets.html
# Copy a file from one storage bucket to another.
# Then delete the file from the source bucket.
# Returns the public URL in the new location, or None for error.
def moveFileBetweenBuckets(src_bucket, dest_bucket, file_name):
    try:
        src = storage_client.get_bucket(src_bucket)
        dest = storage_client.get_bucket(dest_bucket)

        # get image in source bucket
        src_image = src.get_blob(file_name)
        if src_image is None:
            logging.error('storage.moveFileBetweenBuckets file {} ' \
                    'not found in bucket {}'.format(file_name, src_bucket))
            return None
    
        # copy image to dest bucket
        dest_image = src.copy_blob(src_image, dest)
        dest_image.make_public() # bucket is already public, just for safety 

        # delete the src image
        src_image.delete() # throws an exception, but still works. WTF?
    except:
        pass

    # return the new public url
    return dest_image.public_url


#------------------------------------------------------------------------------
# Download and save a file to the provided file obj.
def downloadFile(fp, bucket_name: str, file_name: str) -> bool:
    try:
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.get_blob(file_name)
        if blob is None:
            logging.error(f'storage.downloadFile file {file_name} ' \
                    f'not found in bucket {bucket_name}')
            return False
        blob.download_to_file(fp)
        return True
    except Exception as e:
        logging.error(f'storage.downloadFile {e}')
        return False


#------------------------------------------------------------------------------
# Upload a file from the provided file obj.
# Returns the public URL for success or None for error.
def uploadFile(fp, bucket_name: str, file_name: str, content_type: str = 'image/png') -> bool:
    try:
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_name) # make a new blob
        blob.upload_from_file(fp, rewind=True, content_type=content_type)
        logging.debug(f'storage.uploadFile {file_name} to {blob.public_url}')
        return blob.public_url
    except Exception as e:
        logging.error(f'storage.uploadFile {e}')
        return None


#------------------------------------------------------------------------------
# Save the image bytes to a file in cloud storage.
# The cloud storage bucket we are using allows "allUsers" to read files.
# Return the public URL to the file in a cloud storage bucket.
# Note: this is only used by the deprecated code.
def saveFile(varName, imageType, imageBytes, deviceId ):

    bucket = storage_client.get_bucket(env_vars.cs_bucket)
    filename = '{}_{}_{}.{}'.format( deviceId, varName,
        time.strftime( '%FT%XZ', time.gmtime() ), imageType )
    blob = bucket.blob( filename ) # make a new blob

    content_type = 'image/{}'.format( imageType )

    blob.upload_from_string( imageBytes, content_type=content_type )
    logging.info( "storage.saveFile: image saved to %s" % \
            blob.public_url )
    return blob.public_url


# ------------------------------------------------------------------------------
# Check if the file is in the uploads bucket yet (can take a bit for file to
# show up in the bucket).
# Returns True or False.
def isUploadedImageInBucket(file_name, src_bucket_name):

    src_bucket = storage_client.get_bucket(src_bucket_name)
    src_image = src_bucket.get_blob(file_name)
    if src_image is None:
        logging.debug("storage.isUploadedImageInBucket: file NOT in bucket={}"
                .format(file_name))
        return False # image not in bucket (yet)

    logging.debug("storage.isUploadedImageInBucket: file={} in bucket={}"
            .format(file_name, src_bucket_name))
    return True # image is here



