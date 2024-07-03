import os
import re


def split_file_path(filepath):
    base_dir, filename = os.path.split(filepath)
    file_name_without_extension, file_extension = os.path.splitext(filename)
    return base_dir, file_name_without_extension, file_extension


def split_minio_path(path: str) -> tuple:
    """
    Split the MinIO path into bucket name and object name.

    Parameters:
    path (str): MinIO path in the format '/bucket_name/object_name'.

    Returns:
    tuple: (bucket_name, object_name)
    """
    if not path.startswith('/'):
        raise ValueError("Path must start with '/'")
    parts = path[1:].split('/', 1)
    if len(parts) != 2:
        raise ValueError("Path must be in the format '/bucket_name/object_name'")
    return parts[0], parts[1]


class MinIOUtils:
    @staticmethod
    def validate_bucket_name(bucket_name: str) -> bool:
        """
        Validate the bucket name according to the provided rules
        https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html

        Parameters:
        bucket_name (str): Name of the bucket.

        Returns:
        bool: True if the bucket name is valid, False otherwise.
        """
        if not (3 <= len(bucket_name) <= 63):
            return False

        if not re.match(r'^[a-z0-9][a-z0-9.-]*[a-z0-9]$', bucket_name):
            return False

        if '..' in bucket_name:
            return False

        if re.match(r'^\d{1,3}(\.\d{1,3}){3}$', bucket_name):
            return False

        if bucket_name.startswith(('xn--', 'sthree-', 'sthree-configurator')):
            return False

        if bucket_name.endswith(('-s3alias', '--ol-s3')):
            return False

        return True

    @staticmethod
    def validate_object_name(object_name: str) -> bool:
        """
        Validate the object name according to the provided rules.

        Parameters:
        object_name (str): Name of the object.

        Returns:
        bool: True if the object name is valid, False otherwise.
        """
        # For now, assume object names just need to be non-empty.
        return bool(object_name)
