import os

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
