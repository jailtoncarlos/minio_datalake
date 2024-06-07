class MinIOBucket:
    """
    Class to represent a MinIO bucket.

    Attributes:
    client: Instance of the MinIO client.
    bucket_name: Name of the bucket.
    """
    def __init__(self, client, bucket_name):
        self.client = client
        self.bucket_name = bucket_name

    def create_bucket(self):
        """
        Create a bucket if it does not exist.
        """
        self.client.make_bucket(self.bucket_name)

    def bucket_exists(self):
        """
        Check if the bucket exists.

        Returns:
        bool: True if the bucket exists, False otherwise.
        """
        return self.client.bucket_exists(self.bucket_name)

    def list_objects(self, prefix=None, recursive=False):
        """
        List objects in the bucket.

        Parameters:
        prefix (str): Prefix of the objects.
        recursive (bool): If True, list objects recursively.

        Returns:
        list: List of objects.
        """
        return self.client.client.list_objects(self.bucket_name, prefix=prefix, recursive=recursive)
