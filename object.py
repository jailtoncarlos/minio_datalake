class MinIOObject:
    """
    Class to represent a MinIO object.

    Attributes:
    client: Instance of the MinIO client.
    bucket_name: Name of the bucket.
    object_name: Name of the object.
    """
    def __init__(self, client, bucket_name, object_name):
        self.client = client
        self.bucket_name = bucket_name
        self.object_name = object_name

    def get_object(self):
        """
        Get an object from the bucket.

        Returns:
        Object: Object from the bucket.
        """
        return self.client.get_object(self.bucket_name, self.object_name)

    def put_object(self, file_path):
        """
        Upload a file to the bucket.

        Parameters:
        file_path (str): Path to the file.
        """
        self.client.fput_object(self.bucket_name, self.object_name, file_path)
