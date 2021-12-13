import re
from google.cloud import storage


class GcsPath:
    """
    Modeling after GCS storage object, offering simplistic way of
        *) checking if the paths exists, and if exists,
        *) represent a file or a 'directory',
        *) ...
    """

    def __init__(self, gs_path: str):

        if not gs_path.startswith("gs://"):
            raise ValueError(f"Provided gs path isn't valid: {gs_path}")

        arr = re.sub("^gs://", '', gs_path).split('/')
        self.bucket = arr[0]
        self.prefix = '/'.join(arr[1:-1])
        self.file = arr[-1]

    def exists(self, client: storage.client.Client) -> bool:
        return self.is_file(client=client) or self.is_emulate_dir(client=client)

    def is_file(self, client: storage.client.Client) -> bool:
        return storage.Blob(bucket=client.bucket(self.bucket), name=f'{self.prefix}/{self.file}').exists(client)

    def is_emulate_dir(self, client: storage.client.Client) -> bool:
        if self.is_file(client=client):
            return False
        return any(True for _ in client.list_blobs(client.bucket(self.bucket), prefix=f'{self.prefix}/{self.file}'))