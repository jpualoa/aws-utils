# Standard dependencies
import logging
from abc import ABC, abstractmethod
from io import BytesIO
from typing import Optional, Any

# External dependencies
import boto3
from boto3.resources.factory import ServiceResource
from PIL import Image

# Logger
log = logging.getLogger("awsutils.s3")


class S3ObjectReader(ABC):
    client: ServiceResource  # s3 resource for interacting with buckets

    @abstractmethod
    def read(self, *args, **kwargs) -> Any:
        """Reads data from an S3 bucket object"""
        pass


class S3ObjectWriter(ABC):
    client: ServiceResource  # s3 resource for interacting with buckets

    @abstractmethod
    def write(self, *args, **kwargs):
        """Writes data to an S3 bucket object"""
        pass


class ImageHandler(S3ObjectReader, S3ObjectWriter):
    def __init__(self, client: Optional[ServiceResource] = None):
        if client is None:
            client = boto3.resource('s3')
        self.client = client

    def read(self, bucket: str, key: str) -> Image:
        """Reads image data from a bucket and returns a PIL Image"""
        bucket = self.client.Bucket(bucket)
        obj = bucket.Object(key)
        # Read image data
        log.info("Reading data from bucket=%s, key=%s" % (bucket, key))
        img_data = obj.get().get('Body').read()
        # Construct Image
        img = Image.open(BytesIO(bytearray(img_data)))

        return img

    def write(self, bucket: str, key: str, image: Image, format: str = "png"):
        """Writes a PIL Image to an s3 bucket """
        file_stream = BytesIO()
        image.save(file_stream, format=format)
        obj = self.client.Object(bucket, key)
        response = obj.put(Body=file_stream.getvalue())

        return response


# Simple main test
if __name__ == "__main__":
    import argparse

    # Setup logging
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("bucket", type=str, help="""Bucket to read image from""")
    parser.add_argument("key", type=str, help="""Key of image file to read""")
    args = parser.parse_args()

    handler = ImageHandler()
    image = handler.read(bucket=args.bucket, key=args.key)

    image.show()
    exit()

