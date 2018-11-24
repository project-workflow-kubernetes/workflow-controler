import os


PERSISTENT_ADDR = os.environ['PERSISTENT_ADDR'] if os.environ.get('PERSISTENT_ADDR', None) else 'localhost:9030'
TEMPORARY_ADDR = os.environ['TEMPORARY_ADDR'] if os.environ.get('TEMPORARY_ADDR', None) else 'localhost:9060'
ACCESS_KEY = os.environ['MINIO_ACCESS_KEY'] if os.environ.get('MINIO_ACCESS_KEY', None) else 'minio'
SECRET_KEY = os.environ['MINIO_SECRET_KEY'] if os.environ.get('MINIO_SECRET_KEY', None) else 'minio1234'
