import os


DEPENDENCIES_FILE = os.path.join(os.path.abspath(
    os.path.join(__file__, '../../../..')), 'dependencies.json')
RESOURCES_PATH = os.path.join(os.path.abspath(
    os.path.join(__file__, '../../../')), 'resources')

PERSISTENT_ADDR = os.environ['PERSISTENT_ADDR'] if os.environ.get(
    'PERSISTENT_ADDR', None) else 'localhost:9030'
TEMPORARY_ADDR = os.environ['TEMPORARY_ADDR'] if os.environ.get(
    'TEMPORARY_ADDR', None) else 'localhost:9060'
VOLUME_PATH = '/data'
ACCESS_KEY = os.environ['MINIO_ACCESS_KEY'] if os.environ.get(
    'MINIO_ACCESS_KEY', None) else 'minio'
SECRET_KEY = os.environ['MINIO_SECRET_KEY'] if os.environ.get(
    'MINIO_SECRET_KEY', None) else 'minio1234'
