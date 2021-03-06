import os


DEPENDENCIES_FILE = os.path.join(os.path.abspath(os.path.join(__file__, '../../../..')), 'dependencies.json')
RESOURCES_PATH = os.path.join(os.path.abspath(os.path.join(__file__, '../../../')), 'resources')

PERSISTENT_ADDR = os.path.join('http://', os.environ['PERSISTENT_ADDR']
                               if os.environ.get('PERSISTENT_ADDR', None) else 'localhost:9060')
VOLUME_PATH = os.environ['DATA_PATH'] if os.environ.get('DATA_PATH', None) else RESOURCES_PATH

ACCESS_KEY = os.environ['MINIO_ACCESS_KEY'] if os.environ.get('MINIO_ACCESS_KEY', None) else 'minio'
SECRET_KEY = os.environ['MINIO_SECRET_KEY'] if os.environ.get('MINIO_SECRET_KEY', None) else 'minio1234'

REDIS_HOST = os.environ['REDIS_HOST'] if os.environ.get('REDIS_HOST', None) else 'localhost'
REDIS_PORT = os.environ['REDIS_PORT'] if os.environ.get('REDIS_PORT', None) else '6379'
