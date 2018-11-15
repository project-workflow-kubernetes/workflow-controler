import os

RESOURCES_PATH = os.path.join(os.path.abspath(os.path.join(__file__, '../../..')), 'resources')

ARGO_VOLUME = os.environ['ARGO_PATH'] if os.environ.get('ARGO_PATH', None) else RESOURCES_PATH
TEMPORARY_STORAGE = os.environ['TEMPORARY_STORAGE_PATH'] if os.environ.get('TEMPORARY_STORAGE_PATH', None) else RESOURCES_PATH
PERSISTENT_STORAGE = os.environ['PERSISTENT_STORAGE_PATH'] if os.environ.get('PERSISTENT_STORAGE_PATH', None) else RESOURCES_PATH
