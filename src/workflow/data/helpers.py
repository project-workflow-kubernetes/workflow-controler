import os
from os.path import join, isdir, exists
import shutil


def create_or_delete(paths):
    for p in paths:
        if not exists(p):
            os.makedirs(p)
        else:
            shutil.rmtree(p)


def is_valid_repository(path, code_path, data_path):
    if not isdir(join(path, code_path)):
        return False

    if not isdir(join(path, data_path)):
        return False

    if not exists(join(path, 'dependencies.yaml')):
        return False

    return True


def is_valid_request(request_json):
    REQUIRED_KEYS = ['job_name', 'job_url']

    if not set(request_json.keys()).issubset(set(REQUIRED_KEYS)):
        raise KeyError('Some information is missing')
