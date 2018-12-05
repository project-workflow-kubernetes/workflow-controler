import os
from os.path import join, isdir, exists, isfile
import shutil
import operator

from retrying import retry
import filecmp
import yaml

from workflow import settings as s
from workflow.dag import dag_helpers as d


def create_or_delete(paths):
    for p in paths:
        if not exists(p):
            os.mkdir(p)
        else:
            shutil.rmtree(p)


def get_inputs(dependencies):
    inputs = []

    for i in dependencies:
        inputs += dependencies[i]['inputs']

    return inputs


def get_lookup_paths(dependencies, commit_hash, tmp_path,
                     repo_code_path, repo_data_path='data',
                     minio_code_path='code', minio_data_path='data'):

    lookup = {}
    tmp_code_path = join(tmp_path, repo_code_path)
    tmp_data_path = join(tmp_path, repo_data_path)
    minio_code_path = join(commit_hash, minio_code_path)
    minio_data_path = join(commit_hash, minio_data_path)
    # TODO: this could return what is data and what is code
    relevants = d.get_all_files(dependencies)

    lookup[join(tmp_path, 'commit_date.txt')] = join(
        commit_hash, 'commit_date.txt')
    lookup[join(tmp_path, 'dependencies.yaml')] = join(
        commit_hash, 'dependencies.yaml')

    for f in os.listdir(tmp_data_path):
        file_path = join(tmp_data_path, f)
        if isfile(file_path) and f in relevants:
            lookup[file_path] = join(minio_data_path, f)

    for f in os.listdir(tmp_code_path):
        file_path = join(tmp_code_path, f)
        if isfile(file_path) and f in relevants:
            lookup[file_path] = join(minio_code_path, f)

    return lookup


def is_valid_repository(path, code_path, data_path='data'):
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


@retry(stop_max_attempt_number=5)
def tmp_to_persistent(minioClient, job_name, lookup_paths):

    @retry(stop_max_attempt_number=5)
    def safe_transfer(minio, path):
        minioClient.fput_object(job_name, minio, path)

    for tmp_file, minio_file in lookup_paths.items():
        safe_transfer(minio_file, tmp_file)


def get_latest_path(all_commits):
    return max(all_commits.items(), key=operator.itemgetter(1))[0]


def get_changed_files(minioClient, job_name, latest_commit,
                      repo_code_path, repo_data_path='data',  tmp_path=s.VOLUME_PATH,
                      minio_code_path='code', minio_data_path='data'):

    @retry(stop_max_attempt_number=3)
    def safe_get_minio(minio_path, local_path):
        minioClient.fget_object(job_name, minio_path, local_path)

    copied_path = join(tmp_path, job_name, 'tmp')

    safe_get_minio(join(latest_commit, 'dependencies.yaml'),
                   join(copied_path, 'dependencies.yaml'))

    dependencies_equal = filecmp.cmp(join(copied_path, 'dependencies.yaml'),
                                     join(tmp_path, job_name, 'dependencies.yaml'))

    with open(join(tmp_path, job_name, 'dependencies.yaml'), 'r') as stream:
            dependencies = yaml.load(stream)

    if not dependencies_equal:
        changed_files = list(dependencies.keys())

    else:
        changed_files = []
        for f in dependencies.keys():
            print(f)
            safe_get_minio(join(latest_commit, minio_code_path, f),
                           join(copied_path, f))
            are_equal = filecmp.cmp(join(copied_path, f), join(tmp_path, job_name, repo_code_path, f))

            if not are_equal:
                changed_files.append(f)

        for f in get_inputs(dependencies):
            print(f)
            safe_get_minio(join(latest_commit, minio_data_path, f),
                           join(copied_path, f))
            are_equal = filecmp.cmp(join(copied_path, f), join(tmp_path, job_name, repo_data_path, f))

            if not are_equal:
                changed_files.append(f)

    shutil.rmtree(copied_path)

    return dependencies, changed_files
