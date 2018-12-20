import os
from os.path import join, isdir, exists, isfile
import shutil
import operator

import filecmp
import yaml

from datetime import datetime

from git import Repo

from workflow import settings, utils, dag


def get_inputs(dependencies):
    """Get all inputs of all tasks

    Args:
        dependencies: dictionary where keys are scripts names (or tasks)
                      and values are dictionaries with keys `inputs`
                      and `outputs` of each task

    Returns:
        List of string with all inputs of a job

    """
    inputs = []

    for i in dependencies:
        inputs += dependencies[i]['inputs']

    return inputs


def get_lookup_paths(dependencies, commit_hash, tmp_path,
                     repo_code_path, repo_data_path='data',
                     minio_code_path='code', minio_data_path='data'):
    """Get lookup of path between temporary and persistent repository

    Args:
        dependencies: dictionary where keys are scripts names (or tasks)
                      and values are dictionaries with keys `inputs`
                      and `outputs` of each task
        commit_hash: string with whole sha of a commit
        tmp_path: string with temporary path
        repo_code_path: string with code's path in the repository
        repo_data_path: string with data's path in the repository
        minio_code_path: string with code's path in the persistent storage
        minio_data_path: string with data's path in the persistent storage

    Returns:
       List of tuples with origin path (temporary) and destination path (persistent) of each file in the job

    """

    lookup = {}
    tmp_code_path = join(tmp_path, repo_code_path)
    tmp_data_path = join(tmp_path, repo_data_path)
    minio_code_path = join(commit_hash, minio_code_path)
    minio_data_path = join(commit_hash, minio_data_path)
    relevants = dag.get_all_files(dependencies)

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
    """True if repository have all required folders and `dependencies.yaml` file, false otherwise"""
    if not isdir(join(path, code_path)):
        return False

    if not isdir(join(path, data_path)):
        return False

    if not exists(join(path, 'dependencies.yaml')):
        return False

    return True


def is_valid_request(request_json):
    """True if the request has `job_name` and `job_url`, false otherwise"""
    REQUIRED_KEYS = ['job_name', 'job_url']

    if not set(request_json.keys()).issubset(set(REQUIRED_KEYS)):
        raise KeyError('Some information is missing')


def tmp_to_persistent(bucket, job_name, lookup_paths):
    """Move files from temporary storage to persistent"""
    for tmp_file, minio_file in lookup_paths.items():
        bucket.upload_file(tmp_file, minio_file)


def get_latest_path(all_commits):
    """Get newest / last run of the job which was registered"""
    return max(all_commits.items(), key=operator.itemgetter(1))[0]


def get_changed_files(minioClient, job_name, latest_commit,
                      repo_code_path, repo_data_path='data', tmp_path=settings.VOLUME_PATH,
                      minio_code_path='code', minio_data_path='data'):
    """Compare the new submission with the lastest one and look for changed files

    Args:
        minioClient: connection boto object
        job_name: string with job name
        latest_commit: string with sha of latest commit
        dependencies: dictionary where keys are scripts names (or tasks)
                      and values are dictionaries with keys `inputs`
                      and `outputs` of each task
        commit_hash: string with whole sha of a commit
        tmp_path: string with temporary path
        repo_code_path: string with code's path in the repository
        repo_data_path: string with data's path in the repository
        minio_code_path: string with code's path in the persistent storage
        minio_data_path: string with data's path in the persistent storage

    Returns:
       Dependencies dictionary and list of files what were modified (operator and data)

    """

    copied_path = join(tmp_path, job_name, 'new', 'tmp')
    utils.create_or_delete([copied_path])

    minioClient.Bucket(job_name).download_file(join(latest_commit, 'dependencies.yaml'),
                                               join(copied_path, 'dependencies.yaml'))

    dependencies_equal = filecmp.cmp(join(copied_path, 'dependencies.yaml'),
                                     join(tmp_path, job_name, 'new', 'dependencies.yaml'))

    with open(join(tmp_path, job_name, 'new', 'dependencies.yaml'), 'r') as stream:
        dependencies = yaml.load(stream)

    if not dependencies_equal:
        changed_files = list(dependencies.keys())

    else:
        changed_files = []
        for f in dependencies.keys():
            minioClient.Bucket(job_name).download_file(join(latest_commit, minio_code_path, f),
                                                       join(copied_path, f))
            are_equal = filecmp.cmp(join(copied_path, f), join(
                tmp_path, job_name, 'new', repo_code_path, f))

            if not are_equal:
                changed_files.append(f)

        for f in get_inputs(dependencies):
            minioClient.Bucket(job_name).download_file(join(latest_commit, minio_data_path, f),
                                                       join(copied_path, f))
            are_equal = filecmp.cmp(join(copied_path, f), join(
                tmp_path, job_name, 'new', repo_data_path, f))

            if not are_equal:
                changed_files.append(f)

    shutil.rmtree(copied_path)

    return dependencies, changed_files


def get_persistent_commits(minioClient, job_name):
    """Get all runs registered in the persistent storage

    Args:
        minioClient: connection boto object
        job_name: string with job name

    Returns:
      Dictionary where keys are the commits shas and the values are theirs timestamp

    """
    all_commits = {}

    my_bucket = minioClient.Bucket('job')
    folders = list(set([x.key.split('/')[0] for x in my_bucket.objects.all()]))

    for i in folders:
        commit_date_path = join(i, 'commit_date.txt')
        d = minioClient.Object(job_name, commit_date_path).get()[
            'Body'].read().decode('utf-8')
        all_commits[i] = datetime.strptime(d, '%Y-%m-%d %H:%M:%S')

    return all_commits


def get_persistent_state(minioClient, job_name, job_url,
                         prefix_tmp_path=settings.VOLUME_PATH):
    """Download data referent job_url from github and check if is already registered

    Args:
        minioClient: connection boto object
        job_name: string with job name
        job_url: string with job url in github
        prefix_tmp_path: string with temporary path

    Returns:
       True if run is not registred yet, false otherwise

    """

    all_commits = get_persistent_commits(minioClient, job_name)

    tmp_path = join(prefix_tmp_path, job_name)
    utils.create_or_delete([tmp_path])
    tmp_path = join(tmp_path, 'new')
    utils.create_or_delete([tmp_path])
    repo = Repo.clone_from(job_url, tmp_path)

    commit_hash = repo.commit().hexsha

    commit_date = (repo.commit()
                   .committed_datetime
                   .strftime("%Y-%m-%d %H:%M:%S"))
    with open(join(tmp_path, 'commit_date.txt'), "w") as f:
        f.write(commit_date)

    return commit_hash not in all_commits.keys(), commit_hash, all_commits
