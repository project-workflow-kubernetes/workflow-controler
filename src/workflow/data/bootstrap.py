import logging
from os.path import join
from datetime import datetime
import shutil

from git import Repo
import yaml
from retrying import retry

from workflow.data import helpers as h
from workflow import settings as s


def register_job(minioClient, job_name, job_url, repo_code_path,
                 repo_data_path='data', minio_code_path='code', mini_data_path='data',
                 prefix_tmp_path=s.VOLUME_PATH):

    minioClient.create_bucket(Bucket=job_name)

    tmp_path = join(prefix_tmp_path, job_name)

    h.create_or_delete([tmp_path])

    repo = Repo.clone_from(job_url, tmp_path)

    commit_date = (repo.commit()
                   .committed_datetime
                   .strftime("%Y-%m-%d %H:%M:%S"))
    with open(join(tmp_path, 'commit_date.txt'), "w") as f:
        f.write(commit_date)

    commit_hash = repo.commit().hexsha

    if not h.is_valid_repository(tmp_path, repo_code_path, repo_data_path):
        raise ValueError('{} in {} is not valid'.format(job_name, job_url))

    with open(join(tmp_path, 'dependencies.yaml'), 'r') as stream:
        dependencies = yaml.load(stream)

    lookup = h.get_lookup_paths(dependencies, commit_hash,
                                tmp_path, repo_code_path, repo_data_path)

    bucket = minioClient.Bucket(job_name)
    print('bucket')
    h.tmp_to_persistent(bucket, job_name, lookup)

    logging.warning('The job `{}` was sucefully registered in `{}`'.format(
        job_name, commit_hash))

    shutil.rmtree(tmp_path)


def get_persistent_commits(minioClient, job_name):
    all_commits = {}

    @retry(stop_max_attempt_number=5)
    def safe_list_objects(job_name):
        return minioClient.list_objects(job_name)

    for i in safe_list_objects(job_name):
        folder_name = i.object_name[0:-1]
        commit_date_path = join(i.object_name, 'commit_date.txt')
        d = minioClient.get_object(job_name, commit_date_path).data.decode()
        all_commits[folder_name] = datetime.strptime(d, '%Y-%m-%d %H:%M:%S')

    return all_commits


def get_persistent_state(minioClient, job_name, job_url, prefix_tmp_path=s.VOLUME_PATH):

    all_commits = get_persistent_commits(minioClient, job_name)

    tmp_path = join(prefix_tmp_path, job_name)
    h.create_or_delete([tmp_path])
    repo = Repo.clone_from(job_url, tmp_path)

    commit_hash = repo.commit().hexsha

    print('got persistent')

    return commit_hash not in all_commits.keys(), commit_hash, all_commits
