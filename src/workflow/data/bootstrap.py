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

    my_bucket = minioClient.Bucket('job')
    folders = list(set([x.key.split('/')[0] for x in my_bucket.objects.all()]))

    for i in folders:
        commit_date_path = join(i, 'commit_date.txt')
        d = minioClient.Object(job_name, commit_date_path).get()['Body'].read().decode('utf-8')
        all_commits[i] = datetime.strptime(d, '%Y-%m-%d %H:%M:%S')

    return all_commits


def get_persistent_state(minioClient, job_name, job_url, prefix_tmp_path=s.VOLUME_PATH):

    all_commits = get_persistent_commits(minioClient, job_name)

    tmp_path = join(prefix_tmp_path, job_name)
    h.create_or_delete([tmp_path])
    tmp_path = join(tmp_path, 'new')
    h.create_or_delete([tmp_path])
    repo = Repo.clone_from(job_url, tmp_path)

    commit_hash = repo.commit().hexsha

    return commit_hash not in all_commits.keys(), commit_hash, all_commits
