import logging
from os import listdir
from os.path import join, isfile
import shutil

from git import Repo
import yaml

from workflow.data import helpers as h
from workflow.dag import dag_helpers as d
from workflow import settings as s


def register_job(minioClient, job_name, job_url, repo_code_path,
                 minio_code_path='code', data_path='data',
                 prefix_tmp_path=s.VOLUME_PATH):

    minioClient.make_bucket(job_name)

    tmp_path = join(prefix_tmp_path, job_name)
    tmp_code_path = join(tmp_path, repo_code_path)
    tmp_data_path = join(tmp_path, data_path)

    h.create_or_delete([tmp_path])

    repo = Repo.clone_from(job_url, tmp_path)

    commit_date = (repo.commit()
                   .committed_datetime
                   .strftime("%Y-%m-%d %H:%M:%S"))
    with open(join(tmp_path, 'commit_date.txt'), "w") as f:
        f.write(commit_date)

    commit_hash = repo.commit().hexsha

    minio_code_path = join(commit_hash, minio_code_path)
    minio_data_path = join(commit_hash, data_path)

    if not h.is_valid_repository(tmp_path, repo_code_path, data_path):
        raise ValueError('{} in {} is not valid'.format(job_name, job_url))

    with open(join(tmp_path, 'dependencies.yaml'), 'r') as stream:
        dependencies = yaml.load(stream)

    relevants = d.get_all_files(dependencies)

    [minioClient.fput_object(job_name,
                             join(minio_code_path, f),
                             join(tmp_code_path, f))
     for f in listdir(tmp_code_path)
     if isfile(join(tmp_code_path, f)) and f in relevants]

    [minioClient.fput_object(job_name,
                             join(minio_data_path, f),
                             join(tmp_data_path, f))
     for f in listdir(tmp_data_path)
     if isfile(join(tmp_data_path, f)) and f in relevants]

    minioClient.fput_object(job_name,
                            join(commit_hash, 'dependencies.yaml'),
                            join(tmp_path, 'dependencies.yaml'))

    minioClient.fput_object(job_name,
                            join(commit_hash, 'commit_date.txt'),
                            join(tmp_path, 'commit_date.txt'))

    logging.warning('The job `{}` was sucefully registered in `{}`'.format(
        job_name, commit_hash))

    shutil.rmtree(tmp_path)




