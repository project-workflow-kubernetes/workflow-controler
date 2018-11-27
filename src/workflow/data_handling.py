import os
import operator
from datetime import datetime

from minio import Minio

import settings as s
import utils as u


minioPersistent = Minio(s.PERSISTENT_ADDR,
                        access_key=s.ACCESS_KEY,
                        secret_key=s.SECRET_KEY,
                        secure=False)


def latest_run(minioClient, bucket_name):

    if not minioClient.bucket_exists(bucket_name):
        raise Exception('You need to bootstrap the project first')

    folders = [f.object_name for f in minioClient.list_objects('job-python')]

    commit_dates = [(minioClient.get_object(
        'job-python', os.path.join(f, 'commit_time.txt')).data.decode()) for f in folders]
    commit_dates = dict(zip(folders, [datetime.strptime(
        c, '%Y-%m-%d %H:%M:%S') for c in commit_dates]))
    latest_commit = max(commit_dates.items(), key=operator.itemgetter(1))[0]

    return latest_commit[0:-1]


def copy_latest(minioClient, bucket_name, latest_commit,
                code_path='code', data_path='data', temporary_path=s.VOLUME_PATH):

    latest_path = os.path.join(temporary_path, bucket_name, 'latest-committed')
    temporary_code_path = os.path.join(latest_path, code_path)
    temporary_data_path = os.path.join(latest_path, data_path)

    minio_code_path = os.path.join(latest_commit, code_path)
    minio_data_path = os.path.join(latest_commit, data_path)

    [u.create_or_destroy(f) for f in [latest_path,
                                      temporary_code_path, temporary_code_path]]

    code_objs = [f.object_name for f in minioClient.list_objects(
        bucket_name, minio_code_path, recursive=True)]
    data_objs = [f.object_name for f in minioClient.list_objects(
        bucket_name, minio_data_path, recursive=True)]

    [minioClient.fget_object(bucket_name, d, os.path.join(
        temporary_data_path, d.split('/')[-1])) for d in data_objs]

    [minioClient.fget_object(bucket_name, d, os.path.join(
        temporary_code_path, d.split('/')[-1])) for d in code_objs]

    minioClient.fget_object(bucket_name,
                            os.path.join(latest_commit, 'dependencies.yaml'),
                            os.path.join(latest_path, 'dependencies.yaml'))


if __name__ == '__main__':
    commit = latest_run(minioPersistent, 'job-python')

    copy_latest(minioPersistent, 'job-python', commit)
