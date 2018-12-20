import logging
import time
from os.path import join
import shutil
import subprocess as sp

from git import Repo
import yaml
from flask import abort

from workflow import settings, utils, data, dag, argo


def register_job(minioClient, job_name, job_url, repo_code_path,
                 repo_data_path='data', minio_code_path='code', mini_data_path='data',
                 prefix_tmp_path=settings.VOLUME_PATH):
    """Creates a bucket in the persistent storage and save job's data

    Args:
        minioClient: connection boto object
        job_name: string with job name
        job_url: string with job url in github
        prefix_tmp_path: string with temporary path
        repo_code_path: string with code's path in the repository
        repo_data_path: string with data's path in the repository
        minio_code_path: string with code's path in the persistent storage
        minio_data_path: string with data's path in the persistent storage

    Returns:


    """
    minioClient.create_bucket(Bucket=job_name)

    tmp_path = join(prefix_tmp_path, job_name)

    utils.create_or_delete([tmp_path])

    repo = Repo.clone_from(job_url, tmp_path)

    commit_date = (repo.commit()
                   .committed_datetime
                   .strftime("%Y-%m-%d %H:%M:%S"))
    with open(join(tmp_path, 'commit_date.txt'), "w") as f:
        f.write(commit_date)

    commit_hash = repo.commit().hexsha

    if not data.is_valid_repository(tmp_path, repo_code_path, repo_data_path):
        raise ValueError('{} in {} is not valid'.format(job_name, job_url))

    with open(join(tmp_path, 'dependencies.yaml'), 'r') as stream:
        dependencies = yaml.load(stream)

    lookup = data.get_lookup_paths(dependencies, commit_hash,
                                   tmp_path, repo_code_path, repo_data_path)

    bucket = minioClient.Bucket(job_name)
    data.tmp_to_persistent(bucket, job_name, lookup)

    logging.warning('The job `{}` was sucefully registered in `{}`'.format(
        job_name, commit_hash))

    shutil.rmtree(tmp_path)


def runner(minioClient, job_name, job_url, repo_code_path, repo_data_path='data'):
    """Check if the request is valid, build argo template, send it to argo and wait until it is done

    Args:
        minioClient: connection boto object
        job_name: string with job name
        job_url: string with job url in github
        prefix_tmp_path: string with temporary path
        repo_code_path: string with code's path in the repository
        repo_data_path: string with data's path in the repository

    Returns:


    """

    temp_path = join(settings.VOLUME_PATH, job_name, 'new')
    utils.create_or_delete([temp_path])

    valid_run, commit, all_commits = data.get_persistent_state(
        minioClient, job_name, job_url)
    valid_repo = data.is_valid_repository(
        join(settings.VOLUME_PATH, job_name, 'new'), join('src', job_name))

    if not valid_repo:
        shutil.rmtree(join(settings.VOLUME_PATH, job_name))
        message = 'Invalid repository format, please check it in `{}`'.format(
            'URL')
        logging.error(message)
        abort(500, message)
    if not valid_run:
        shutil.rmtree(join(settings.VOLUME_PATH, job_name))
        message = 'Invalid run `{}`, please update repository of `{}`'.format(
            commit, job_url)
        logging.error(message)
        abort(500, message)

    latest_commit = data.get_latest_path(all_commits)
    dependencies, changed_files = data.get_changed_files(minioClient,
                                                         job_name, latest_commit,
                                                         join('src', job_name))

    logging.warning(changed_files)

    dags = [dag.get_subdag(dependencies, x) for x in changed_files]
    tasks = dag.get_merged_tasks(dags)
    data_to_argo = argo.get_data_argo(dependencies, tasks)
    inputs_to_run = dag.get_required_data(dependencies, tasks)
    dag_to_argo = argo.get_argo_spec(job_name, commit, data_to_argo)

    logging.warning(inputs_to_run)
    logging.warning(dag_to_argo)

    with open(join(settings.VOLUME_PATH, job_name, 'new', 'dag.yaml'), 'w') as yaml_file:
        yaml.dump(dag_to_argo, yaml_file, default_flow_style=False)

    logging.error(
        '{}/{}/new/dag.yaml'.format(settings.VOLUME_PATH, job_name))

    cmd = 'bash argo_handler.sh -f {}/{}/new/dag.yaml -t 10m'.format(
        settings.VOLUME_PATH, job_name)

    process = sp.Popen(cmd,
                       stdin=sp.PIPE,
                       stdout=sp.PIPE,
                       stderr=sp.PIPE,
                       close_fds=True,
                       shell=True)

    while process.poll() == 0:
        time.sleep(0.1)

    out, err = process.communicate()
    out = out.decode('utf-8').split('\n') if out else out
    err = err.decode('utf-8').split('\n') if err else err

    logging.error(out)
    logging.error(err)

    if not err:
        looup_path = data.get_lookup_paths(dependencies, commit,
                                           join(settings.VOLUME_PATH,
                                                job_name, 'new'),
                                           repo_code_path='src/{}'.format(job_name))
        bucket = minioClient.Bucket(job_name)
        data.tmp_to_persistent(bucket, job_name, looup_path)
    else:
        logging.error(err)
        abort(500, err)
