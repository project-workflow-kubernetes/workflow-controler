import sys
import logging
from os.path import join
import shutil

from flask import request, Blueprint
from flask_api import status
from flask_jsontools import jsonapi
from flask import abort
from minio import Minio
import urllib3

import boto3
from botocore.client import Config


from workflow import settings as s
from workflow.dag import argo, dag_helpers
from workflow.data import bootstrap as b
from workflow.data import helpers as h


mod = Blueprint('endpoints', __name__)


httpClient = urllib3.ProxyManager(
    'https://proxy_host.sampledomain.com:8119/',
    timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
    retries=urllib3.Retry(
        total=10,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504]))

minioPersistent = Minio(s.PERSISTENT_ADDR,
                        access_key=s.ACCESS_KEY,
                        secret_key=s.SECRET_KEY,
                        secure=False)


@mod.route('/', methods=['GET'])
@jsonapi
def home():
    return status.HTTP_200_OK


@mod.route('/run', methods=['POST'])
@jsonapi
def run():

    s3 = boto3.resource('s3', endpoint_url='http://' + s.PERSISTENT_ADDR,
                        aws_access_key_id=s.ACCESS_KEY,
                        aws_secret_access_key=s.SECRET_KEY,
                        config=Config(signature_version='s3v4'),
                        region_name='us-east-1')

    request_json = request.json
    h.is_valid_request(request_json)
    job_name = request_json['job_name']
    job_url = request_json['job_url']

    if job_name not in [x.name for x in s3.buckets.all()]:
        print('register')
        b.register_job(s3,
                       job_name,
                       job_url,
                       join('src', job_name))

        # import pdb; pdb.set_trace()
        del s3

        return status.HTTP_201_CREATED


    else:
        valid_run, commit, all_commits = b.get_persistent_state(
            minioPersistent, job_name, job_url)
        valid_repo = h.is_valid_repository(
            join(s.VOLUME_PATH, job_name), join('src', job_name))

        if not valid_repo:
            shutil.rmtree(join(s.VOLUME_PATH, job_name))
            message = 'Invalid repository format, please check it in `{}`'.format(
                'URL')
            logging.error(message)
            abort(500, message)
        if not valid_run:
            shutil.rmtree(join(s.VOLUME_PATH, job_name))
            message = 'Invalid run `{}`, please update repository of `{}`'.format(
                commit, job_url)
            logging.error(message)
            abort(500, message)

        latest_commit = h.get_latest_path(all_commits)
        dependencies, changed_files = h.get_changed_files(minioPersistent,
                                                          job_name, latest_commit,
                                                          join('src', job_name))

        print(changed_files)

        dags = [dag_helpers.get_subdag(dependencies, x) for x in changed_files]
        tasks = dag_helpers.get_merged_tasks(dags)
        data_to_argo = argo.get_data_argo(dependencies, tasks)
        inputs_to_run = dag_helpers.get_required_data(dependencies, tasks)
        dag_to_argo = argo.get_argo_spec(job_name, commit, data_to_argo)

        print(inputs_to_run)
        print(dag_to_argo)

        # if not validation.valid_run_id(s.PERSISTENT_STORAGE,
        #                            request_json['job_name'],
        #                            request_json['run_id']):
        #     raise KeyError('run_id is not valid')

        # dag.generate_yaml(request_json['old_path_code'],
        #               request_json['new_path_code'],
        #               request_json['src'],
        #               request_json['job_name'],
        #               request_json['run_id'])

        # return status.HTTP_201_CREATED
