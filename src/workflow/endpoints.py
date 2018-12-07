import sys
import logging
from os.path import join
import shutil
import yaml
import time
import subprocess as sp

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

        del s3

        return status.HTTP_201_CREATED


    else:

        valid_run, commit, all_commits = b.get_persistent_state(s3, job_name, job_url)
        valid_repo = h.is_valid_repository(
            join(s.VOLUME_PATH, job_name, 'new'), join('src', job_name))

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
        dependencies, changed_files = h.get_changed_files(s3,
                                                          job_name, latest_commit,
                                                          join('src', job_name))

        logging.warning(changed_files)

        dags = [dag_helpers.get_subdag(dependencies, x) for x in changed_files]
        tasks = dag_helpers.get_merged_tasks(dags)
        data_to_argo = argo.get_data_argo(dependencies, tasks)
        inputs_to_run = dag_helpers.get_required_data(dependencies, tasks)
        dag_to_argo = argo.get_argo_spec(job_name, commit, data_to_argo)

        logging.warning(inputs_to_run)
        logging.warning(dag_to_argo)

        with open(join(s.VOLUME_PATH, job_name, 'new', 'dag.yaml'), 'w') as yaml_file:
            yaml.dump(dag_to_argo, yaml_file, default_flow_style=False)

        time.sleep(2*60)

        cmd = 'argo submit --watch {}/{}/new/dag.yaml'.format(s.VOLUME_PATH, job_name)

        process = sp.Popen(cmd,
                       stdin=sp.PIPE,
                       stdout=sp.PIPE,
                       stderr=sp.STDOUT,
                       close_fds=True,
                       shell=True)

        while process.poll() == 0:
            time.sleep(0.1)

        out, err = process.communicate()
        out = out.decode('utf-8').split('\n') if out else out
        err = err.decode('utf-8').split('\n') if err else err

        logging.error(out)

        del s3
