import sys
import logging
from os.path import join
import shutil

from flask import request, Blueprint
from flask_api import status
from flask_jsontools import jsonapi
from flask import abort
from minio import Minio


from workflow import settings as s
from workflow.dag import argo, dag_helpers
from workflow.data import bootstrap as b
from workflow.data import helpers as h
from workflow import data_handling


mod = Blueprint('endpoints', __name__)


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
    request_json = request.json
    h.is_valid_request(request_json)
    job_name = request_json['job_name']
    job_url = request_json['job_url']

    if not minioPersistent.bucket_exists(job_name):
        b.register_job(minioPersistent,
                       job_name,
                       job_url,
                       join('src', job_name))

        return status.HTTP_201_CREATED

    else:

        valid_run, commit, all_commits = b.get_persistent_state(minioPersistent, job_name, job_url)
        print('1')
        valid_repo = h.is_valid_repository(join(s.VOLUME_PATH, job_name), join('src', job_name))

        print('2')
        if not valid_repo:
            shutil.rmtree(join(s.VOLUME_PATH, job_name))
            message = 'Invalid repository format, please check it in `{}`'.format('URL')
            logging.error(message)
            abort(500, message)
        print('3')
        if not valid_run:
            shutil.rmtree(join(s.VOLUME_PATH, job_name))
            message = 'Invalid run `{}`, please update repository of `{}`'.format(commit, job_url)
            logging.error(message)
            abort(500, message)

        latest_commit = h.get_latest_path(all_commits)
        print('4')
        dependencies, changed_files = h.get_changed_files(minioPersistent,
                                                          job_name, latest_commit,
                                                          join('src', job_name))

        print(changed_files)




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
