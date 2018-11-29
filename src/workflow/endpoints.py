import sys
import logging
from os.path import join


from flask import request, Blueprint
from flask_api import status
from flask_jsontools import jsonapi
from minio import Minio

from workflow import settings as s
from workflow.data import bootstrap as b
from workflow.data import helpers as h
from workflow import validation, dag, data_handling


mod = Blueprint('endpoints', __name__)


minioPersistent = Minio(s.PERSISTENT_ADDR,
                        access_key=s.ACCESS_KEY,
                        secret_key=s.SECRET_KEY,
                        secure=False)


@mod.route('/', methods=['GET'])
@jsonapi
def home():
    return status.HTTP_200_OK


@mod.route('/run/', methods=['POST'])
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

    # else:

    #     if not validation.valid_run_id(s.PERSISTENT_STORAGE,
    #                                request_json['job_name'],
    #                                request_json['run_id']):
    #         raise KeyError('run_id is not valid')



    #     dag.generate_yaml(request_json['old_path_code'],
    #                   request_json['new_path_code'],
    #                   request_json['src'],
    #                   request_json['job_name'],
    #                   request_json['run_id'])


        return status.HTTP_201_CREATED
