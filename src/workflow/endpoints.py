import sys
import logging


from flask import request, Blueprint
from flask_api import status
from flask_jsontools import jsonapi

from workflow import settings as s
from workflow import validation, dag


mod = Blueprint('endpoints', __name__)


@mod.route('/', methods=['GET'])
@jsonapi
def home():
    return status.HTTP_200_OK


@mod.route('/run/', methods=['POST'])
@jsonapi
def run():
    REQUIRED_KEYS = ['old_path_data', 'new_path_data',
                     'old_path_code', 'new_path_code',
                     'src', 'job_name', 'run_id']

    request_json = request.json
    print(request_json)

    print(list(request_json.keys()))

    logging.error(request_json.keys())

    if set(request_json.keys()) > set(REQUIRED_KEYS):
        raise KeyError('Some information is missing')

    if not validation.valid_run_id(s.PERSISTENT_STORAGE,
                                   request_json['job_name'],
                                   request_json['run_id']):
        raise KeyError('run_id is not valid')

    dag.generate_yaml(request_json['old_path_code'],
                      request_json['new_path_code'],
                      request_json['src'],
                      request_json['job_name'],
                      request_json['run_id'])

    # TODO: move files

    return status.HTTP_201_CREATED
