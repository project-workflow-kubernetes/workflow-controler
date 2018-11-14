from flask import request, Blueprint
from flask_api import status
from flask_jsontools import jsonapi
import sys


mod = Blueprint('endpoints', __name__)


@mod.route('/', methods=['GET'])
@jsonapi
def home():
    return status.HTTP_200_OK


@mod.route('/run/', methods=['POST'])
@jsonapi
def run():
    REQUIRED_KEYS = ['old_path_data', 'new_path_data',
                     'old_path_code', 'new_path_code']

    request_json = request.json
    print(request_json)

    print(list(request_json.keys()))

    if set(request_json.keys()) > set(REQUIRED_KEYS):
        raise KeyError('Some information is missing')

    # 1. github request to compare files
    # 2. s3 request (in this case to minios) to compare files
    # 3. run dag to get de max diff
    # 4. save the dag + inputs to be run in new minio bucket (temporary with run_id)
    # 5. call something to run the dag in kubernetes

    return status.HTTP_201_CREATED
