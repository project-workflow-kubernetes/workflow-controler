from os.path import join

from flask import request, Blueprint
from flask_api import status
from flask_jsontools import jsonapi

import boto3
from botocore.client import Config

from workflow import settings, controlers, data


mod = Blueprint('endpoints', __name__)


@mod.route('/', methods=['GET'])
@jsonapi
def home():
    return status.HTTP_200_OK


@mod.route('/run', methods=['POST'])
@jsonapi
def run():
    """
       Checks if job is already registered. If the job is not registerd,
       it creates a new bucket with the `job_name` in the persistent storage.
       If the job is registered, it checks which files were changed and spins
       a job in argo with all required tasks

    """

    s3 = boto3.resource('s3', endpoint_url=settings.PERSISTENT_ADDR,
                        aws_access_key_id=settings.ACCESS_KEY,
                        aws_secret_access_key=settings.SECRET_KEY,
                        config=Config(signature_version='s3v4'),
                        region_name='us-east-1')

    request_json = request.json
    data.is_valid_request(request_json)
    job_name = request_json['job_name']
    job_url = request_json['job_url']

    if job_name not in [x.name for x in s3.buckets.all()]:
        controlers.register_job(s3,
                                job_name,
                                job_url,
                                join('src', job_name))

        del s3

        return status.HTTP_201_CREATED

    else:

        controlers.runner(s3, job_name, job_url, join('src', job_name))

        del s3

        return status.HTTP_201_CREATED
