import os

def valid_run_id(path, job_name, run_id):
    path = os.path.join(path, job_name)

    return not (run_id in os.listdir(path))
