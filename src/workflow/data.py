import os
import shutil

from git import Repo
import yaml
import filecmp
import networkx as nx

from workflow import settings as s
from workflow import dag, argo


def create_or_clean(paths):
    for p in paths:
        if not os.path.exists(p):
            os.makedirs(p)
        else:
            shutil.rmtree(p)


def download(old_code_path,
             new_code_path,
             old_data_path,
             new_data_path,
             old_code_url,
             new_code_url):

    create_or_clean([old_code_path, new_code_path,
                     old_data_path, new_data_path])

    Repo.clone_from(old_code_url, old_code_path)
    Repo.clone_from(new_code_url, new_code_path)

    # TODO: download files with minio client
    # TODO: move files to temporary folder


def get_changes(dependencies, new_code_path, old_code_path, src):
    changed_files = {}

    for f in dependencies.keys():
        new_path = os.path.join(new_code_path, src, f)
        old_path = os.path.join(old_code_path, src, f)
        changed_files[f] = not filecmp.cmp(new_path, old_path)

    inputs = [x['inputs'] for x in dependencies.values()]
    inputs = list(set([y for x in inputs for y in x]))
    for i in inputs:
        changed_files[i] = False

    return changed_files
