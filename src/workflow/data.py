import os
import shutil

from git import Repo
import yaml
import filecmp
import networkx as nx

from workflow import settings as s


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


def get_changes(dependencies, new_path, old_path, code_path='code', data_path='data'):
    data_files = [(d, os.path.join(old_path, data_path, d),
                   os.path.join(new_path, data_path, d))
                  for d in dependencies.values()]

    code_files = [(d, os.path.join(old_path, code_path, d),
                   os.path.join(new_path, code_path, d))
                  for d in dependencies.keys()]

    files = data_files + code_files

    changed_files = []
    for d, old, new in files:
        if not filecmp.cmp(old, new):
            changed_files.append(d)

    return changed_files
