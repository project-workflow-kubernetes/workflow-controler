import os
from os.path import join, isdir, exists, isfile
import shutil


def create_or_destroy(path):
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        shutil.rmtree(path)
        os.makedirs(path)


def create_or_delete(paths):
    for p in paths:
        if not exists(p):
            os.makedirs(p)
        else:
            shutil.rmtree(p)


def create_or_clean(paths):
    for p in paths:
        if exists(p):
            shutil.rmtree(p)
        os.makedirs(p)


def rename(s):
    return s.replace('.', '-').replace('_', '-')
