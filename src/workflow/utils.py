import os
from os.path import exists
import shutil


def create_or_destroy(path):
    """Delete folders and create empty ones"""
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        shutil.rmtree(path)
        os.makedirs(path)


def create_or_delete(paths):
    """Delete folders if exists or create empty ones"""
    for p in paths:
        if not exists(p):
            os.makedirs(p)
        else:
            shutil.rmtree(p)


def rename(s):
    return s.replace('.', '-').replace('_', '-')
