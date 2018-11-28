import os
import shutil


def create_or_destroy(path):
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        shutil.rmtree(path)
        os.makedirs(path)


def rename(s):
    return s.replace('.', '-').replace('_', '-')