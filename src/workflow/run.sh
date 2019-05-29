#!/usr/bin/env bash

set -eo pipefail

gunicorn --bind 0.0.0.0:8000 \
         --log-level debug \
         --timeout 300 \
         --graceful-timeout 300 \
         --reload \
         -k eventlet manage:app
