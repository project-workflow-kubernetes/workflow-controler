#!/usr/bin/env bash

set -eo pipefail

gunicorn --log-level debug --timeout 300 --graceful-timeout 300 -k eventlet manage:app
