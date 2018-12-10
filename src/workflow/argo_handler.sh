#!/usr/bin/env bash

set -eo pipefail


while getopts ":f:t:" opt; do
  case $opt in
      f) FILE_PATH="$OPTARG";;
      t) TIMEOUT="$OPTARG";;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

submit-and-wait () {

    DAG_NAME=$(argo submit -n workflow ${FILE_PATH} | grep Name: | awk '{ print $2 }')

    argo wait -n workflow ${DAG_NAME} --request-timeout ${TIMEOUT}

    argo delete -n workflow ${DAG_NAME}

}

(submit-and-wait)
