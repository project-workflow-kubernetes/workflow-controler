# workflow-controler

[![Build Status](http://img.shields.io/travis/liabifano/ml-aws.svg?style=flat)](https://travis-ci.com/project-workflow-kubernetes/workflow-controler)


## Overview
`Workflow` is a service to be deployed in a kubernetes cluster (through [deploy](https://github.com/project-workflow-kubernetes/deploy)).

Its main responsabilities are:

- Verify what has been changed in a repository in github
- Identify what tasks will be impacted by those changes
- Create a subdag of the entire job which takes in account just tasks that need to be run
- Render the subdag into an `yaml` file that [argo](https://github.com/argoproj) will consume and spin tasks (pods) inside the cluster
- Save the final data in a persistent storage (through minio interface) with a unique id (commit sha)


The gif below shows a brief overview of all interactions inside the cluster.

![gif](images/workflow.gif)


## Endpoints

The JSON API has three endpoints, the `home` returns `200` if the service is up, `/run` and `/sync` deals with `POST` requests containing `job_name` and `job_url` which will return status `201` if everthing worked as expected.


If you are running a local kubernetes, you should submit your job at [http://localhost:8000/run](http://localhost:8000/run):

```python
import requests

HOST = 'http://localhost:8000/run'
data = {'job_name': 'job',
        'job_url': 'https://github.com/project-workflow-kubernetes/job-python.git'}

r = requests.post(HOST, json=data)
print(r.status_code)
print(r.reason)
print(r.text)
```

or

```bash
curl -H "Content-Type: application/json" -X POST -d '{"job_name": "job","job_url": "https://gitlab.com/liabifano/job.git"}' http://localhost:8000/run
```

### Sync

```python
import requests

url = "http://localhost:8000/sync"

payload = "{\n\t\"job_name\": \"job\",\n\t\"job_url\": \"https://gitlab.com/rusucosmin/job.git\"\n}"
headers = {
    'Content-Type': "application/json",
    'Cache-Control': "no-cache"
    }

response = requests.request("POST", url, data=payload, headers=headers)

print(response.text)
```

or

```bash
curl -X POST \
  http://localhost:8000/sync \
  -H 'Cache-Control: no-cache' \
  -H 'Content-Type: application/json' \
  -d '{
	"job_name": "job",
	"job_url": "https://gitlab.com/rusucosmin/job.git"
}'
```



## Troubleshooting and Development

```bash
make install
```
Create a virtual environment with all dependencies and `workflow` installed as developer mode

```bash
make test
```
Run tests inside virtual environment

```bash
make build
```
Build image

```bash
make push
```
Push image to `liabifano/workflow` repository (don't forget to have `$DOCKER_USERNAME` and `$DOCKER_PASSWORD` set in your environment).

```bash
make local-run
```
You must have the port `8000` free, a minio application running at [http://localhost:9060](http://localhost:9060) (simulates persistent storage) and a folder `resources` (simulates temporary storage) in the root of this directory.


## Project structure

#### `src/workflow/controlers.py`
Contains functions `register_job` (will register for the first time a job) and `runner` (will check if `dependencies.yaml`, `commit hash`, `folders` are valid and submit to argo). Those function will be called by the endpoint `/run` in the file `endpoints.py`.

#### `src/workflow/data.py`
Contains functions to move data around, check if data is valid, get state of persistent storage, check changes in new repositories.

### `src/workflow/dd.py`
Contains functions to check if images have changed and we need to rerun the workflows.

#### `src/workflow/dag.py`
Contains all functions to deal with dependencies of a job. The tests are available in `src/test/test_dag.py`.


#### `src/workflow/argo.py`
Contains functions to render the resulting dag (subdag) in a format that `argo` is able to understand and execute the tasks in a certain order. The tests are available in `src/test/test_argo.py`.


#### `src/workflow/argo_handler.sh`
Script to send template to `argo` and wait until the `tasks` are done.


