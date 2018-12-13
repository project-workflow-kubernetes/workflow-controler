# workflow-controler

[![Build Status](http://img.shields.io/travis/liabifano/ml-aws.svg?style=flat)](https://travis-ci.com/project-workflow-kubernetes/workflow-controler)


`Workflow` is a service to be deployed in a kubernetes cluster (through [deploy](https://github.com/project-workflow-kubernetes/deploy). Its main responsabilities are:

- Verify what has been changed in a repository in github
- Identify what tasks are impacted by those changes
- Create a subdag of the entire job which takes in account just tasks that need to be run
- Render the subdag into an `yaml` file that [argo](https://github.com/argoproj) can read
- Save the final data in a persistent storage with a unique id


The video below shows a brief overview of all interactions inside the cluster.

![gif](images/workflow.gif)


If you are running a local kubernetes, the endpoint to be reached with a `POST` request is [http://localhost:8000/run](http://localhost:8000/run) and it is a JSON API which expects `job_name` and `job_url` where `job_url` is the github with `https` url. For instance:

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
