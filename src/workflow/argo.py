import collections

import networkx as nx

from workflow import utils, dag


def is_dependency_valid(dependencies):
    """Returns True if `dependencies.yaml` has a valid format, False otherwise"""

    inputs_req = lambda x: 'inputs' in dependencies[d] and isinstance(x['inputs'], collections.Iterable)
    outputs_req = lambda x: 'outputs' in dependencies[d] and isinstance(x['outputs'], collections.Iterable)
    image_req = lambda x: 'image' in dependencies[d] and isinstance(x['image'], str)
    command_req = lambda x: 'command' in dependencies[d] and isinstance(x['command'], str)

    for d in dependencies:
        is_ok = (inputs_req(dependencies[d])
                 and outputs_req(dependencies[d])
                 and image_req(dependencies[d])
                 and command_req(dependencies[d]))

        if not is_ok:
            return False

    return True


def get_header(job_name, run_id, volume_name='minio-tmp', log_level='INFO'):
    """Returns a dictionary with the header of file to be sent to argo"""
    return {'apiVersion': 'argoproj.io/v1alpha1',
            'kind': 'Workflow',
            'metadata': {'generateName': 'dag-{job}-{id}-'.format(job=job_name, id=run_id)},
            'spec': {'entrypoint': '{job}-{id}'.format(job=job_name, id=run_id),
                     'arguments': {'parameters': [{'name': 'log-level',
                                                   'value': 'INFO'}]},
                     'volumes': [{'name': 'shared-volume',
                                  'persistentVolumeClaim': {'claimName': volume_name}}]
                     }
            }


def get_template(job_name, run_id, task_name, image_name, image_id, command,
                 mount_path='/data'):
    """Returns a dictionary with template of tasks to be sent to argo"""
    return {'name': '{job}-{task}'.format(job=job_name, task=task_name),
            'container': {'image': '{}:{}'.format(image_name, image_id),
                          'env': [
                              {'name': 'LOG_LEVEL',
                               'value': '"{{workflow.parameters.log-level}}"'},
                              {'name': 'DATA_INPUT_PATH',
                               'value': '/data/{}/new/data'.format(job_name)},
                              {'name': 'DATA_OUTPUT_PATH',
                               'value': '/data/{}/new/data'.format(job_name)},
                              {'name': 'LOGS_OUTPUT_PATH',
                               'value': '/data/{}/new/data'.format(job_name)},
                              {'name': 'METADATA_OUTPUT_PATH',
                               'value': '/data/{}/new/data'.format(job_name)}
                          ],
                          'imagePullPolicy': 'IfNotPresent',
                          'command': ['python', 'executor/src/executor/main.py', command],
                          'volumeMounts': [{'name': 'shared-volume', 'mountPath': mount_path}]
                          }
            }


def get_dag_template(job_name, task_name, dependencies):
    """Return dictionary of dag templates to be sent to argo"""
    task = '{job}-{task}'.format(job=job_name, task=task_name)

    if dependencies:
        dependencies = ['{}-{}'.format(job_name, utils.rename(d))
                        for d in dependencies]
        return {'name': task,
                'dependencies': dependencies,
                'template': task}
    else:
        return {'name': task,
                'template': task}


def get_data_argo(dependencies, tasks):
    """Transform data to be consumed by `get_argo_spec`

    Args:
        dependencies: dictionary where keys are scripts names (or tasks)
                      and values are dictionaries with keys `inputs`
                      and `outputs` of each task
        tasks: list of string with tasks names

    Returns:
        Dictionary where the keys are tasks names and the values are dictionaries with `dependencies`, `command` and `image`

    """
    edges, _ = dag.get_dag_inputs(dependencies)
    dag_graph = nx.DiGraph(edges)

    ancestors_operators = [[o for o in list(nx.ancestors(dag_graph, t))
                            if o in dependencies.keys() and o in tasks]
                           for t in tasks]

    data = {}
    for i, t in enumerate(tasks):
        data[t] = {}
        data[t]['dependencies'] = ancestors_operators[i]
        data[t]['command'] = dependencies[t]['command']
        data[t]['image'] = dependencies[t]['image']

    return data


def get_argo_spec(job_name, run_id, data):
    """Returns full template to be sent to argo"""
    header = get_header(job_name, run_id)
    image_id = run_id[0:7]

    templates = [get_template(utils.rename(job_name),
                              run_id,
                              utils.rename(k),
                              v['image'],
                              image_id,
                              v['command'])
                 for k, v in data.items()]

    tasks = [get_dag_template(utils.rename(job_name),
                              utils.rename(k),
                              v['dependencies'])
             for k, v in data.items()]

    tasks = {'name': '{job}-{id}'.format(job=utils.rename(job_name), id=run_id),
             'dag': {'tasks': tasks}}

    templates.append(tasks)

    argo_specs = header
    argo_specs['spec']['templates'] = templates

    return argo_specs
