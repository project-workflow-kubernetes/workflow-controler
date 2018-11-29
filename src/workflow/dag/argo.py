import collections

import networkx as nx

from workflow.dag.dag_helpers import get_dag_inputs


def is_dependency_valid(dependencies):
    inputs_req = lambda x: 'inputs' in dependencies[d] and isinstance(x['inputs'], collections.Iterable)
    outputs_req = lambda x: 'outputs' in dependencies[d] and isinstance(x['outputs'], collections.Iterable)
    image_req = lambda x: 'image' in dependencies[d] and isinstance(x['image'], str)
    command_req = lambda x: 'command' in dependencies[d] and isinstance(x['command'], str)

    for d in dependencies:
        is_ok = inputs_req(dependencies[d]) and outputs_req(dependencies[d]) and image_req(dependencies[d]) or command_req(dependencies[d])

        if not is_ok:
            return False

    return True


def get_header(job_name, run_id, volume_name='minio-tmp', log_level='INFO'):

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


def get_template(job_name, run_id, task_name, container_id, command,
                 mount_path='/data'):

    return {'name': '{job}-{task}'.format(job=job_name, task=task_name),
            'container': {'image': container_id,
                          'env': [
                              {'name': 'LOG_LEVEL',
                               'value': '"{{workflow.parameters.log-level}}"'},
                              {'name': 'DATA_INPUT_PATH',
                               'valueFrom': {'configMapKeyRef':
                                             {'name': '{}-{}-config'.format(job_name, run_id),
                                              'key': 'data_input_path'}}},
                              {'name': 'DATA_OUTPUT_PATH',
                               'valueFrom': {'configMapKeyRef':
                                             {'name': '{}-{}-config'.format(job_name, run_id),
                                              'key': 'data_output_path'}}},
                              {'name': 'LOGS_OUTPUT_PATH',
                               'valueFrom': {'configMapKeyRef':
                                             {'name': '{}-{}-config'.format(job_name, run_id),
                                              'key': 'data_logs_path'}}},
                              {'name': 'METADATA_OUTPUT_PATH',
                               'valueFrom': {'configMapKeyRef':
                                             {'name': '{}-{}-config'.format(job_name, run_id),
                                              'key': 'data_metadata_path'}}}
                          ],
                          'imagePullPolicy': 'IfNotPresent',
                          'command': ['python', 'executor/src/executor/main.py', command],
                          'volumeMounts': [{'name': 'shared-volume', 'mountPath': mount_path}]
                          }
            }


def get_dag_template(job_name, task_name, dependencies):
    task = '{job}-{task}'.format(job=job_name, task=task_name)

    if dependencies:
        dependencies = ['{}-{}'.format(job_name, rename(d))
                        for d in dependencies]
        return {'name': task,
                'dependencies': dependencies,
                'template': task}
    else:
        return {'name': task,
                'template': task}


def rename(s):
    return s.replace('.', '-').replace('_', '-')


def get_data_argo(dependencies, tasks):
    edges, _ = get_dag_inputs(dependencies)
    dag = nx.DiGraph(edges)

    ancestors_operators = [[o for o in list(nx.ancestors(dag, t))
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

    header = get_header(job_name, run_id)

    templates = [get_template(rename(job_name),
                              run_id,
                              rename(k),
                              v['image'],
                              v['command'])
                 for k, v in data.items()]

    tasks = [get_dag_template(rename(job_name),
                              rename(k),
                              v['dependencies'])
             for k, v in data.items()]

    tasks = {'name': '{job}-{id}'.format(job=rename(job_name), id=run_id),
             'dag': {'tasks': tasks}}

    templates.append(tasks)

    argo_specs = header
    argo_specs['spec']['templates'] = templates

    return argo_specs
