import os

import networkx as nx
import yaml

from workflow import argo, data
from workflow import settings as s


DEPENDENCIES_FILE = os.path.join(os.path.abspath(
    os.path.join(__file__, '../../../..')), 'dependencies.json')
RESOURCES_PATH = os.path.join(os.path.abspath(
    os.path.join(__file__, '../../../')), 'resources')


def get_all_files(dependencies):
    all_scripts = list(dependencies.keys())
    all_inputs = [n for m in [x['inputs']
                              for x in dependencies.values()] for n in m]
    all_outputs = [n for m in [x['outputs']
                               for x in dependencies.values()] for n in m]

    return list(set(all_scripts + all_outputs + all_inputs))


def build_DAG(tasks):
    edges = []
    attrs = {}

    for t in tasks:
        task = tasks[t]
        edges += [(input_, t) for input_ in task['inputs']]
        edges += [(t, output_) for output_ in task['outputs']]

        attrs[t] = {'type': 'operator'}
        attrs.update(dict([[x, {'type': 'data'}]
                           for x in task['inputs'] + task['outputs']]))

    return edges, attrs


def is_DAG_valid(dag):
    # TODO: add more test such as no missing nodes and no nodes alone
    return nx.is_directed_acyclic_graph(dag)


def create_subgraph(G, node):
    edges = nx.dfs_successors(G, node)
    nodes = []

    for k, v in edges.items():
        nodes.extend([k])
        nodes.extend(v)

    return G.subgraph(nodes)


def get_next_tasks(dag, changed_step):
    sub_dag = create_subgraph(dag, changed_step)

    sorted_sub_dag = nx.lexicographical_topological_sort(sub_dag)
    data_sub_dag = sub_dag.nodes(data=True)

    pendent_tasks = [node for node in sorted_sub_dag
                     if data_sub_dag[node]['type'] == 'operator']

    return pendent_tasks


def get_dag(dependencies, changed_file):
    if changed_file not in get_all_files(dependencies):
        raise KeyError('{file} is not a valid file in this job'
                       .format(file=changed_file))

    edges, nodes_attr = build_DAG(dependencies)
    dag = nx.DiGraph(edges)
    nx.set_node_attributes(dag, nodes_attr)

    if not is_DAG_valid(dag):
        raise Exception('Not valid DAG, check your dependencies.json file')

    return create_subgraph(dag, changed_file)


def get_pendent_tasks(dag):
    sorted_sub_dag = nx.lexicographical_topological_sort(dag)
    data_sub_dag = dag.nodes(data=True)

    pendent_tasks = [node for node in sorted_sub_dag
                     if data_sub_dag[node]['type'] == 'operator']

    return pendent_tasks



def generate_yaml(old_code_url,
                  new_code_url,
                  src,
                  job_name,
                  run_id):

    old_code_path = os.path.join(s.ARGO_VOLUME, 'old_code')
    new_code_path = os.path.join(s.ARGO_VOLUME, 'new_code')
    old_data_path = os.path.join(s.ARGO_VOLUME, 'old_data')
    new_data_path = os.path.join(s.ARGO_VOLUME, 'new_data')
    new_dependencies_path = os.path.join(new_code_path, 'dependencies.yaml')
    # old_dependencies_path = os.path.join(old_code_path, 'dependencies.yaml')


    data.download(old_code_url, new_code_url,
                  old_code_path, new_code_path,
                  old_data_path, new_data_path)

    # dependencies_same = filecmp.cmp(new_dependencies_path, old_dependencies_path)

    with open(new_dependencies_path, 'r') as stream:
        dependencies = yaml.load(stream)

    changed_files = data.get_changes(dependencies, new_code_path,
                                     old_code_path, src)

    changed_files = [x for x in changed_files.keys() if changed_files[x]]

    dags = [get_dag(dependencies, x) for x in changed_files]

    final_dag = nx.compose_all(dags)

    next_tasks = get_pendent_tasks(final_dag)
    requeried_inputs = dependencies[next_tasks[0]]['inputs']

    data_to_run = {}
    for t in next_tasks:
        data_to_run[t] = {'image': dependencies[t]['image'],
                          'command': dependencies[t]['command']}

    yaml_file = argo.build_argo_yaml(next_tasks, data_to_run, job_name, run_id)

    yaml_file_path = os.path.join(s.ARGO_VOLUME, "dag-{job_name}-{id}.yaml".format(job_name=job_name, id=run_id))
    inputs_file_path = os.path.join(s.ARGO_VOLUME, "inputs-{job_name}-{id}.txt".format(job_name=job_name, id=run_id))

    text_file = open(yaml_file_path, "w")
    text_file.write(yaml_file)
    text_file.close()

    with open(inputs_file_path, 'w') as f:
        for item in requeried_inputs:
            f.write("%s\n" % item)
