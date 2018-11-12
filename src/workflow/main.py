import os
import json

import argparse
import networkx as nx

from workflow import argo


DEPENDENCIES_FILE = os.path.join(os.path.abspath(
    os.path.join(__file__, '../../../..')), 'dependencies.json')
RESOURCES_PATH = os.path.join(os.path.abspath(
    os.path.join(__file__, '../../../')), 'resources')


def get_all_files(dependencies):
    # TODO: fix for primary inputs like train.csv and test.csv
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


def next_tasks(dag, changed_step):
    sub_dag = create_subgraph(dag, changed_step)

    sorted_sub_dag = nx.lexicographical_topological_sort(sub_dag)
    data_sub_dag = sub_dag.nodes(data=True)

    pendent_tasks = [node for node in sorted_sub_dag
                     if data_sub_dag[node]['type'] == 'operator']

    return pendent_tasks


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("job_name", help="job_name", type=str)
    parser.add_argument("changed_file", help="changed file", type=str)
    parser.add_argument("id", help="run id", type=str)
    args = parser.parse_args()
    changed_file = args.changed_file
    job_name = args.job_name
    run_id = args.id

    dependencies = json.load(open(DEPENDENCIES_FILE))

    if changed_file not in get_all_files(dependencies):
        raise KeyError('{file} is not a valid file in this job'
                       .format(file=changed_file))

    edges, nodes_attr = build_DAG(dependencies)
    dag = nx.DiGraph(edges)
    nx.set_node_attributes(dag, nodes_attr)

    if not is_DAG_valid(dag):
        raise Exception('Not valid DAG, check your dependencies.json file')

    next_tasks = next_tasks(dag, changed_file)
    # TODO: remove the changed file from the list si jamais
    requeried_inputs = dependencies[next_tasks[0]]['inputs']

    data_to_run = {}

    for t in next_tasks:
        data_to_run[t] = {'image': dependencies[t]['image'],
                          'command': dependencies[t]['command']}

    yaml_file = argo.build_argo_yaml(next_tasks, data_to_run, job_name, run_id)

    # TODO: Change it do use yaml library, it is nasty
    yaml_file_path = os.path.join(RESOURCES_PATH, "dag-{job_name}-{id}.yaml".format(job_name=job_name, id=run_id))
    inputs_file_path = os.path.join(RESOURCES_PATH, "inputs-{job_name}-{id}.txt".format(job_name=job_name, id=run_id))

    text_file = open(yaml_file_path, "w")
    text_file.write(yaml_file)
    text_file.close()

    with open(inputs_file_path, 'w') as f:
        for item in requeried_inputs:
            f.write("%s\n" % item)
