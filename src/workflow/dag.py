import networkx as nx


def get_all_files(dependencies):
    all_scripts = list(dependencies.keys())
    all_inputs = [n for m in [x['inputs']
                              for x in dependencies.values()] for n in m]
    all_outputs = [n for m in [x['outputs']
                               for x in dependencies.values()] for n in m]

    return list(set(all_scripts + all_outputs + all_inputs))


def get_dag_inputs(tasks):
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


def get_subdag(dependencies, changed_file):
    if changed_file not in get_all_files(dependencies):
        raise KeyError('{file} is not a valid file in this job'
                       .format(file=changed_file))

    edges, nodes_attr = get_dag_inputs(dependencies)
    dag = nx.DiGraph(edges)
    nx.set_node_attributes(dag, nodes_attr)

    if not is_dag_valid(dag):
        raise Exception('Not valid DAG, check your dependencies.json file')

    return get_subgraph(dag, changed_file)


def is_dag_valid(dag):
    return nx.is_directed_acyclic_graph(dag)


def get_subgraph(G, node):
    edges = nx.dfs_successors(G, node)
    nodes = []

    for k, v in edges.items():
        nodes.extend([k])
        nodes.extend(v)

    return G.subgraph(nodes)


def get_next_tasks(dag, changed_step):
    sub_dag = get_subgraph(dag, changed_step)

    sorted_sub_dag = nx.lexicographical_topological_sort(sub_dag)
    data_sub_dag = sub_dag.nodes(data=True)

    pendent_tasks = [node for node in sorted_sub_dag
                     if data_sub_dag[node]['type'] == 'operator']

    return pendent_tasks


def get_merged_tasks(dags):
    dag = nx.compose_all(dags)
    sorted_sub_dag = nx.lexicographical_topological_sort(dag)
    data_sub_dag = dag.nodes(data=True)

    pendent_tasks = [node for node in sorted_sub_dag
                     if data_sub_dag[node]['type'] == 'operator']

    return pendent_tasks


def get_required_data(dependencies, tasks):
    all_inputs = [v['inputs'] for k, v in dependencies.items() if k in tasks]

    return list(set(sum(all_inputs, [])))
