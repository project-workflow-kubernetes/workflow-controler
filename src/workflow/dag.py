import networkx as nx
from workflow import dd

def get_all_files(dependencies):
    """Get all required files in a job

    Args:
        dependencies: dictionary where keys are scripts names (or tasks)
                      and values are dictionaries with keys `inputs`
                      and `outputs` of each task

    Returns:
        List with all scripts names, inputs and outputs

    """

    all_scripts = list(dependencies.keys())

    all_inputs = [n for m in [x['inputs']
                              for x in dependencies.values()] for n in m]
    all_outputs = [n for m in [x['outputs']
                               for x in dependencies.values()] for n in m]

    return list(set(all_scripts + all_outputs + all_inputs))


def get_dag_inputs(tasks):
    """Get edges and attributes to build a graph

    Args:
        tasks: dictionary where keys are scripts names (or tasks)
               and values are dictionaries with keys `inputs`
               and `outputs` of each task

    Returns:
        List of tuples representing all dependencies in `tasks` and a list
        dictionary where keys are the files in tasks and if it is `data` or `operator`

    """

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

def get_subdag_of_changed_images(dependencies, changed_tasks):
    all_scripts = list(dependencies.keys())

    edges, nodes_attr = get_dag_inputs(dependencies)
    dag = nx.DiGraph(edges)
    nx.set_node_attributes(dag, nodes_attr)

    if not is_dag_valid(dag):
        raise Exception('Not valid DAG, check your dependencies.json file')

    changed_dags = [get_subgraph(dag, changed_task) for changed_task in changed_tasks]
    return get_merged_tasks(changed_dags)

def get_subdag(dependencies, changed_file):
    """Get all the tasks and inputs dependencies related with the changed file

    Args:
        dependencies: dictionary where keys are scripts names (or tasks)
                      and values are dictionaries with keys `inputs`
                      and `outputs` of each task
        changed_file: string with a task name

    Returns:
        NetworkX graph object with a subgraph of dependencies related with
        the changed file

    """

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
    """Returns True is graph is DAG (Directed Acyclic Graph), False otherwise"""
    return nx.is_directed_acyclic_graph(dag)


def get_subgraph(G, node):
    """Returns a subgraph based on the given node"""
    edges = nx.dfs_successors(G, node)
    nodes = []

    for k, v in edges.items():
        nodes.extend([k])
        nodes.extend(v)

    return G.subgraph(nodes)


def get_next_tasks(dag, changed_step):
    """Get pendent tasks to run based on the changed step

    Args:
        dependencies: graph describing dependencies in the job
        changed_step: string with a task name

    Returns:
        List of string with name of pendent tasks

    """
    sub_dag = get_subgraph(dag, changed_step)

    sorted_sub_dag = nx.lexicographical_topological_sort(sub_dag)
    data_sub_dag = sub_dag.nodes(data=True)

    pendent_tasks = [node for node in sorted_sub_dag
                     if data_sub_dag[node]['type'] == 'operator']

    return pendent_tasks


def get_merged_tasks(dags):
    """Returns all pendent tasks based in a list of graphs"""
    dag = nx.compose_all(dags)
    sorted_sub_dag = nx.lexicographical_topological_sort(dag)
    data_sub_dag = dag.nodes(data=True)

    pendent_tasks = [node for node in sorted_sub_dag
                     if data_sub_dag[node]['type'] == 'operator']

    return pendent_tasks


def get_required_data(dependencies, tasks):
    """Returns a list of inputs required to run the pendent tasks"""
    all_inputs = [v['inputs'] for k, v in dependencies.items() if k in tasks]

    return list(set(sum(all_inputs, [])))
