import unittest

import networkx as nx

from workflow import dag as d


class TestDag(unittest.TestCase):
    dependencies = {'o1': {'inputs': ['d1', 'd2'],
                           'outputs': ['d3']},
                    'o2': {'inputs': ['d3', 'd4'],
                           'outputs': ['d5']},
                    'o3': {'inputs': ['d1', 'd5'],
                           'outputs': []},
                    'o4': {'inputs': [],
                           'outputs': ['d6']}}

    wrong_dependencies = {'o1': {'inputs': ['d1', 'd2'],
                          'outputs': ['d1']},
                          'o2': {'inputs': ['d3', 'd4'],
                                 'outputs': ['d5']}}


    def test_get_dag_inputs(self):
        expected_output = ([('d1', 'o1'),
                            ('d2', 'o1'),
                            ('o1', 'd3'),
                            ('d3', 'o2'),
                            ('d4', 'o2'),
                            ('o2', 'd5'),
                            ('d1', 'o3'),
                            ('d5', 'o3'),
                            ('o4', 'd6')],
                           {'o1': {'type': 'operator'},
                            'd1': {'type': 'data'},
                            'd2': {'type': 'data'},
                            'd3': {'type': 'data'},
                            'o2': {'type': 'operator'},
                            'd4': {'type': 'data'},
                            'd5': {'type': 'data'},
                            'o3': {'type': 'operator'},
                            'o4': {'type': 'operator'},
                            'd6': {'type': 'data'}})
        output = d.get_dag_inputs(self.__class__.dependencies)

        self.assertListEqual(output[0], expected_output[0])
        self.assertDictEqual(output[1], expected_output[1])


    def test_get_all_files(self):
        expected_output = ['d6', 'd1', 'o1', 'd3', 'o3', 'd5', 'd4', 'd2', 'o4', 'o2']
        output = d.get_all_files(self.__class__.dependencies)
        self.assertListEqual(sorted(output), sorted(expected_output))


    def test_is_dag_valid(self):
        edges, _ = d.get_dag_inputs(self.__class__.dependencies)
        dag = nx.DiGraph(edges)
        self.assertTrue(d.is_dag_valid(dag))

        edges, _ = d.get_dag_inputs(self.__class__.wrong_dependencies)
        dag = nx.DiGraph(edges)
        self.assertFalse(d.is_dag_valid(dag))


    def test_get_subgraph(self):
        edges, _ = d.get_dag_inputs(self.__class__.dependencies)
        dag = nx.DiGraph(edges)

        expected_output = ['o3', 'd5', 'o2']
        output = list(d.get_subgraph(dag, 'o2').nodes())
        self.assertListEqual(sorted(output), sorted(expected_output))

        edges, _ = d.get_dag_inputs(self.__class__.dependencies)
        dag = nx.DiGraph(edges)

        expected_output = ['o1', 'd3', 'o3', 'd5', 'd2', 'o2']
        output = list(d.get_subgraph(dag, 'd2').nodes())
        self.assertListEqual(sorted(output), sorted(expected_output))

        expected_output = ['d6', 'o4']
        output = list(d.get_subgraph(dag, 'o4').nodes())
        self.assertListEqual(sorted(output), sorted(expected_output))


    def test_get_next_tasks(self):
        edges, nodes = d.get_dag_inputs(self.__class__.dependencies)
        dag = nx.DiGraph(edges)
        nx.set_node_attributes(dag, nodes)

        expected_output = ['o1', 'o2', 'o3']
        output = d.get_next_tasks(dag, 'd1')
        self.assertListEqual(output, expected_output)

        expected_output = ['o1', 'o2', 'o3']
        output = d.get_next_tasks(dag, 'o1')
        self.assertListEqual(output, expected_output)

        expected_output = ['o2', 'o3']
        output = d.get_next_tasks(dag, 'd4')
        self.assertListEqual(output, expected_output)

        expected_output = []
        output = d.get_next_tasks(dag, 'd6')
        self.assertListEqual(output, expected_output)

        expected_output = ['o4']
        output = d.get_next_tasks(dag, 'o4')
        self.assertListEqual(output, expected_output)


    def test_get_merged_tasks(self):
        changed_files = ['o1', 'd3']
        dags = [d.get_subdag(self.__class__.dependencies, c) for c in changed_files]
        expected_output = ['o1', 'o2', 'o3']
        output = d.get_merged_tasks(dags)
        self.assertListEqual(output, expected_output)

        changed_files = ['d3']
        dags = [d.get_subdag(self.__class__.dependencies, c) for c in changed_files]
        expected_output = ['o2', 'o3']
        output = d.get_merged_tasks(dags)
        self.assertListEqual(output, expected_output)

        changed_files = ['o2', 'd4']
        dags = [d.get_subdag(self.__class__.dependencies, c) for c in changed_files]
        expected_output = ['o2', 'o3']
        output = d.get_merged_tasks(dags)
        self.assertListEqual(output, expected_output)

        changed_files = ['d1']
        dags = [d.get_subdag(self.__class__.dependencies, c) for c in changed_files]
        expected_output = ['o1', 'o2', 'o3']
        output = d.get_merged_tasks(dags)
        self.assertListEqual(output, expected_output)

        changed_files = ['d5', 'o4']
        dags = [d.get_subdag(self.__class__.dependencies, c) for c in changed_files]
        expected_output = ['o3', 'o4']
        output = d.get_merged_tasks(dags)
        self.assertListEqual(output, expected_output)

        changed_files = ['d6']
        dags = [d.get_subdag(self.__class__.dependencies, c) for c in changed_files]
        expected_output = []
        output = d.get_merged_tasks(dags)
        self.assertListEqual(output, expected_output)


    def test_get_required_data(self):
        expected_output = ['d1', 'd5']
        output = d.get_required_data(self.__class__.dependencies, ['o3', 'o4'])
        self.assertListEqual(sorted(output), sorted(expected_output))

        expected_output = []
        output = d.get_required_data(self.__class__.dependencies, ['o4'])
        self.assertListEqual(sorted(output), sorted(expected_output))


if __name__ == '__main__':
    unittest.main()
