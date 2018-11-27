import unittest

import networkx as nx

from workflow import dag as d


class TestDag(unittest.TestCase):
    dependencies = {'o1': {'inputs': ['d1', 'd2'],
                           'outputs': ['d3']},
                    'o2': {'inputs': ['d3', 'd4'],
                           'outputs': ['d5']}}

    wrong_dependencies = {'o1': {'inputs': ['d1', 'd2'],
                          'outputs': ['d1']},
                          'o2': {'inputs': ['d3', 'd4'],
                                 'outputs': ['d5']}}


    def test_build_DAG(self):
        expected_output = ([('d1', 'o1'),
                            ('d2', 'o1'),
                            ('o1', 'd3'),
                            ('d3', 'o2'),
                            ('d4', 'o2'),
                            ('o2', 'd5')],
                           {'o1': {'type': 'operator'},
                            'o2': {'type': 'operator'},
                            'd1': {'type': 'data'},
                            'd2': {'type': 'data'},
                            'd3': {'type': 'data'},
                            'd4': {'type': 'data'},
                            'd5': {'type': 'data'}})
        output = d.build_DAG(self.__class__.dependencies)

        self.assertListEqual(output[0], expected_output[0])
        self.assertDictEqual(output[1], expected_output[1])


    def test_get_all_files(self):
        expected_output = ['o1', 'o2', 'd1', 'd2', 'd3', 'd4', 'd5']
        output = d.get_all_files(self.__class__.dependencies)
        self.assertListEqual(sorted(output), sorted(expected_output))


    def test_is_dag_valid(self):
        edges, _ = d.build_DAG(self.__class__.dependencies)
        dag = nx.DiGraph(edges)
        self.assertTrue(d.is_DAG_valid(dag))

        edges, _ = d.build_DAG(self.__class__.wrong_dependencies)
        dag = nx.DiGraph(edges)
        self.assertFalse(d.is_DAG_valid(dag))


    def test_create_subgraph(self):
        edges, _ = d.build_DAG(self.__class__.dependencies)
        dag = nx.DiGraph(edges)

        expected_output = ['o2', 'd5']
        output = list(d.create_subgraph(dag, 'o2').nodes())
        self.assertListEqual(sorted(output), sorted(expected_output))

        edges, _ = d.build_DAG(self.__class__.dependencies)
        dag = nx.DiGraph(edges)

        expected_output = ['o1', 'd3', 'd5', 'd2', 'o2']
        output = list(d.create_subgraph(dag, 'd2').nodes())
        self.assertListEqual(sorted(output), sorted(expected_output))


    def test_get_next_tasks(self):
        edges, nodes = d.build_DAG(self.__class__.dependencies)
        dag = nx.DiGraph(edges)
        nx.set_node_attributes(dag, nodes)

        expected_output = ['o1', 'o2']
        output = d.get_next_tasks(dag, 'd1')
        self.assertListEqual(sorted(output), sorted(expected_output))

        expected_output = ['o1', 'o2']
        output = d.get_next_tasks(dag, 'o1')
        self.assertListEqual(sorted(output), sorted(expected_output))

        expected_output = ['o2']
        output = d.get_next_tasks(dag, 'd4')
        self.assertListEqual(sorted(output), sorted(expected_output))

        expected_output = []
        output = d.get_next_tasks(dag, 'd5')
        self.assertListEqual(output, expected_output)


    def test_get_pendent_tasks(self):
        changed_files = ['o1', 'd3']
        dags = [d.get_dag(self.__class__.dependencies, c) for c in changed_files]
        expected_output = ['o1', 'o2']
        output = d.get_pendent_tasks(dags)
        self.assertListEqual(sorted(output), sorted(expected_output))

        changed_files = ['d3']
        dags = [d.get_dag(self.__class__.dependencies, c) for c in changed_files]
        expected_output = ['o2']
        output = d.get_pendent_tasks(dags)
        self.assertListEqual(sorted(output), sorted(expected_output))

        changed_files = ['o2', 'd4']
        dags = [d.get_dag(self.__class__.dependencies, c) for c in changed_files]
        expected_output = ['o2']
        output = d.get_pendent_tasks(dags)
        self.assertListEqual(sorted(output), sorted(expected_output))

        changed_files = ['d1']
        dags = [d.get_dag(self.__class__.dependencies, c) for c in changed_files]
        expected_output = ['o1', 'o2']
        output = d.get_pendent_tasks(dags)
        self.assertListEqual(sorted(output), sorted(expected_output))

        changed_files = ['d5']
        dags = [d.get_dag(self.__class__.dependencies, c) for c in changed_files]
        expected_output = []
        output = d.get_pendent_tasks(dags)
        self.assertListEqual(output, expected_output)




if __name__ == '__main__':
    unittest.main()
