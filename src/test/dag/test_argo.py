import unittest

import networkx as nx

from workflow.dag import argo as a


class TestArgo(unittest.TestCase):
    dependencies = {'o1': {'inputs': ['d1', 'd2'],
                           'outputs': ['d3'],
                           'image': 'repo/image1:h1',
                           'command': 'run1'},
                    'o2': {'inputs': ['d3', 'd4'],
                           'outputs': ['d5'],
                           'image': 'repo/image2:h2',
                           'command': 'run1'},
                    'o3': {'inputs': ['d1', 'd5'],
                           'outputs': [],
                           'image': 'repo/image3:h3',
                           'command': 'run3'},
                    'o4': {'inputs': [],
                           'outputs': ['d6'],
                           'image': 'repo/image4:h4',
                           'command': 'run4'}}

    def test_get_data_argo(self):
        tasks = ['o2', 'o3']
        expected_output = {'o2': {'dependencies': [],
                                  'command': 'run1',
                                  'image': 'repo/image2:h2'},
                           'o3': {'dependencies': ['o2'],
                                  'command': 'run3',
                                  'image': 'repo/image3:h3'}}
        output = a.get_data_argo(self.__class__.dependencies, tasks)
        self.assertDictEqual(output, expected_output)

        tasks = ['o4']
        expected_output = {'o4': {'dependencies': [],
                                  'command': 'run4',
                                  'image': 'repo/image4:h4'}}
        output = a.get_data_argo(self.__class__.dependencies, tasks)
        self.assertDictEqual(output, expected_output)


    def test_rename(self):
        expected_output = 'my-file-py'
        output = a.rename('my_file.py')
        self.assertEqual(output, expected_output)


    def test_get_argo_spec(self):
        data =  a.get_data_argo(self.__class__.dependencies, ['o2', 'o3'])
        expected_output = {'apiVersion': 'argoproj.io/v1alpha1',
                           'kind': 'Workflow',
                           'metadata': {'generateName': 'dag-my-job-ed3795t-'},
                           'spec': {'entrypoint': 'my-job-ed3795t',
                                    'arguments': {'parameters': [{'name': 'log-level', 'value': 'INFO'}]},
                                    'volumes': [{'name': 'shared-volume',
                                                 'persistentVolumeClaim': {'claimName': 'minio-tmp'}}],
                                    'templates': [{'name': 'my-job-o2',
                                                   'container': {'image': 'repo/image2:h2',
                                                                 'env': [{'name': 'LOG_LEVEL',
                                                                          'value': '"{{workflow.parameters.log-level}}"'},
                                                                         {'name': 'DATA_INPUT_PATH',
                                                                          'valueFrom': {'configMapKeyRef': {'name': 'my-job-ed3795t-config',
                                                                                                            'key': 'data_input_path'}}},
                                                                         {'name': 'DATA_OUTPUT_PATH',
                                                                          'valueFrom': {'configMapKeyRef': {'name': 'my-job-ed3795t-config',
                                                                                                            'key': 'data_output_path'}}},
                                                                         {'name': 'LOGS_OUTPUT_PATH',
                                                                          'valueFrom': {'configMapKeyRef': {'name': 'my-job-ed3795t-config',
                                                                                                            'key': 'data_logs_path'}}},
                                                                         {'name': 'METADATA_OUTPUT_PATH',
                                                                          'valueFrom': {'configMapKeyRef': {'name': 'my-job-ed3795t-config',
                                                                                                            'key': 'data_metadata_path'}}}],
                                                                 'imagePullPolicy': 'IfNotPresent',
                                                                 'command': ['python', 'executor/src/executor/main.py', 'run1'],
                                                                 'volumeMounts': [{'name': 'shared-volume', 'mountPath': '/data'}]}},
                                                  {'name': 'my-job-o3',
                                                   'container': {'image': 'repo/image3:h3',
                                                                 'env': [{'name': 'LOG_LEVEL',
                                                                          'value': '"{{workflow.parameters.log-level}}"'},
                                                                         {'name': 'DATA_INPUT_PATH',
                                                                          'valueFrom': {'configMapKeyRef': {'name': 'my-job-ed3795t-config',
                                                                                                            'key': 'data_input_path'}}},
                                                                         {'name': 'DATA_OUTPUT_PATH',
                                                                          'valueFrom': {'configMapKeyRef': {'name': 'my-job-ed3795t-config',
                                                                                                            'key': 'data_output_path'}}},
                                                                         {'name': 'LOGS_OUTPUT_PATH',
                                                                          'valueFrom': {'configMapKeyRef': {'name': 'my-job-ed3795t-config',
                                                                                                            'key': 'data_logs_path'}}},
                                                                         {'name': 'METADATA_OUTPUT_PATH',
                                                                          'valueFrom': {'configMapKeyRef': {'name': 'my-job-ed3795t-config',
                                                                                                            'key': 'data_metadata_path'}}}],
                                                                 'imagePullPolicy': 'IfNotPresent',
                                                                 'command': ['python', 'executor/src/executor/main.py', 'run3'],
                                                                 'volumeMounts': [{'name': 'shared-volume', 'mountPath': '/data'}]}},
                                                  {'name': 'my-job-ed3795t',
                                                   'dag': {'tasks': [{'name': 'my-job-o2', 'template': 'my-job-o2'},
                                                                     {'name': 'my-job-o3',
                                                                      'dependencies': ['my-job-o2'],
                                                                      'template': 'my-job-o3'}]}}]}}
        output = a.get_argo_spec('my-job', 'ed3795t', data)
        self.assertDictEqual(output, expected_output)
