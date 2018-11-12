ARGO_HEADER = """
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: dag-{job_name}-{run_id}-
spec:
  entrypoint: {job_name}-{run_id}
  arguments:
    parameters:
    - name: log-level
      value: INFO
  volumes:
  - name: shared-volume
    persistentVolumeClaim:
      claimName: minio-pv-claim
  templates:"""


TEMPLATES = """
  - name: {job_name}-{task_name}
    container:
      image: {container_id}
      env:
        - name: LOG_LEVEL
          value: "{{workflow.parameters.log-level}}"
        - name: DATA_INPUT_PATH
          valueFrom:
            configMapKeyRef:
              name: {job_name}-{run_id}-config
              key: data_input_path
        - name: DATA_OUTPUT_PATH
          valueFrom:
            configMapKeyRef:
              name: {job_name}-{run_id}-config
              key: data_output_path
        - name: LOGS_OUTPUT_PATH
          valueFrom:
            configMapKeyRef:
              name: {job_name}-{run_id}-config
              key: data_logs_path
        - name: METADATA_OUTPUT_PATH
          valueFrom:
            configMapKeyRef:
              name: {job_name}-{run_id}-config
              key: data_metadata_path
      imagePullPolicy: IfNotPresent
      command: {executor}
      args: {command_to_run}
      volumeMounts:
        - name: shared-volume
          mountPath: /data"""

ARGO_DAG_HEADER = """
  - name: {job_name}-{run_id}
    dag:
      tasks:"""

FIRST_TASK = """
      - name: {job_name}-{task_name}
        template: {job_name}-{task_name}"""

TASKS = """
      - name: {job_name}-{task_name}
        dependencies: [{job_name}-{prev_task_name}]
        template: {job_name}-{task_name}"""


def build_argo_yaml(tasks_to_run, data_to_run, job_name, run_id):
    # TODO: find a better way but PYAML doesn't work as expected, it doesn't keep it in the same order
    header = ARGO_HEADER.format(job_name=job_name, run_id=run_id)

    templates = [TEMPLATES.format(task_name=t.replace('.', '-').replace('_', '-'),
                                  job_name=job_name,
                                  run_id=run_id,
                                  container_id=data_to_run[t]['image'],
                                  executor=['python', 'src/{job_name}/executor.py'.format(job_name=job_name)],
                                  command_to_run=[data_to_run[t]['command']])
                 for t in data_to_run]

    dag_header = ARGO_DAG_HEADER.format(job_name=job_name, run_id=run_id)

    first_task = FIRST_TASK.format(job_name=job_name,
                                   task_name=tasks_to_run[0].replace('.', '-').replace('_', '-'),
                                   run_id=run_id)

    dag = [TASKS.format(job_name=job_name,
                        task_name=tasks_to_run[i].replace('.', '-').replace('_', '-'),
                        prev_task_name=tasks_to_run[i-1].replace('.', '-').replace('_', '-'))
           for i in range(1, len(tasks_to_run))]

    return ''.join([header] + templates + [dag_header] + [first_task] + dag)
