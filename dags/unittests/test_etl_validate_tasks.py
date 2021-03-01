import unittest
from datetime import datetime
from airflow.models import DagBag, TaskInstance

class TestETLValidateTask(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag()
        self.dag_id = 'etl_covid_data_dag'

    def test_task_count(self):

        dag = self.dagbag.get_dag(self.dag_id)
        self.assertEqual(len(dag.tasks), 65)

    def test_contain_tasks(self):

        dag = self.dagbag.get_dag(self.dag_id)
        tasks = dag.tasks
        task_ids = list(map(lambda task: task.task_id, tasks))
        self.assertIn("extract", task_ids)
        self.assertIn("transform", task_ids)
        self.assertIn("load", task_ids)

    def test_dependencies_of_transform_task(self):

        dag = self.dagbag.get_dag(self.dag_id)
        dummy_task = dag.get_task('transform')

        upstream_task_ids = list(map(lambda task: task.task_id, dummy_task.upstream_list))
        self.assertListEqual(upstream_task_ids, ["extract"])
        downstream_task_ids = list(map(lambda task: task.task_id, dummy_task.downstream_list))
        self.assertIn("load", downstream_task_ids)

    def test_dependencies_of_extract_task(self):

        dag = self.dagbag.get_dag(self.dag_id)
        hello_task = dag.get_task('extract')

        upstream_task_ids = list(map(lambda task: task.task_id, hello_task.upstream_list))
        self.assertListEqual(upstream_task_ids, [])
        downstream_task_ids = list(map(lambda task: task.task_id, hello_task.downstream_list))
        self.assertIn("transform", downstream_task_ids)
        self.assertEqual([], upstream_task_ids)

if __name__ == '__main__':
    unittest.main()
