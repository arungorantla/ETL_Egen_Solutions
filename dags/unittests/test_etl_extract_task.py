import unittest
from datetime import datetime
from airflow.models import DagBag, TaskInstance
from task_scripts.extract_task import Extract

class TestETLExtractTask(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag()
        self.dag_id = 'etl_covid_data_dag'
        self.task_name = 'extract'
        self.extract = Extract()

    def test_task_count(self):
        """Check task count of etl_covid_data_dag"""

        dag = self.dagbag.get_dag(self.dag_id)
        self.assertEqual(len(dag.tasks), 65)

    def test_contain_tasks(self):
        """Check task contains in etl_covid_data_dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        tasks = dag.tasks
        task_ids = list(map(lambda task: task.task_id, tasks))
        self.assertIn("extract", task_ids)

    def test_get_data_from_api(self):
        """Check the task dependencies of  getDataFromAPI in etl_covid_data_dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        extract_task = dag.get_task('extract')
        resp = self.extract.getDataFromAPI()
        self.assertIsNotNone(resp)
        self.assertEqual(type(resp), list)

    def test_get_us_daily_data(self):
        """Check the task dependencies of getDailyUSDataFromAPI in etl_covid_data_dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        extract_task = dag.get_task('extract')
        resp = self.extract.getDailyUSDataFromAPI()
        self.assertIsNotNone(resp)
        self.assertEqual(type(resp), list)

    def test_get_ny_daily_data(self):
        """Check the task dependencies of getDailyNyDataFromAPI in etl_covid_data_dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        extract_task = dag.get_task('extract')
        resp = self.extract.getDailyNyDataFromAPI()
        self.assertIsNotNone(resp)
        self.assertEqual(type(resp), list)

    def test_get_county_name(self):
        """Check the task dependencies of getCountyNamesFromAPI in etl_covid_data_dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        extract_task = dag.get_task('extract')
        resp = self.extract.getCountyNamesFromAPI()
        self.assertIsNotNone(resp)
        self.assertEqual(type(resp), list)

if __name__ == '__main__':
    unittest.main()
