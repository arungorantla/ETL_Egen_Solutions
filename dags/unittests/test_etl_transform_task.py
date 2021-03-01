import unittest
from datetime import datetime
from airflow.models import DagBag, TaskInstance
from task_scripts.transform_task import Transform

class TestETLTransformTask(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag()
        self.dag_id = 'etl_covid_data_dag'
        self.task_name = 'transform'
        self.transform = Transform()

    def test_task_count(self):
        """Check task count of etl_covid_data_dag"""

        dag = self.dagbag.get_dag(self.dag_id)
        self.assertEqual(len(dag.tasks), 65)

    def test_contain_tasks(self):
        """Check task contains in etl_covid_data_dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        tasks = dag.tasks
        task_ids = list(map(lambda task: task.task_id, tasks))
        self.assertIn("transform", task_ids)

    def test_covid_data_transform(self):
        """Check the task dependencies of transform_covid_test_data in etl_covid_data_dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        extract_task = dag.get_task('extract')
        transform_task = dag.get_task('transform')
        load_task = dag.get_task('transform')

        execution_date = datetime.now()

        extract_task_ti = TaskInstance(task=extract_task, execution_date=execution_date)
        context = extract_task_ti.get_template_context()
        extract_task.execute(context)

        transform_task_ti = TaskInstance(task=transform_task, execution_date=execution_date)
        context = transform_task_ti.get_template_context()
        transform_task.execute(context)

        transformed_covid_test_data = transform_task_ti.xcom_pull(key="transformed_covid_test_data")

        for county_names, data in transformed_covid_test_data.items():
            self.assertNotIn(" ", county_names)
            for row in data:
                self.assertIsNotNone(row[-1])


    def test_us_data_transform(self):
        """Check the task dependencies of transform_us_hospital_data in etl_covid_data_dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        extract_task = dag.get_task('extract')
        transform_task = dag.get_task('transform')

        execution_date = datetime.now()

        extract_task_ti = TaskInstance(task=extract_task, execution_date=execution_date)
        context = extract_task_ti.get_template_context()
        extract_task.execute(context)

        transform_task_ti = TaskInstance(task=transform_task, execution_date=execution_date)
        context = transform_task_ti.get_template_context()
        transform_task.execute(context)

        transformed_us_hospital_data = transform_task_ti.xcom_pull(key="transformed_us_hospital_data")

        for row in transformed_us_hospital_data:
            self.assertIsNotNone(row[0])

    def test_ny_data_transform(self):
        """Check the task dependencies of transform_ny_hospital_data in etl_covid_data_dag"""
        dag = self.dagbag.get_dag(self.dag_id)
        extract_task = dag.get_task('extract')
        transform_task = dag.get_task('transform')

        execution_date = datetime.now()

        extract_task_ti = TaskInstance(task=extract_task, execution_date=execution_date)
        context = extract_task_ti.get_template_context()
        extract_task.execute(context)

        transform_task_ti = TaskInstance(task=transform_task, execution_date=execution_date)
        context = transform_task_ti.get_template_context()
        transform_task.execute(context)

        transformed_ny_hospital_data = transform_task_ti.xcom_pull(key="transformed_ny_hospital_data")

        for row in transformed_ny_hospital_data:
            self.assertIsNotNone(row[0])


if __name__ == '__main__':
    unittest.main()
