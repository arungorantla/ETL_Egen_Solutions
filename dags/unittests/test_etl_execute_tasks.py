import unittest
from datetime import datetime
from airflow.models import DagBag, TaskInstance

class TestETLExecuteTask(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag()
        self.dag_id = 'etl_covid_data_dag'

    def test_xcoms_extract(self):
        dag = self.dagbag.get_dag(self.dag_id)
        extract_task = dag.get_task('extract')
        transform_task = dag.get_task('transform')

        execution_date = datetime.now()

        extract_task_ti = TaskInstance(task=extract_task, execution_date=execution_date)
        context = extract_task_ti.get_template_context()
        extract_task.execute(context)

        transform_task_ti = TaskInstance(task=transform_task, execution_date=execution_date)

        result = transform_task_ti.xcom_pull(key="covid_test_data")
        self.assertIsNotNone(result)


    def test_xcom_transform(self):
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

        load_task_ti = TaskInstance(task=load_task, execution_date=execution_date)
        transformed_covid_test_data = transform_task_ti.xcom_pull(key="transformed_covid_test_data")
        transformed_ny_hospital_data = transform_task_ti.xcom_pull(key="transformed_ny_hospital_data")
        transformed_us_hospital_data = transform_task_ti.xcom_pull(key="transformed_us_hospital_data")

        self.assertIsNotNone(transformed_covid_test_data)
        self.assertIsNotNone(transformed_ny_hospital_data)
        self.assertIsNotNone(transformed_us_hospital_data)
        self.assertIn("Albany", transformed_covid_test_data)


if __name__ == '__main__':
    unittest.main()
