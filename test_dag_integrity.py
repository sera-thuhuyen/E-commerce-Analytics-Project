import unittest
from airflow.models import DagBag

class TestDagIntegrity(unittest.TestCase):
    def setUp(self):
        # We point to the folder containing the DAGs
        self.dagbag = DagBag(dag_folder='airflow/dags', include_examples=False)

    def test_dag_loaded(self):
        """Test that the DAG is loaded without import errors."""
        dag = self.dagbag.get_dag(dag_id='ecommerce_analytics')
        self.assertIsNotNone(dag, "DAG 'ecommerce_analytics' not found")
        self.assertEqual(len(self.dagbag.import_errors), 0, f"Import errors: {self.dagbag.import_errors}")

    def test_dag_structure(self):
        """Test the tasks and their dependencies."""
        dag = self.dagbag.get_dag(dag_id='ecommerce_analytics')
        tasks = dag.tasks
        task_ids = set(t.task_id for t in tasks)
        expected_task_ids = {'check_spark_master', 'run_pyspark_extraction', 'verify_parquet_output'}
        
        self.assertEqual(task_ids, expected_task_ids, f"Expected tasks {expected_task_ids}, but got {task_ids}")
        
        # Check dependencies: check_spark >> run_extraction >> verify_files
        check_spark = dag.get_task('check_spark_master')
        run_extraction = dag.get_task('run_pyspark_extraction')
        verify_files = dag.get_task('verify_parquet_output')
        
        self.assertIn(run_extraction.task_id, [t.task_id for t in check_spark.downstream_list])
        self.assertIn(verify_files.task_id, [t.task_id for t in run_extraction.downstream_list])

if __name__ == '__main__':
    unittest.main()
