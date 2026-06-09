import unittest
from airflow.models import DagBag


class TestDagIntegrity(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(dag_folder='airflow/dags', include_examples=False)

    def test_no_import_errors(self):
        """DAG file import được, không có lỗi syntax hay thiếu thư viện."""
        self.assertEqual(
            len(self.dagbag.import_errors), 0,
            f"Import errors: {self.dagbag.import_errors}"
        )

    def test_dag_loaded(self):
        """DAG tồn tại trong DagBag."""
        dag = self.dagbag.get_dag(dag_id='ecommerce_analytics_extraction_dag')
        self.assertIsNotNone(dag, "DAG 'ecommerce_analytics_extraction_dag' not found")

    def test_dag_has_correct_tasks(self):
        """Tất cả 6 extraction tasks + boundary tasks đều tồn tại."""
        dag = self.dagbag.get_dag(dag_id='ecommerce_analytics_extraction_dag')
        task_ids = set(t.task_id for t in dag.tasks)

        expected_tasks = {
            'start',
            'extract_orders',
            'extract_order_items',
            'extract_order_item_refunds',
            'extract_products',
            'extract_website_sessions',
            'extract_website_pageviews',
            'wave2_gate',
            'validate_extraction',
            'end',
        }
        self.assertEqual(task_ids, expected_tasks,
                         f"Task mismatch.\nExpected: {expected_tasks}\nGot: {task_ids}")

    def test_wave1_tasks_downstream_of_start(self):
        """Wave 1 tasks phải chạy ngay sau start."""
        dag = self.dagbag.get_dag(dag_id='ecommerce_analytics_extraction_dag')
        start_downstream = {t.task_id for t in dag.get_task('start').downstream_list}
        wave1 = {'extract_orders', 'extract_order_items',
                 'extract_order_item_refunds', 'extract_products'}
        for task_id in wave1:
            self.assertIn(task_id, start_downstream,
                          f"'{task_id}' phải là downstream của 'start'")

    def test_wave2_tasks_downstream_of_gate(self):
        """Wave 2 tasks phải chạy sau wave2_gate."""
        dag = self.dagbag.get_dag(dag_id='ecommerce_analytics_extraction_dag')
        gate_downstream = {t.task_id for t in dag.get_task('wave2_gate').downstream_list}
        wave2 = {'extract_website_sessions', 'extract_website_pageviews'}
        for task_id in wave2:
            self.assertIn(task_id, gate_downstream,
                          f"'{task_id}' phải là downstream của 'wave2_gate'")

    def test_validate_is_last_before_end(self):
        """validate_extraction phải chạy trước end."""
        dag = self.dagbag.get_dag(dag_id='ecommerce_analytics_extraction_dag')
        validate_downstream = {t.task_id for t in dag.get_task('validate_extraction').downstream_list}
        self.assertIn('end', validate_downstream,
                      "'end' phải là downstream của 'validate_extraction'")

    def test_no_cycles(self):
        """DAG không có circular dependency."""
        dag = self.dagbag.get_dag(dag_id='ecommerce_analytics_extraction_dag')
        # Airflow 2.x dùng topological_sort thay vì test_cycle
        # Nếu có cycle, topological_sort sẽ raise exception
        try:
            dag.topological_sort()
            cycle_detected = False
        except Exception:
            cycle_detected = True
        self.assertFalse(cycle_detected, "DAG có circular dependency!")


if __name__ == '__main__':
    unittest.main()
