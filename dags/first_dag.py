from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
        'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
)

# Define the Python function
def my_print_function():
    print("Hello from the Python Operator")

# Use the PythonOperator to execute the Python function
run_this = PythonOperator(
    task_id='print_the_context',
    python_callable=my_print_function,
    dag=dag,
)
    
# Using the >> operator, you can set the order of tasks
run_this
