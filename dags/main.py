from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import ( PythonOperator )

# Define the default arguments for the DAG
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

# Create the DAG instance
dag = DAG(
    'simple_etl_dag',
    default_args=default_args,
    schedule_interval=None,  # You can set a schedule interval here if needed
    catchup=False,
)

# Define the Python functions for ETL
def extract_data():
    # This function simulates extracting data from a source (e.g., CSV file)
    data = ['Row 1', 'Row 2', 'Row 3']
    return data

def transform_data(data):
    # This function adds a prefix to each line of data
    transformed_data = [f'Processed: {line}' for line in data]
    return transformed_data

def load_data(data):
    for line in data:
        print(line)

# Define the tasks using PythonOperators
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=[extract_task.output],
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_args=[transform_task.output],
    provide_context=True,
    dag=dag,
)

# Define the task dependencies
extract_task >> transform_task >> load_task
