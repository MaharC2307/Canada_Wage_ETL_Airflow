from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from stats_can import StatsCan 
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator



sc = StatsCan()

def extract_data():
    # Using the correct method to fetch the data
    df = sc.table_to_df("14-10-0206-01") 
    print(df)
    
    # Save the DataFrame to CSV
    df.to_csv('mydata.csv', index=False)
    print('DataFrame saved as mydata.csv!')
    


transform_data = {
    'existing_cluster_id': '',  # Replace with your actual existing cluster ID
    'notebook_task': {
        'notebook_path': '',  # Path to the Databricks notebook
    },
}


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 1),
}

# Define the DAG
with DAG(
    'wage_etl_pipeline',  # DAG name should be a string
    default_args=default_args,  # Use correct variable
    schedule_interval='@daily',  # Schedule interval as string
) as dag:
    
    # Task to extract data
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data
    )
    
    
    # Task to transform data
    transform_task = DatabricksSubmitRunOperator(
        task_id='transform_task',
        databricks_conn_id='databricks_default',  # Using the programmatically created connection
        json=transform_data,
        )
    
   
    extract_task  >> transform_task 
