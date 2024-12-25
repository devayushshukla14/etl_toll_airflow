from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments
default_args = {
    'owner': 'dummy_owner',  
    'start_date': datetime.now(), 
    'email': ['dummy_email@example.com'], 
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id='ETL_toll_data_1',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily',  
    catchup=False,  
) as dag:

    # First task: Unzip data
    unzip_data = BashOperator(
        task_id='unzip_data',
        bash_command='tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/'
    )

    # Second task: Extract data from CSV file
    extract_data_from_csv = BashOperator(
        task_id='extract_data_from_csv',
        bash_command=""" 
            head -n 1 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv &&
            tail -n +2 /home/project/airflow/dags/finalassignment/vehicle-data.csv | cut -d"," -f1,2,3,4 >> /home/project/airflow/dags/finalassignment/csv_data.csv
        """
    )

    # Third task: Extract data from TSV file
    extract_data_from_tsv = BashOperator(
        task_id='extract_data_from_tsv',
        bash_command=""" 
            head -n 1 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/tsv_data.csv &&
            tail -n +2 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv | cut -d$'\\t' -f1,2,3 >> /home/project/airflow/dags/finalassignment/tsv_data.csv
        """
    )

    # Fourth task: Extract data from fixed-width file
    extract_data_from_fixed_width = BashOperator(
        task_id='extract_data_from_fixed_width',
        bash_command=""" 
            head -n 1 /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv &&
            tail -n +2 /home/project/airflow/dags/finalassignment/payment-data.txt | cut -c1-5,6-10 >> /home/project/airflow/dags/finalassignment/fixed_width_data.csv
        """
    )

    # Fifth task: Consolidate data into one CSV
    consolidate_data = BashOperator(
        task_id='consolidate_data',
        bash_command="""
            paste -d',' /home/project/airflow/dags/finalassignment/csv_data.csv \
                         /home/project/airflow/dags/finalassignment/tsv_data.csv \
                         /home/project/airflow/dags/finalassignment/fixed_width_data.csv \
                > /home/project/airflow/dags/finalassignment/extracted_data.csv
        """
    )

    # Sixth task: Transform the data
    transform_data = BashOperator(
        task_id='transform_data',
        bash_command=(
            'echo "Transforming vehicle_type to uppercase..."; '
            'awk -F, \'{ OFS=","; $4=toupper($4); print $0 }\' /home/project/airflow/dags/finalassignment/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv; '
            'echo "Transformation complete."'
        ),
        dag=dag,
    )


    # define the task pipeline
    unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
