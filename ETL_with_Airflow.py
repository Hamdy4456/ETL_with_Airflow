#import libraries
from airflow.operators.bash_operator import BashOperator
from airflow import DAG 
from airflow.utils.dates import days_ago
from datetime import timedelta

# Task: DAG arguments
default_args = {
    'owner':'Hamdy',
    'start_date':days_ago(0),
    'email':['ahmed0hamdi0ashri@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':1,
    'retry_delay':timedelta(minutes = 1),
}

# Task: DAG Defination
dag = DAG(
    'ETL_with_Airflow',
    default_args=default_args,
    description='Apache Airflow ETL Project',
    schedule_interval=timedelta(days=7),
)

# Task: unzip file
data = BashOperator(
    task_id = 'unzip_file',
    bash_command = 'tar -xzf tolldata.tgz',
    dag = dag,
)

# Task: extract data from csv
csv_extract = BashOperator(
    task_id = 'extract_from_csv',
    bash_command = 'cut -d"," -f1-4 vehicle-data.csv > csv_data.csv',
    dag = dag,
)

# Task: extract data from tsv
tsv_extract = BashOperator(
    task_id = 'extract_from_tsv',
    bash_command = 'cut -f5-7 tollplaza-data.tsv > tsv_data.csv',
    dag = dag,
)

# Task: extract data from fixed width
fixed_width_extract = BashOperator(
    task_id = 'extract_from_fixed_width',
    bash_command = 'awk "NF{print $(NF-1),$NF}"  OFS="\t"  payment-data.txt > fixed_width_data.csv',
    dag = dag,
)

# Task: integrate extracted data
integrate = BashOperator(
    task_id = 'integrate',
    bash_command = 'paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag = dag,
)

# Task: Transform and load integrated data
transform = BashOperator(
    task_id = 'transform',
    bash_command = 'awk "$5 = toupper($5)" < extracted_data.csv > transformed_data.csv',
    dag = dag,
)

# Task: Define the pipeline
data >> csv_extract >> tsv_extract >> fixed_width_extract >> integrate >> transform
