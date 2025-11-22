from airflow import DAG 
from datetime import datetime, timedelta
import pandas as pd
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 1) Fetch churn data (Extract and Transform)
def get_data():
    url = 'https://raw.githubusercontent.com/dogukannulu/datasets/master/Churn_Modelling.csv'

    try:
        df = pd.read_csv(url) # Transform to DataFrame
        data = df.drop('RowNumber', axis = 1)
        data = data.drop_duplicates(subset=['CustomerId'])

        print("Data uploaded! \n", data.info())

    except Exception as err:
        print(err)
    
    return data 


# 2) Create and store data in table on postgres (Load)
def insert_data(ti):

    data = ti.xcom_pull(task_ids = 'fetch_all_data')

    index = 0

    try:
        pg_hook = PostgresHook(postgres_conn_id = 'churn_conn')
    
        insert_query = '''  
        INSERT INTO customers (CustomerId,Surname,CreditScore,Geography,Gender,Age,Tenure,Balance,NumOfProducts,HasCrCard,IsActiveMember,EstimatedSalary,Exited)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        for raw in range(data.size - 1):
            pg_hook.run(
                insert_query,
                parameters = (
                    int(data['CustomerId'][index]),
                    str(data['Surname'][index]),
                    str(data['CreditScore'][index]),
                    str(data['Geography'][index]),
                    str(data['Gender'][index]),
                    int(data['Age'][index]),
                    int(data['Tenure'][index]),
                    int(data['Balance'][index]),
                    int(data['NumOfProducts'][index]),
                    int(data['HasCrCard'][index]),
                    int(data['IsActiveMember'][index]),
                    float(data['EstimatedSalary'][index]),
                    int(data['Exited'][index])
                )
            )
            index = index + 1

        print("Data Inserted!\n", data.head())

    except Exception as err:
        print(err)


    



# Config setup
default_args = {
    'owner':'airflow',
    'dependes_on_past':False,
    'start_date':datetime(2025,11,5),
    'retries':1,
    'retry_delay':timedelta(seconds=10),
}

dag = DAG(
    'fetch_data',
    default_args = default_args,
    description = 'test',
)

# Operators and Hooks

fetch_data_task = PythonOperator(
    task_id = 'fetch_all_data',
    python_callable = get_data,
    dag = dag,
)

create_table_task = SQLExecuteQueryOperator(
    task_id = 'create_table',
    conn_id = 'churn_conn',
    sql = 'customers.sql',
    dag = dag,
)

insert_data_task = PythonOperator(
    task_id = 'insert_task',
    python_callable = insert_data,
    dag = dag,
)

fetch_data_task >> create_table_task >> insert_data_task