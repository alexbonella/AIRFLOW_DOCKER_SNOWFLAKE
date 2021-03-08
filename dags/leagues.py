
"""

********************************************************
Autor = @lexbonella -- https://github.com/alexbonella  *                        
Fecha = '06/03/2021'                                   * 
Nombre = Extracting Data from Multiple football league * 
******************************************************** 

"""

import logging
import airflow
from airflow.models import Variable
from airflow import models
from airflow.models import DAG 
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta 
import pandas as pd
import snowflake.connector as sf
import time
import random
import os 



default_arguments = {   'owner': 'Alexbonella',
                        'email': 'yolonella2809@gmail.com',
                        'email_on_failure': True,   
                        'email_on_success': True,
                        'start_date': airflow.utils.dates.days_ago(0),
                        'retries':1 ,
                        'retry_delay':timedelta(minutes=5)}  

# Instanciamos nuestro DAG 
etl_dag = DAG( 'FOOTBAL_LEAGUES',
                default_args=default_arguments,
                description='Extracting Data Footbal League' ,
                schedule_interval = '@weekly',
                tags=['tabla_espn'],
                 )


conn=sf.connect(
    user=Variable.get("USER"),
    password=Variable.get("PSWD"),
    account=Variable.get("ACCOUNT"),
    warehouse=Variable.get("WRH"),
    database=Variable.get("DATABASE"),
    schema=Variable.get("SCHEMA")
    )

def validation_connection(conn, **kwargs):

    
    
    print('This is my Database : ' +str(conn.database))
    print('\n This my Schema : ' +str(conn.schema))
    
    print('\n Conection SUCCESS !!!! ' )
    
    
# Data Processing Function 
def data_processing():     

    tiempo = [1,4,5,6,7,10]
    
    df_premier=pd.read_html('https://www.espn.com.co/futbol/liga/_/nombre/eng.1/liga-premier')
    premier=df_premier[0]
    premier.set_index('TEAM', inplace=True)
    time.sleep(random.choice(tiempo))

    df_bbva=pd.read_html('https://www.espn.com.co/futbol/liga/_/nombre/esp.1/primera-division-de-espana')
    bbva=df_bbva[0]
    bbva.set_index('TEAM', inplace=True)
    time.sleep(random.choice(tiempo))

    df_italia=pd.read_html('https://www.espn.com.co/futbol/liga/_/nombre/ita.1/serie-a-de-italia')
    italia=df_italia[0]
    italia.set_index('TEAM', inplace=True)
    time.sleep(random.choice(tiempo))

    df_francia=pd.read_html('https://www.espn.com.co/futbol/liga/_/nombre/fra.1/ligue-1-francia')
    francia=df_francia[0]
    francia.set_index('TEAM', inplace=True)
    time.sleep(random.choice(tiempo))

    df_bundesliga=pd.read_html('https://www.espn.com.co/futbol/liga/_/nombre/ger.1/bundesliga')
    bundesliga=df_bundesliga[0]
    bundesliga.set_index('TEAM', inplace=True)
    time.sleep(random.choice(tiempo))

    df_final=pd.concat([premier,italia,francia,bundesliga,bbva],ignore_index=False)

    df_final.to_csv('/opt/airflow/premier_positions.csv',header=True)

def carga_stage(conn, **kwargs):

    csv_file='/opt/airflow/premier_positions.csv'
    
    sql = 'put file://{0} @{1} auto_compress=true'.format(csv_file,Variable.get("STAGE"))
    curs=conn.cursor()
    curs.execute(sql)
    print('UPLOAD TO STAGE SUCCESS')

def carga_tabla(conn, **kwargs):
    
    sql2 = "copy into {0} from @{1}/premier_positions.csv.gz FILE_FORMAT=(TYPE=csv field_delimIter=',' skip_header=1) ON_ERROR = 'CONTINUE' ".format(Variable.get("TABLE"),Variable.get("STAGE"))
    curs=conn.cursor()
    curs.execute(sql2)
    print('UPLOAD TABLE SUCCESS')  
    
# Conection Task
validation = PythonOperator(task_id='CONNECTION_SUCCESS', 
                            provide_context=True,    
                            python_callable=validation_connection,
                            op_kwargs={"conn":conn},     
                            dag=etl_dag )

# Data Extraction Task 
extract_features = PythonOperator(task_id='EXTRACTING_DATA',     
                             python_callable=data_processing,     
                             dag=etl_dag )
# Data Upload  Task 
upload_stage = PythonOperator(task_id='UPLOAD_STAGE',     
                             python_callable=carga_stage,
                             op_kwargs={"conn":conn},     
                             dag=etl_dag )
# Data Upload  Task 
upload_tabla = PythonOperator(task_id='UPLOAD_TABLE_SNOWFLAKE',     
                             python_callable=carga_tabla,
                             op_kwargs={"conn":conn},     
                             dag=etl_dag )



validation >> extract_features >> upload_stage >> upload_tabla

