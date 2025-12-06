import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
'owner': 'airflow',
'start_date':datetime(2020, 1, 1),
}

dag_spark = DAG(
    dag_id="dag_project_7",
    default_args=default_args,
    schedule_interval='@weekly',  # Запуск раз в неделю
)

friends_reccomendation_d7 = SparkSubmitOperator(
task_id='friends_reccomendation_d7',
dag=dag_spark,
application ='/user/valentinat/friends_reccomendation.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "7",  "/user/master/data/geo/events", "/user/valentinat/data/analystics/friends_reccomendation_d7"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

locations_cnt_d7 = SparkSubmitOperator(
task_id='locations_cnt_d7',
dag=dag_spark,
application ='/user/valentinat/locations_cnt.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "7",  "/user/master/data/geo/events", "/user/valentinat/data/analystics/locations_cnt_d7"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

messages_coordinates_d7 = SparkSubmitOperator(
task_id='messages_coordinates_d7',
dag=dag_spark,
application ='/user/valentinat/messages_coordinates.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-31", "7",  "/user/master/data/geo/events", "/user/valentinat/data/analystics/messages_coordinates_d7"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

friends_reccomendation_d7 >> locations_cnt_d7 >> messages_coordinates_d7