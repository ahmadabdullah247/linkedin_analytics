from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

import requests
from bs4 import BeautifulSoup

import pymongo, yaml
from LIB.UTILS.logs import get_logger
from linkedin_job_scraper import LinkedInJobScraper
from LIB.CONFIG.credentials import credentials

# get config file
with open("dags/LIB/CONFIG/scraper.yaml", "r") as f:
    config = yaml.load(f)
    
# initialize logs
log = get_logger('LinkedInJobScraper') 

# Pytroch 
# https://www.youtube.com/watch?v=SKq-pmkekTk&list=PLlMkM4tgfjnJ3I-dbhO9JTw7gNty6o_2m&ab_channel=SungKim

# db setup 
# https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html
# for authentication
# https://stackoverflow.com/questions/52056809/how-to-activate-authentication-in-apache-airflow/52057433

# !pip install pyyaml pymongo 'pymongo[srv]'
# !python -m pip install 'mongo[srv]' dnspython

# Container deployment 
# Linked in scrayper
# Make python package

# Set default args
default_args = {
    'owner': 'Ahmad',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['ahmadabdullah247@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    'retries': 3
}

# Set Schedule: Run pipeline once a day.
# Use cron to define exact time (UTC). Eg. 8:15 AM would be '15 08 * * *'
schedule_interval = '30 09 * * *'

# connecting to database 
try:
    log.info('Trying to connect to database')
    client = pymongo.MongoClient(config['mongo_connect_url'].format(credentials['mongo_username'],credentials['mongo_password'],'jobs'))#,config['mongo_db'])
    db = client["linkedin_jobs"]
    collection = db["jobs"]
except Exception as e:
    log.error('Error connecting to database: ',e)

scraper = LinkedInJobScraper(100, None)

# Define DAG: Set ID and assign default args and schedule interval
dag = DAG(
  dag_id= 'Linkedin_Scrapper', 
  description= 'Getting Job Description from Linkedin',
  default_args= default_args,
  schedule_interval= schedule_interval)



def get_job_ids(**context):
    job_ids = scraper.search_jobs_ids('Software Engineer')
    print(job_ids)
    context['ti'].xcom_push(key='jobIds', value=job_ids)

def consolidating_job_ids(**context):
    job_ids = context['ti'].xcom_pull(key='jobIds')
    # get uniqe ids
    job_ids = list(set(job_ids))
    # filter ids that are not present in database
    job_ids = list(filter(lambda x: collection.count({ '_id': x }, limit = 1) == 0, job_ids))
    print(job_ids)
    context['ti'].xcom_push(key='jobIds', value=job_ids)


def get_job_description(**context):
    job_descriptions = []
    job_ids = context['ti'].xcom_pull(key='jobIds')
    # get job descriptions
    job_descriptions = [scraper.get_job_description(job_id) for job_id in job_ids]
    print("yolo",job_descriptions)
    # inserting jobs in database
    for job in job_descriptions:    
        collection.insert_one(job)  




with dag:
    start = DummyOperator(task_id="start")

    # Task 1: scraping job ids from linkedin
    task_get_job_ids= PythonOperator(
        task_id='get_job_ids', 
        python_callable=get_job_ids, 
        provide_context=True,
        dag=dag)

    # Task 2: Only keeping unique ids
    task_consolidating_job_ids= PythonOperator(
        task_id='Consolidating_Job_Ids', 
        python_callable=consolidating_job_ids, 
        provide_context=True,
        dag=dag)

    # Task 3: scraping job description for job ids
    task_get_job_description = PythonOperator(
        task_id='Get_Job_Description', 
        python_callable=get_job_ids, 
        provide_context=True,
        dag=dag)

    end = DummyOperator(task_id="end")

    start >> task_get_job_ids >> task_consolidating_job_ids >> task_get_job_description >> end