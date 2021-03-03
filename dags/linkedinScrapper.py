from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

import requests
from bs4 import BeautifulSoup

# db setup 
# https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html
# for authentication
# https://stackoverflow.com/questions/52056809/how-to-activate-authentication-in-apache-airflow/52057433

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

# Define DAG: Set ID and assign default args and schedule interval
dag = DAG(
  dag_id= 'Linkedin_Scrapper', 
  description= 'Getting Job Description from Linkedin',
  default_args= default_args,
  schedule_interval= schedule_interval)


def subdag(parent_dag_name, child_dag_name, args):
    """
    Generate a DAG to be used as a subdag.

    :param str parent_dag_name: Id of the parent DAG
    :param str child_dag_name: Id of the child DAG
    :param dict args: Default arguments to provide to the subdag
    :return: DAG to use as a subdag
    :rtype: airflow.models.DAG
    """
    dag_subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        default_args=default_args,
        start_date=days_ago(2),
        schedule_interval="@daily",
    )

    with dag_subdag:
        task_SE_job_ids = PythonOperator(
            task_id='Software_Engineer_Job_Ids', 
            python_callable=get_job_ids, 
            provide_context=True,
            dag=dag_subdag)

        task_DE_job_ids = PythonOperator(
            task_id='Data_Engineer_Job_Ids', 
            python_callable=get_job_ids, 
            provide_context=True,
            dag=dag_subdag)

    return dag_subdag


def get_job_ids(**context):
    total_pages=1
    job_ids = []
    for i in range(total_pages):
        # Set the URL you want to webscrape from
        url = 'https://www.linkedin.com/jobs/search?keywords=Software%20Engineer&location=Berlin%2C%20Berlin%2C%20Germany&geoId=&trk=homepage-jobseeker_jobs-search-bar_search-submit&redirect=false&position=1&pageNum={}'.format(i)
        # Connect to the URL
        response = requests.get(url)

        # Parse HTML and save to BeautifulSoup object¶
        soup = BeautifulSoup(response.text, "html.parser")
        
        ## extract job ids from the selected page
        jobs = soup.findAll("li", attrs={"class":"result-card job-result-card result-card--with-hover-state"})
        job_ids.append([job["data-id"] for job in jobs])

    context['ti'].xcom_push(key='jobIds', value=job_ids)
    # return job_ids

def get_job_description(**context):
    job_descriptions = []
    job_ids = context['ti'].xcom_pull(key='jobIds')
    
    for job_id in job_ids:
        url = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{}".format(job_id)
        # Connect to the URL
        response = requests.get(url)
        # Parse HTML and save to BeautifulSoup object¶
        soup = BeautifulSoup(response.text, "html.parser")
        description = soup.find("section",attrs={"class":"description"}).text
        job_descriptions.append(description)

    return job_descriptions

def consolidating_job_ids(**context):
    # get job ids form all sub-tasks
    job_ids = context['ti'].xcom_pull(key='jobIds')
    
    # save only unique from them
    # Take out id's that are already in database 


with dag:
    start = DummyOperator(task_id="start")

    # Task 1: scraping job ids from linkedin
    task_get_job_ids = SubDagOperator(
        task_id='Get_Job_Ids',
        subdag=subdag('Linkedin_Scrapper', 'Get_Job_Ids', default_args),
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