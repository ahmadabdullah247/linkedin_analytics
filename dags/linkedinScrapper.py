from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator

import requests
from bs4 import BeautifulSoup

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


with dag:
    # Task 1: scraping job ids from linkedin
    task_get_job_ids = PythonOperator(
        task_id='Get_Job_Ids', 
        python_callable=get_job_ids, 
        provide_context=True,
        dag=dag)

    # Task 2: scraping job description for job ids
    task_get_job_description = PythonOperator(
        task_id='Get_Job_Description', 
        python_callable=get_job_ids, 
        provide_context=True,
        dag=dag)


    # Echo task finish
    task_finish = BashOperator(
        task_id = 'finish_task', 
        bash_command = 'echo finish', 
        dag = dag)

    task_get_job_ids >> task_get_job_description >> task_finish