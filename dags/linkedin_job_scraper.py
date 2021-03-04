import requests, yaml, time
import pandas as pd
from bs4 import BeautifulSoup
import urllib.request

from LIB.UTILS.logs import get_logger

# get config file
with open("dags/LIB/CONFIG/scraper.yaml", "r") as f:
    config = yaml.load(f)

# initialize logs
log = get_logger('LinkedInJobScraper') 

class LinkedInJobScraper:
    def __init__(self, num_jobs, query, config_path=None):
        self.num_jobs = num_jobs
        self.query    = query
        self.job_ids  = [] ## list for holding per page job ids

    def search_jobs_ids(self, search_term):
        for i in range(config['total_search_pages']):
            # Set the URL you want to webscrape from
            url = config['search_url'].format(search_term,i)

            log.info('Searching jobs in page {}/{}'.format(i+1, config['total_search_pages']))
            # Connect to the URL
            response = requests.get(url)

            # Parse HTML and save to BeautifulSoup object
            soup = BeautifulSoup(response.text, "html.parser")
            
            log.info('Extracting Job Ids from the page')

            ## extract job ids from the selected page
            jobs = soup.findAll("li", attrs={"class":"result-card job-result-card result-card--with-hover-state"})
            self.job_ids.extend([job["data-id"] for job in jobs])

        return self.job_ids


    def get_job_description(self, job_id):
        url = config['li_jobs_api'].format(job_id)
        # Connect to the URL
        response = requests.get(url)

        # Parse HTML and save to BeautifulSoup object
        soup = BeautifulSoup(response.text, "html.parser")
        job_info = {}
        ## find jd section
        job_info['_id'] = job_id
        if soup.find("h2",attrs={"class":config['job_title_class']}):
            job_info['job_title'] = soup.find("h2",attrs={"class":config['job_title_class']}).text
        else:
            job_info['job_title'] = '<NOT_GIVEN>'
        
        if soup.find("section",attrs={"class":"description"}):
            job_info['description'] = soup.find("section",attrs={"class":"description"}).text
        else:
            job_info['description'] = '<NOT_GIVEN>'

        if soup.find("span",attrs={"class":config['job_location_class']}):
            job_info['location'] = soup.find("span",attrs={"class":config['job_location_class']}).text
        else:
            job_info['location'] = '<NOT_GIVEN>'

        if soup.find("a",attrs={"class":config['employer_name_class']}):
            job_info['employer_name'] = soup.find("a",attrs={"class":config['employer_name_class']}).text
        else:
            job_info['employer_name'] = '<NOT_GIVEN>'
        if soup.find("span",attrs={"class":config['job_date_class']}):
            job_info['date_posted'] = rel_time_to_absolute_datetime(soup.find("span",attrs={"class":config['job_date_class']}).text)
        else:
            job_info['date_posted'] = '<NOT_GIVEN>'
        
        job_meta_ul = soup.find("ul",attrs={"class": config['job_meta_info_class'] })

        if soup.find("span",attrs={"class": config['n_applicants_class'] }):
            job_info['n_applicants'] = int(soup.find("span",attrs={"class": config['n_applicants_class'] }).text.split(' ')[0])  
        else:
            job_info['n_applicants'] = 0
        if  job_meta_ul:
            for item in job_meta_ul.findAll('li'):
                    key = item.find('h3').text.lower()
                    for index, meta_data in enumerate(item.findAll('span')):
                        if meta_data.text:
                            job_info['{}_{}'.format(key, index)] = meta_data.text

        return job_info