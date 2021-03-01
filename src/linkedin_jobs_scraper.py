import requests
import urllib.request
import time
from bs4 import BeautifulSoup
import time
import pandas as pd
from utils.helpers import *



class LinkedInJobsScraper:
    def __init__(self, num_jobs, query, config_path=None):

        self.num_jobs = num_jobs
        self.query = query
        self.job_ids = [] ## list for holding per page job ids
        
        self.scraper_config = read_config(config_path) ## loading configuration
        self.scraper_logger = get_logger() ## get logger for logging system state
    
    def search_jobs_ids(self):
        
        for i in range(self.scraper_config['total_search_pages']):
            # Set the URL you want to webscrape from
            url = self.scraper_config['search_url'].format(i)

            self.scraper_logger.info('Searching jobs in page {}/{}'.format(i+1, self.scraper_config['total_search_pages']))
            # Connect to the URL
            response = requests.get(url)

            # Parse HTML and save to BeautifulSoup object¶
            soup = BeautifulSoup(response.text, "html.parser")
            
            self.scraper_logger.info('Extracting Job Ids from the page')

            ## extract job ids from the selected page
            self.extract_job_ids(soup)

    def extract_job_ids(self, soup):
        jobs = soup.findAll(self.scraper_config['job_title_element'], 
                        attrs={"class":self.scraper_config['job_title_element_class']})

        ## iterating over job elements to extract job ids
        for job in jobs:
            self.job_ids.append('{}'.format(job[self.scraper_config['job_id_element_identifier']]))

        self.scraper_logger.info('Writing job ids to file')
        job_ids_str = "\n".join(self.job_ids)
        write_to_file(job_ids_str, self.scraper_config['jobs_ids_file'])

        self.scraper_logger.info('Lets go again!')
        ## flush historical job ids
        self.job_ids = []
    
    def get_job_data(self, job_id):
        url = self.scraper_config['li_jobs_api'].format(job_id)
        # Connect to the URL
        response = requests.get(url)

        # Parse HTML and save to BeautifulSoup object¶
        soup = BeautifulSoup(response.text, "html.parser")
        job_info = {}
        ## find jd section
        job_info['job_id'] = job_id
        job_info['job_title'] = soup.find("h2",attrs={"class":self.scraper_config['job_title_class']}).text
        job_info['description'] = soup.find("section",attrs={"class":"description"}).text
        job_info['employer_name'] = soup.find("a",attrs={"class":self.scraper_config['employer_name_class']}).text
        job_info['date_posted'] = rel_time_to_absolute_datetime(soup.find("span",attrs={"class":self.scraper_config['job_date_class']}).text)
        job_meta_ul = soup.find("ul",attrs={"class": self.scraper_config['job_meta_info_class'] })

        if soup.find("span",attrs={"class": self.scraper_config['n_applicants_class'] }):
            job_info['n_applicants'] = int(soup.find("span",attrs={"class": self.scraper_config['n_applicants_class'] }).text.split(' ')[0])  
        else:
            job_info['n_applicants'] = 0

        for item in job_meta_ul.findAll('li'):
                key = item.find('h3').text.lower()
                for index, meta_data in enumerate(item.findAll('span')):
                    if meta_data.text:
                        job_info['{}_{}'.format(key, index)] = meta_data.text

        return job_info
    
    def fetch_job_info(self, job_ids, scraper_history):
        total_jobs = len(job_ids)
        while (len(job_ids)>0): ## iterate until no jobs left
            self.scraper_logger.info('Fetching data for JOB[{}/{}]'.format((total_jobs - len(job_ids)), total_jobs))
            job_id = job_ids.pop() ## get last job in queue
            
            if job_id not in scraper_history:
                job_info = self.get_job_data(job_id)
                
                if job_info:
                    ## TODO: update status and dump to ES
                    write_to_file(str(job_id), self.scraper_config['scraper_history_file'])
                    write_to_es(self.scraper_config['es_index'], job_info)
            time.sleep(1) ## sleep for one second
            

            
        

def main():
    scraper = LinkedInJobsScraper(num_jobs=-1, query=None)
  
    job_ids = read_from_file(scraper.scraper_config['job_ids_file'])[:2]
    scraper_history = read_from_file(scraper.scraper_config['scraper_history_file'])
    scraper.fetch_job_info(job_ids, scraper_history)

if __name__ == "__main__":
    main()
    
