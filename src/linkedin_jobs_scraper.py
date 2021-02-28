import requests
import urllib.request
import time
from bs4 import BeautifulSoup
import time
import pandas as pd


from utils.helpers import read_config, get_logger, write_to_file


class LinkedInJobsScraper:
    def __init__(self, num_jobs, query, config_path=None):

        self.num_jobs = num_jobs
        self.query = query
        self.job_ids = [] ## list for holding per page job ids
        
        self.scraper_config = read_config(config_path) ## loading configuration
        self.scraper_logger = get_logger() ## get logger for logging system state
    
    def search_jobs(self):
        
        for i in range(self.scraper_config['total_search_pages']):
            # Set the URL you want to webscrape from
            url = self.scraper_config['search_url'].format(i)

            self.scraper_logger.info('Searching jobs in page {}/{}'.format(i, self.scraper_config['total_search_pages']))
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
        print(job_ids_str)
        write_to_file(job_ids_str)

        self.scraper_logger.info('Lets go again!')
        ## flush historical job ids
        self.job_ids = []
        
        

def main():
    scraper = LinkedInJobsScraper(num_jobs=5, query=None)
    scraper.search_jobs()

if __name__ == "__main__":
    main()