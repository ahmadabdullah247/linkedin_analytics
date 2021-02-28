import logging
import yaml
from exceptions.exception import InvalidConfigurationException


def read_config(config_path):
    if not config_path:
        config_path = 'config/scraper_config.yaml'

    with open(r'{}'.format(config_path)) as file:
        # The FullLoader parameter handles the conversion from YAML
        # scalar values to Python the dictionary format
        scraper_config = yaml.load(file, Loader=yaml.FullLoader)
    
    return scraper_config

def get_logger():
    logging.basicConfig(level=logging.DEBUG)
    return logging.getLogger(name='LinkedIn Jobs Scraper')

def write_to_file(data):
    with open('job_ids.txt','a+') as f:
        f.write('{}\n'.format(data))
