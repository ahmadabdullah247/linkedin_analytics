from flask import Flask,request
from flask_cors import CORS
from scraper.linkedin_jobs_scraper import LinkedInJobsScraper
app = Flask(__name__)
cors = CORS(app, resources={r"/api/v1/*": {"origins": "*"}})

@app.route('/api/v1/scrape_linkedin/', methods=['GET'])
def scrape_linkedin():
    scraper = LinkedInJobsScraper(num_jobs=-1, query=None)
    for search_term in scraper.scraper_config['search_terms']:
        search_term = "%20".join(search_term.split())
        scraper.search_jobs_ids(search_term)
    
    return "scraping completed..."

@app.route('/api/v1/heartbeat/', methods=['GET'])
def heartbeat():
    return 'scraper is alive!'

if __name__ == '__main__':
    app.run()