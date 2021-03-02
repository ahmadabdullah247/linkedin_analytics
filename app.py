from flask import Flask, request, jsonify
from flask_cors import CORS
from scraper.linkedin_jobs_scraper import LinkedInJobsScraper
from after_response import AfterResponse
app = Flask(__name__)
cors = CORS(app, resources={r"/*/*/*": {"origins": "*"}})
AfterResponse(app)

@app.route('/api/v1/scrape_linkedin/', methods=['GET'])
def scrape_linkedin():    
    return jsonify({'response':'scraper started!'})

@app.after_response
def start_scraper():
    scraper = LinkedInJobsScraper(num_jobs=-1, query=None)
    for search_term in scraper.scraper_config['search_terms']:
        search_term = "%20".join(search_term.split())
        scraper.search_jobs_ids(search_term)

def offline_worker():
    scraper = LinkedInJobsScraper(num_jobs=-1, query=None)
    while(1):
        for search_term in scraper.scraper_config['search_terms']:
            search_term = "%20".join(search_term.split())
            scraper.search_jobs_ids(search_term)

    

if __name__ == '__main__':
    offline_worker()

    #app.run()