from flask import Flask, request, jsonify
from flask_cors import CORS
from scraper.linkedin_jobs_scraper import LinkedInJobsScraper
from after_response import AfterResponse
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime

app = Flask(__name__)
cors = CORS(app, resources={r"/*/*/*": {"origins": "*"}})
AfterResponse(app)
scheduler = BlockingScheduler()
@app.route('/api/v1/scrape_linkedin/', methods=['GET'])
def scrape_linkedin():    
    return jsonify({'response':'scraper started!'})

@app.after_response
def start_scraper():
    scraper = LinkedInJobsScraper(num_jobs=-1, query=None)
    for search_term in scraper.scraper_config['search_terms']:
        search_term = "%20".join(search_term.split())
        scraper.search_jobs_ids(search_term)


#@scheduler.scheduled_job('interval', minutes=2)
def offline_worker():
    while(1):
        scraper = LinkedInJobsScraper(num_jobs=-1, query=None)
        total_search_terms = len(scraper.scraper_config['search_terms'])
        scraper.scraper_logger.info("[SCHEDULER] LAST RUN: {}".format(datetime.now()))
        for index, search_term in enumerate(scraper.scraper_config['search_terms']):
            scraper.scraper_logger.info("[SCRAPER] RUNNING FOR SEARCH TERM: [{}/{}]".format(index, total_search_terms))
            search_term = "%20".join(search_term.split())
            scraper.search_jobs_ids(search_term)

#scheduler.start() ## using aps scheduler
if __name__ == "__main__":
    main()
