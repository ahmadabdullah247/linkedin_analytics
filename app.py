from flask import Flask, request, jsonify
from flask_cors import CORS
from scraper.linkedin_jobs_scraper import LinkedInJobsScraper
app = Flask(__name__)
cors = CORS(app, resources={r"/*/*/*": {"origins": "*"}})

@app.route('/api/v1/scrape_linkedin/', methods=['GET'])
def scrape_linkedin():    
    return jsonify({'response':'scraper started!'})

@app.after_response
def start_scraper():
    scraper = LinkedInJobsScraper(num_jobs=-1, query=None)
    for search_term in scraper.scraper_config['search_terms']:
        search_term = "%20".join(search_term.split())
        scraper.search_jobs_ids(search_term)


@app.route('/api/v1/heartbeat/', methods=['GET'])
def heartbeat():
    return jsonify({'heartbeat':'scraper is alive!'})

@app.route('/')
def index():
    """Return homepage."""
    json_data = {'Hello': 'World!'}
    return jsonify(json_data)

if __name__ == '__main__':
    app.run()