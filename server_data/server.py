from flask import Flask, request, render_template, jsonify
from datetime import datetime, date
import requests
import xml.etree.ElementTree as ET
import json
import socket
import pandas as pd
import os
import pickle

app = Flask(__name__)

LOGSTASH_HOST = 'logstash'
LOGSTASH_PORT = 5044
ARTICLES_PER_REQUEST = 10
DATA_DIR = '/app/data'

ARTICLES_FILE = os.path.join(DATA_DIR, 'articles.pkl')
INDEX_FILE = os.path.join(DATA_DIR, 'index.txt')
LAST_FETCH_DATE_FILE = os.path.join(DATA_DIR, 'last_fetch_date.txt')

if os.path.exists(ARTICLES_FILE) and os.path.getsize(ARTICLES_FILE) > 0:
    print("sto leggendo dal dataset gia creato")
    articles_df = pd.read_pickle(ARTICLES_FILE)
else:
    articles_df = pd.DataFrame(columns=["url", "publishedAt", "description", "source", "title", "urlToImage", "content", "author"])
    print("creo il dataset")

if os.path.exists(INDEX_FILE) and os.path.getsize(INDEX_FILE) > 0:
    with open(INDEX_FILE, 'r') as f:
        current_index = int(f.read())
else:
    current_index = 0

def save_index():
    with open(INDEX_FILE, 'w') as f:
        f.write(str(current_index))

#fino a drug discovery processes
#  "quantum computing", "machine learning", "neural networks", "artificial intelligence",
#    "blockchain", "cybersecurity", "biotechnology", "nanotechnology","genomics", "space exploration",
 #   "Quantum cryptography", "Neural network architectures", "Deep learning optimization",
 #   "Generative adversarial networks", "Reinforcement learning applications",
 
 # "Graph neural networks",
 #   "Autonomous robotics", "Natural language processing", "Computer vision techniques", "Bioinformatics algorithms",
 #   "Climate modeling", "Particle physics experiments", "Quantum field theory", "Black hole thermodynamics",
 #   "Cosmological inflation", "Dark matter detection", "Renewable energy technologies", "Nanoscale materials",
 #   "Organic electronic devices", "Drug discovery processes", "Genome editing technologies", "Cancer genomics",
 #   "Advanced statistical methods", "Financial econometrics", "Cryptocurrency systems",
 #   "Blockchain scalability solutions", "Internet of Things security", "Quantum computing algorithms",
 #   "Machine learning in healthcare", "Artificial intelligence ethics",
 #   "macroeconomics", "microeconomics", "behavioral economics", "financial markets", "monetary policy","economic growth", "international trade",
#  "development economics", "public finance", "labor economics",
#    "econometrics", "sustainable development", "health economics", "agricultural economics",
#    "environmental economics", "urban economics", "game theory", "industrial organization",
#    "income inequality", "fiscal policy", "clinical trials",
#  "epidemiology", "genetic disorders",
#    "oncology", "cardiology", "neurology", "pediatrics", "geriatrics", "immunology", "infectious diseases",

search_terms = [ 
  
    "mental health", "public health", "surgical techniques", "radiology", "anesthesiology", "pharmacology",
    "orthopedics", "endocrinology", "dermatology", "obstetrics and gynecology", "sports medicine",
    "exercise physiology", "biomechanics", "athletic performance", "sports psychology", "nutrition in sports",
    "injury prevention", "rehabilitation", "strength and conditioning", "cardiovascular fitness", "team dynamics",
    "motor skills", "endurance training", "competitive sports", "youth sports", "sports analytics", "eSports",
    "sports management", "doping in sports", "sports sociology", "artificial intelligence in healthcare",
    "blockchain in finance", "machine learning in medicine", "big data analytics in economics", "telemedicine",
    "digital health", "financial technology (FinTech)", "health informatics", "wearable technology in sports",
    "robotics in surgery"
]
current_search_index = 0
def get_next_search_term():
    global current_search_index
    term = search_terms[current_search_index]
    current_search_index = (current_search_index + 1) % len(search_terms)
    return term

def get_arxiv_articles():
    search_term = get_next_search_term()
    print(f"Chosen search term: {search_term}")
    
    base_url = 'http://export.arxiv.org/api/query'
    params = {
        'search_query': f'all:{search_term}',
        'start': 0,
        'max_results': 10
    }

    response = requests.get(base_url, params=params)
    root = ET.fromstring(response.content)

    articles = []
    for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
        article = {
            "url": entry.find('{http://www.w3.org/2005/Atom}id').text,
            "publishedAt": entry.find('{http://www.w3.org/2005/Atom}published').text,
            "description": entry.find('{http://www.w3.org/2005/Atom}summary').text,
            "source": {"name": "arXiv", "id": None},
            "title": entry.find('{http://www.w3.org/2005/Atom}title').text,
            "urlToImage": None,
            "content": entry.find('{http://www.w3.org/2005/Atom}summary').text,
            "author": ", ".join([author.find('{http://www.w3.org/2005/Atom}name').text for author in entry.findall('{http://www.w3.org/2005/Atom}author')])
        }
        articles.append(article)
    
    print(f"Numero di articoli prelevati arxiv: {len(articles)}")
    return articles

def fetch_google_news():
    url = "https://google-news13.p.rapidapi.com/latest"
    querystring = {"lr": "en-US"}
    headers = {
        "x-rapidapi-key": "your_api_key", 
        "x-rapidapi-host": "google-news13.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    data = response.json()

    articles = []
    for item in data.get('items', []):
        subnews = item.get('subnews', [])
        subnews_content = "\n\n".join([f"{sub['title']}: {sub['snippet']}" for sub in subnews])
        content = f"{item.get('snippet', 'No content available')}\n\n{subnews_content}"
        
        article = {
            "url": item.get('newsUrl', 'No URL available'),
            "publishedAt": datetime.fromtimestamp(int(item.get('timestamp', 0)) / 1000).isoformat(),
            "description": item.get('snippet', 'No description available'),
            "source": {"name": item.get('publisher', 'No publisher'), "id": None},
            "title": item.get('title', 'No title available'),
            "urlToImage": item.get('images', {}).get('thumbnail', None),
            "content": content,
            "author": item.get('publisher', 'No publisher')
        }
        articles.append(article)

    print(f"Numero di articoli prelevati gnwes: {len(articles)}")
    return articles

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_articles', methods=['GET'])
def get_articles():
    global current_index, articles_df
    print(f"Serving articles from index: {current_index}")
    if current_index >= len(articles_df):
        data = {
            "@timestamp": datetime.now().isoformat(),
            "articles": [],
            "@version": "1",
            "status": "no more articles",
            "totalResults": "0"
        }
        print("Non ci sono piu articoli")
        return jsonify(data)
    
    end_index = min(current_index + ARTICLES_PER_REQUEST, len(articles_df))

    articles = articles_df.iloc[current_index:end_index].to_dict(orient='records')
    
    current_index = end_index
    save_index()
    
    data = {
        "@timestamp": datetime.now().isoformat(),
        "articles": articles,
        "@version": "1",
        "status": "ok",
        "totalResults": str(len(articles))
    }
    send_to_logstash(data)
    return jsonify(data)

@app.route('/upload', methods=['POST'])
def upload():
    title = request.form['title']
    author = request.form['author']
    content = request.form['content']
    
    data = {
        "@timestamp": datetime.now().isoformat(),
        "articles": [{
            "title": title,
            "author": author,
            "content": content,
            "publishedAt": datetime.now().isoformat(),
            "source": {
                "name": "Manual Upload",
                "id": None
            },
            "description": content[:150],
            "url": None,
            "urlToImage": None
        }],
        "@version": "1",
        "status": "ok",
        "totalResults": "1"
    }
    
    print("Upload manuale")
    send_to_logstash(data)
    return jsonify({"status": "success", "data": data})

@app.route('/fetch_articles', methods=['GET'])
def fetch_articles_route():
    articles = get_arxiv_articles()
    data = {
        "@timestamp": datetime.now().isoformat(),
        "articles": articles,
        "@version": "1",
        "status": "ok",
        "totalResults": str(len(articles))
    }
    send_to_logstash(data)
    return jsonify(data)

@app.route('/fetch_google_news', methods=['GET'])
def fetch_google_news_articles_route():
    global articles_df
    today = date.today().isoformat()
    
    if os.path.exists(LAST_FETCH_DATE_FILE):
        with open(LAST_FETCH_DATE_FILE, 'r') as f:
            print("sto leggendo la data")
            last_fetch_date = f.read().strip()
    else:
        last_fetch_date = ""

    if last_fetch_date != today:
        with open(LAST_FETCH_DATE_FILE, 'w') as file:
            file.write(today)
        
        print("Fetching new articles from Google News API")
        articles = fetch_google_news()
        if not articles_df.empty:
            articles_df = pd.concat([articles_df, pd.DataFrame(articles)], ignore_index=True)
        else:
            articles_df = pd.DataFrame(articles)
        
        articles_df.to_pickle(ARTICLES_FILE)
        
        message = "Fetched new articles."
    else:
        print("Articles already fetched today")
        message = "Articles were already fetched today."

    data = {
        "@timestamp": datetime.now().isoformat(),
        "articles": [],
        "@version": "1",
        "status": "ok",
        "message": message
    }
    return jsonify(data)

def send_to_logstash(data):
    message = json.dumps(data) + '\n'
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((LOGSTASH_HOST, LOGSTASH_PORT))
        sock.sendall(message.encode('utf-8'))
    print("Data sent to Logstash")

def fetch_initial_articles():
    with app.app_context():
        print("Chiamo la funzione iniziale per Google News")
        fetch_google_news_articles_route()

if __name__ == '__main__':
    fetch_initial_articles()
    app.run(host='0.0.0.0', port=5000)
