import requests
import xml.etree.ElementTree as ET
import random
import socket
import json
from flask import Flask, request, render_template, jsonify
from datetime import datetime

app = Flask(__name__)

LOGSTASH_HOST = 'logstash'
LOGSTASH_PORT = 5044

# Lista di termini di ricerca casuali
search_terms = [
    "quantum computing", "machine learning", "neural networks", "artificial intelligence",
    "blockchain", "cybersecurity", "biotechnology", "nanotechnology", "genomics", "space exploration"
]

def get_random_search_term():
    return random.choice(search_terms)

def get_arxiv_articles():
    search_term = get_random_search_term()
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
            "source": {
                "name": "arXiv",
                "id": None
            },
            "title": entry.find('{http://www.w3.org/2005/Atom}title').text,
            "urlToImage": None,
            "content": entry.find('{http://www.w3.org/2005/Atom}summary').text,
            "author": ", ".join([author.find('{http://www.w3.org/2005/Atom}name').text for author in entry.findall('{http://www.w3.org/2005/Atom}author')])
        }
        articles.append(article)
        print(f"Content: {article['content']}")
    
    print(f"Number of articles fetched: {len(articles)}")
    return articles

@app.route('/')
def index():
    return render_template('index.html')

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
    
    send_to_logstash(data)
    return jsonify({"status": "success", "data": data})

@app.route('/fetch_articles', methods=['GET'])
def fetch_articles():
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

def send_to_logstash(data):
    message = json.dumps(data) + '\n'
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((LOGSTASH_HOST, LOGSTASH_PORT))
        sock.sendall(message.encode('utf-8'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
