from elasticsearch import Elasticsearch
import frappe

def get_es_connection():
    """Connect to Elasticsearch."""
    hosts = [{"host": "localhost", "port": 9200, "scheme": "http"}]
    es = Elasticsearch(hosts=hosts)

    if not es.ping():
        raise ConnectionError("Cannot connect to Elasticsearch server")

    return es

def index_document(index, doc_id, document):
    """Index a document in Elasticsearch."""
    es = get_es_connection()
    es.index(index=index, id=doc_id, body=document)

def search_documents(index, query, scroll_id=None):
    """Search for documents in Elasticsearch with scrolling support."""
    es = get_es_connection()  # Ensure this function establishes the connection correctly
    
    # If scroll_id is provided, perform a scroll query
    if scroll_id:
        return es.scroll(scroll_id=scroll_id, scroll="1m")  # Use an appropriate scroll timeout (1 minute)
    
    # Otherwise, perform a regular search query
    return es.search(index=index, body=query, scroll="1m")  # Initial search with scroll parameter
