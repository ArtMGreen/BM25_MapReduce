#!/usr/bin/env python
import logging
from pyspark import SparkContext, SparkConf
from cassandra.cluster import Cluster
import sys
import math
from functools import partial

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

K1 = 1
B = 0.75

# I hate this fact but cassandra sessions can't be serialized
# or so I've heard
def init_cassandra():
    cluster = Cluster(["cassandra-server"])
    session = cluster.connect("bm25")
    
    row = session.execute("SELECT COUNT(length) FROM doc_index").one()
    N = row[0]
    row = session.execute("SELECT AVG(length) FROM doc_index").one()
    avg_dl = row[0] if row else 0
    return session, N, avg_dl

def compute_bm25(query_terms, doc_id):
    # I hope it's a worker???
    session, N, avg_dl = init_cassandra()
    
    row = session.execute(
        "SELECT title, length FROM doc_index WHERE document_id = %s", 
        (doc_id,)
    ).one()
    if not row:
        return (doc_id, "", 0.0)
    
    title, dl = row.title, row.length
    score = 0.0
    
    for term in query_terms:
        tf_row = session.execute(
            "SELECT tf FROM term_frequencies WHERE term = %s AND document_id = %s", 
            (term, doc_id)
        ).one()
        if not tf_row:
            continue
        
        tf = tf_row.tf
        
        df_row = session.execute(
            "SELECT df FROM document_frequencies WHERE term = %s", 
            (term,)
        ).one()
        if not df_row:
            continue
        
        df = df_row.df
        
        idf = math.log((N - df + 0.5) / (df + 0.5))
        numerator = tf * (K1 + 1)
        denominator = tf + K1 * (1 - B + B * (dl / avg_dl))
        score += idf * (numerator / denominator)
    
    session.shutdown()
    return (doc_id, title, score)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("No query provided...", file=sys.stderr)
        sys.exit(1)
    
    query = sys.argv[1].strip().lower()
    query_terms = list(set(query.split()))
    
    conf = SparkConf().setAppName("BM25 Search") \
                     .set("spark.cassandra.connection.host", "cassandra-server")
    sc = SparkContext(conf=conf)
    
    # info logs BEGONE ! ! ! (it just spammed to much)
    sc.setLogLevel("WARN")
    
    session, N, avg_dl = init_cassandra()
    doc_ids = set()
    for term in query_terms:
        rows = session.execute(
            "SELECT document_id FROM term_frequencies WHERE term = %s", 
            (term,)
        )
        doc_ids.update(row.document_id for row in rows)
    session.shutdown()
    
    query_terms_bc = sc.broadcast((query_terms, N, avg_dl))
    
    bm25_scores = (
        sc.parallelize(list(doc_ids))
          .map(lambda doc_id: compute_bm25(query_terms_bc.value[0], doc_id))
          .filter(lambda x: x[2] > 0)
          .collect()
    )
    
    top_docs = sorted(bm25_scores, key=lambda x: -x[2])[:10]
    print("\nTop 10 (or less...) relevant documents:")
    for doc_id, title, score in top_docs:
        print(f"Document ID: {doc_id}, Title: {title}, BM25 Score: {score:.4f}")
    
    sc.stop()
