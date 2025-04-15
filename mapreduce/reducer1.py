#!/usr/bin/env python
import sys
# from cassandra.cluster import Cluster

current_word = None
current_document_id = None
current_count = 0
word = None


# cluster = Cluster(["cassandra-server"])
# session = cluster.connect()
# session.set_keyspace("bm25")


# def to_cassandra_tf(doc_id, term, tf):
#     doc_id = int(doc_id)
#     tf = int(tf)
#     row = session.execute(
#         "SELECT tf FROM term_frequencies WHERE term=%s AND document_id=%s",
#         (term, doc_id)
#     ).one()

#     current_tf = row.tf if row else 0
#     session.execute(
#         "INSERT INTO term_frequencies (term, document_id, tf) VALUES (%s, %s, %s)",
#         (term, doc_id, current_tf + tf)
#     )


for line in sys.stdin:
    line = line.strip()
    document_id, word, count = line.split('\t')
    try:
        count = int(count)
    except ValueError:
        continue

    if current_word == word and current_document_id == document_id:
        current_count += count
    else:
        if current_word and document_id:
            print(f'{document_id}\t{current_word}\t{current_count}')
            # to_cassandra_tf(document_id, current_word, current_count)
        current_count = count
        current_word = word
        current_document_id = document_id

if current_word == word:
    print(f'{document_id}\t{current_word}\t{current_count}')
    # to_cassandra_tf(document_id, current_word, current_count)
