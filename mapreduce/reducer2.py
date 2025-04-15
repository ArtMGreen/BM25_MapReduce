#!/usr/bin/env python
import sys
# from cassandra.cluster import Cluster

current_word = None
current_count = 0
current_doc_id = None
word = None


# cluster = Cluster(["cassandra-server"])
# session = cluster.connect()
# session.set_keyspace("bm25")


# def to_cassandra_df(term, df):
#     df = int(df)
#     row = session.execute(
#         "SELECT df FROM document_frequencies WHERE term=%s",
#         (term,)
#     ).one()
# 
#     current_df = row.df if row else 0
#     session.execute(
#         "INSERT INTO document_frequencies (term, df) VALUES (%s, %s)",
#         (term, current_df + df)
#     )


for line in sys.stdin:
    line = line.strip()
    document_id, word, count = line.split('\t')
    try:
        count = int(count)
    except ValueError:
        continue

    if current_word == word and document_id == current_doc_id:
        continue
    elif current_word == word:
        current_count += 1
        current_doc_id = document_id
    else:
        if current_word:
            print(f'{current_word}\t{current_count}')
            # to_cassandra_df(current_word, current_count)
        current_count = 1
        current_word = word
        current_doc_id = document_id

if current_word == word:
    print(f'{current_word}\t{current_count}')
    # to_cassandra_df(current_word, current_count)
