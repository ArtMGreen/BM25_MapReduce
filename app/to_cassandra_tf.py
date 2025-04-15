from cassandra.cluster import Cluster
import sys


DROP_TABLE = "--drop-table" in sys.argv
DEBUG_OUTPUTS = "--debug-outputs" in sys.argv

cluster = Cluster(["cassandra-server"])
session = cluster.connect()
if DEBUG_OUTPUTS:
    print("Connected to the cluster")
    
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS bm25
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 };
""")
session.set_keyspace("bm25")
if DEBUG_OUTPUTS:
    print("Created/enabled the keyspace")
    
# if DROP_TABLE:
#     session.execute("DROP TABLE IF EXISTS term_frequencies;")
#     if DEBUG_OUTPUTS:
#         print("Dropped old TF table")

session.execute("""
    CREATE TABLE IF NOT EXISTS term_frequencies (
        term text,
        document_id int,
        tf int,
        PRIMARY KEY (term, document_id)
    );
""")
if DEBUG_OUTPUTS:
    print("term_frequencies table now exists")

i = 0
for line in sys.stdin:
    doc_id, term, tf = line.strip().split('\t')
    doc_id = int(doc_id)
    tf = int(tf)
    if DROP_TABLE:
        i+=1
        session.execute(
            "INSERT INTO term_frequencies (term, document_id, tf) VALUES (%s, %s, %s)",
            (term, doc_id, tf)
        )
        if DEBUG_OUTPUTS and i % 10000 == 0:
            print("Put", i, "lines into Cassandra table")
    else:
        row = session.execute(
            "SELECT tf FROM term_frequencies WHERE term=%s AND document_id=%s",
            (term, doc_id)
        ).one()

        current_tf = row.tf if row else 0
        session.execute(
            "INSERT INTO term_frequencies (term, document_id, tf) VALUES (%s, %s, %s)",
            (term, doc_id, current_tf + tf)
        )

