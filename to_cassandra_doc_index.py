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
#     session.execute("DROP TABLE IF EXISTS document_frequencies;")
#     if DEBUG_OUTPUTS:
#         print("Dropped old DF table")

session.execute("""
    CREATE TABLE IF NOT EXISTS doc_index (
        document_id int PRIMARY KEY,
        title text,
        length int
    );
""")
if DEBUG_OUTPUTS:
    print("doc_index table now exists")

i = 0
for line in sys.stdin:
    doc_id, title, length = line.strip().split('\t')
    doc_id = int(doc_id)
    length = int(length)
    if DROP_TABLE:
        i+=1
        session.execute(
            "INSERT INTO doc_index (document_id, title, length) VALUES (%s, %s, %s)",
            (doc_id, title, length)
        )
        if DEBUG_OUTPUTS and i % 10000 == 0:
            print("Put", i, "lines into Cassandra table")
    else:
        row = session.execute(
            "SELECT length FROM doc_index WHERE document_id=%s",
            (doc_id,)
        ).one()

        current_length = row.length if row else 0
        session.execute(
            "INSERT INTO doc_index (document_id, title, length) VALUES (%s, %s, %s)",
            (doc_id, title, current_length + length)
        )

