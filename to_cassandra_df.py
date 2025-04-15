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
#     session.execute("DROP TABLE IF EXISTS document_frequencies;")
#     if DEBUG_OUTPUTS:
#         print("Dropped old DF table")

session.execute("""
    CREATE TABLE IF NOT EXISTS document_frequencies (
        term text PRIMARY KEY,
        df int
    );
""")
if DEBUG_OUTPUTS:
    print("document_frequencies table now exists")

i = 0
for line in sys.stdin:
    term, df = line.strip().split('\t')
    df = int(df)
    if DROP_TABLE:
        i += 1
        session.execute(
            "INSERT INTO document_frequencies (term, df) VALUES (%s, %s)",
            (term, int(df))
        )
        if DEBUG_OUTPUTS and i % 10000 == 0:
            print("Put", i, "lines into Cassandra table")
    else:
        row = session.execute(
            "SELECT df FROM document_frequencies WHERE term=%s",
            (term,)
        ).one()

        current_df = row.df if row else 0
        session.execute(
            "INSERT INTO document_frequencies (term, df) VALUES (%s, %s)",
            (term, current_df + df)
        )

