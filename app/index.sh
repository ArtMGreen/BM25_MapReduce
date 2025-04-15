#!/bin/bash

echo "Index input is:"
echo $1

chmod 744 /app/mapreduce/mapper1.py
chmod 744 /app/mapreduce/reducer1.py
chmod 744 /app/mapreduce/mapper2.py
chmod 744 /app/mapreduce/reducer2.py
chmod 744 /app/mapreduce/mapper3.py
chmod 744 /app/mapreduce/reducer3.py

hdfs dfs -rm -r -f /index/data/output1
hdfs dfs -rm -r -f /index/data/output2
hdfs dfs -rm -r -f /index/data/output3

# python3 create_empty_cassandra_tables.py

mapred streaming \
-files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
-mapper 'python3 mapper1.py' \
-reducer 'python3 reducer1.py' \
-input $1 -output /index/data/output1

mapred streaming \
-files /app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py \
-mapper 'python3 mapper2.py' \
-reducer 'python3 reducer2.py' \
-input $1 -output /index/data/output2

mapred streaming \
-files /app/mapreduce/mapper3.py,/app/mapreduce/reducer3.py \
-mapper 'python3 mapper3.py' \
-reducer 'python3 reducer3.py' \
-input $1 -output /index/data/output3

hdfs dfs -cat /index/data/output1/* | python3 to_cassandra_tf.py --debug-outputs --drop-table
hdfs dfs -cat /index/data/output2/* | python3 to_cassandra_df.py --debug-outputs --drop-table
hdfs dfs -cat /index/data/output3/* | python3 to_cassandra_doc_index.py --debug-outputs --drop-table
