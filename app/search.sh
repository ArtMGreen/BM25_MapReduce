#!/bin/bash


source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 

# Python of the excutor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

unset PYSPARK_DRIVER_PYTHON
spark-submit --master yarn \
--archives /app/.venv.tar.gz#.venv \
--conf "spark.driver.extraJavaOptions=-Dlog4j.logLevel=WARN" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.logLevel=WARN" \
--conf "spark.logConf=true" \
/app/query.py "$1"
