#!/bin/bash

PROJECT_DIR=/Users/vijay/workspace/pyspark_etl""
SPARK_HOME="${PROJECT_DIR}/spark-3.5.1-bin-hadoop3"
SRC_DIR="${PROJECT_DIR}/src"
CONF_DIR="${PROJECT_DIR}/config"
PY_FILES=(
"${SRC_DIR}/appcontext.py"
"${SRC_DIR}/datastore.py"
"${SRC_DIR}/helloworld.py"
"${SRC_DIR}/metrics.py"
"${SRC_DIR}/prepare.py"
"${SRC_DIR}/schema.py"
"${SRC_DIR}/test_pytest1.py"
"${SRC_DIR}/unitest.py"
"${SRC_DIR}/utils.py"
)
MAIN_FILE="${SRC_DIR}/prepare.py"

${SPARK_HOME}/bin/spark-submit ${MAIN_FILE} "${CONF_DIR}/config.json"
