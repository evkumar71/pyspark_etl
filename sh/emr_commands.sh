#!/bin/bash

# set bucket name env

export BUCKET_URL="s3://vijay-pyspark-etl"

# submit application with archive

PYSPARK_PYTHON=./environment/bin/python \
spark-submit \
  --deploy-mode cluster \
  --py-files "${BUCKET_URL}/application.zip" \
  --files "${BUCKET_URL}/config/conf-aws.json" \
  --archives "${BUCKET_URL}/environment.tar.gz#environment" \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
  --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python \
  "${BUCKET_URL}/src/prepare.py" "conf-aws.json"

# view logs for application

yarn logs -applicationId <appId> -log_files stdout
