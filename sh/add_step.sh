#!/bin/bash

REGION="ca-central-1"
CLUSTER_ID="j-3IECOL9TNB48K"
NAME="hello-world"
BUCKET_NAME="s3://vijay-pyspark-etl"
PY_FILES="${BUCKET_NAME}/application.zip"
FILES="${BUCKET_NAME}/config/conf-aws.json"
MAIN="${BUCKET_NAME}/src/helloworld.py"
ARGS="conf-aws.json"

aws emr add-steps --region ${REGION} --cluster-id ${CLUSTER_ID} \
  --steps '[
    {
      "Args":[
        "spark-submit",
        "--deploy-mode","cluster",
        "--py-files","'${PY_FILES}'",
        "--files","'${FILES}'",
        "'${MAIN}'",
        "'${ARGS}'"
      ],
      "Type":"CUSTOM_JAR",
      "ActionOnFailure":"CONTINUE",
      "Jar":"command-runner.jar",
      "Name":"'${NAME}'"
    }
  ]'
