#!/bin/bash

REGION="ca-central-1"
BUCKET_NAME="s3://vijay-pyspark-etl"

aws s3api create-bucket \
    --bucket ${BUCKET_NAME} \
    --region $REGION
