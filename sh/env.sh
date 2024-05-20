#!/bin/bash

export PROJECT_ROOT="/Users/vijay/workspace/pyspark_etl"
export BUILD_DIR="${PROJECT_ROOT}/build"
export SRC_DIR="${PROJECT_ROOT}/src"
export CONFIG_DIR="${PROJECT_ROOT}/config"

export BUCKET_NAME="vijay-pyspark-etl"
export BUCKET_URL="s3://${BUCKET_NAME}"
export REGION="ca-central-1"
export CLUSTER_ID="j-JN0XFKG2C55V"
export KEY_FILE="${PROJECT_ROOT}/vijay_ec2_key.pem"
export PRIMARY_HOST="ec2-3-99-247-157"
export EMR_USER="hadoop"
