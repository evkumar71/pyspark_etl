#!/bin/bash

PROJECT_ROOT="/Users/vijay/workspace/pyspark_etl"
BUILD_DIR="${PROJECT_ROOT}/build"
SOURCES_DIR="${PROJECT_ROOT}/src"
BUCKET_NAME="s3://vijay-pyspark-etl"

function package_application() {
  cd "${SOURCES_DIR}" || exit
  zip -r "${BUILD_DIR}/application.zip" "." -x "*__pycache__*"
  cd - || exit
}

function upload_s3() {
  aws s3 cp "${BUILD_DIR}/application.zip" "${BUCKET_NAME}"
  aws s3 cp --recursive --exclude "*__pycache__*" "${PROJECT_ROOT}/src" "${BUCKET_NAME}/src"
  aws s3 cp --recursive "${PROJECT_ROOT}/config" "${BUCKET_NAME}/config"
}

if [ ! -d ${BUILD_DIR} ]; then
  mkdir ${BUILD_DIR}
fi

package_application
upload_s3
