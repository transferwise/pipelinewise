#!/bin/bash -e
#
# Building a test MongoDB database for integration testing of tap-mongodb
# The listings data is Airbnb listing from http://insideairbnb.com/get-the-data.html
# my_collection data is dummy data from https://www.mockaroo.com/
PWD="$(dirname "$0")"

TEST_DB_DATA_1=${PWD}/mongodb_data/listings.csv
TEST_DB_DATA_2=${PWD}/mongodb_data/my_collection.bson.gz
TEST_DB_DATA_3=${PWD}/mongodb_data/all_datatypes.bson.gz
echo "Building test Mongodb database..."

# To run this script some environment variables must be set.
# Normally it's defined in .circleci/config.yml
if [[ -z "${TAP_MONGODB_HOST}" || -z "${TAP_MONGODB_PORT}" || -z "${TAP_MONGODB_USER}" || -z "${TAP_MONGODB_PASSWORD}" || -z "${TAP_MONGODB_DB}" ]]; then
    echo "ERROR: One or more required environment variable is not defined:"
    echo "       - TAP_MONGODB_HOST"
    echo "       - TAP_MONGODB_PORT"
    echo "       - TAP_MONGODB_USER"
    echo "       - TAP_MONGODB_PASSWORD"
    echo "       - TAP_MONGODB_DB"
    exit 1
fi

URL="mongodb://${TAP_MONGODB_USER}:${TAP_MONGODB_PASSWORD}@${TAP_MONGODB_HOST}:${TAP_MONGODB_PORT}/${TAP_MONGODB_DB}?authSource=admin"

mongoimport --uri ${URL} \
  --collection listings \
  --type csv \
  --headerline \
  --drop ${TEST_DB_DATA_1}

mongorestore --uri ${URL} \
  --db ${TAP_MONGODB_DB} \
  --collection my_collection \
  --drop \
  --gzip \
  ${TEST_DB_DATA_2}

mongorestore --uri ${URL} \
  --db ${TAP_MONGODB_DB} \
  --collection all_datatypes \
  --drop \
  --gzip \
  ${TEST_DB_DATA_3}
