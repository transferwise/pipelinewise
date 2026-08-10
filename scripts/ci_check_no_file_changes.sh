#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Usage instructions:
#
# to check for python changes, run with CHECKS=python
# To check for doc changes, run with CHECKS=doc
# To check for sample/dev config changes, run with CHECKS=config
# To check for several kinds at once, run with CHECKS="python doc config"
if [[ (-z ${PR_NUMBER}) && (-z ${CIRCLE_PULL_REQUEST}) ]]; then
  echo "Not a PR; Exiting with FAILURE code"
  exit 1
fi

if [[ ! -z ${CIRCLE_PULL_REQUEST} ]]; then
  PR_NUMBER=$(grep -Po '.*\/pull\/\K(\d+)' <<< $CIRCLE_PULL_REQUEST) # extract PR number from circleci full PR path
  GITHUB_REPO="${CIRCLE_PROJECT_USERNAME}/${CIRCLE_PROJECT_REPONAME}"
fi

URL="https://api.github.com/repos/${GITHUB_REPO}/pulls/${PR_NUMBER}/files"

echo "PR URL:${URL}"

fetch_changed_files() {
  local page=1
  local page_size
  local response
  local -a curl_args=(
    --fail
    --silent
    --show-error
    --get
    --header "Accept: application/vnd.github+json"
    --header "X-GitHub-Api-Version: 2022-11-28"
  )

  if [[ -n ${GITHUB_TOKEN:-} ]]; then
    curl_args+=(--header "Authorization: Bearer ${GITHUB_TOKEN}")
  fi

  while true; do
    if ! response=$(curl "${curl_args[@]}" \
      --data-urlencode "per_page=100" \
      --data-urlencode "page=${page}" \
      "${URL}"); then
      echo "Failed to fetch changed files from GitHub" >&2
      return 1
    fi

    if ! jq -e 'type == "array" and all(.[]; (.filename | type) == "string")' \
      >/dev/null <<< "${response}"; then
      echo "GitHub changed-files response is invalid" >&2
      return 1
    fi

    if ! page_size=$(jq -r 'length' <<< "${response}"); then
      echo "Failed to read GitHub changed-files page size" >&2
      return 1
    fi
    if ! jq -r '.[].filename' <<< "${response}"; then
      echo "Failed to read filenames from GitHub response" >&2
      return 1
    fi

    if (( page_size < 100 )); then
      break
    fi
    ((page += 1))
  done
}

if ! FILES=$(fetch_changed_files); then
  echo "Unable to determine changed files; Exiting with FAILURE code"
  exit 1
fi

REGEXES=()
for CHECK in "$@"
do
  if [[ ${CHECK} == "python" ]]; then
    REGEX="(^tests\/|^pipelinewise\/|^singer-connectors\/|^scripts\/ci_check_no_file_changes\.sh$|^setup\.py|^Makefile)"
    echo "Searching for changes in python files"

  elif [[ ${CHECK} == "doc" ]]; then
    REGEX="(^docs\/|^scripts/publish_docs.sh)"
    echo "Searching for changes in documentation files"

  elif [[ ${CHECK} == "config" ]]; then
    # dev-project holds the YAML the docs tell users to copy. It is outside the
    # python regex, so without this a config-only change skips every test job.
    REGEX="(^dev-project\/)"
    echo "Searching for changes in dev-project configuration files"

  else
    echo "Invalid check: \"${CHECK}\". Falling back to exiting with FAILURE code"
    exit 1
  fi
  REGEXES=("${REGEXES[@]}" "${REGEX}")
done
echo

cat<<EOF
CHANGED FILES:
$FILES

EOF

while IFS= read -r FILE
do
  if [[ -z ${FILE} ]]; then
    continue
  fi
  for REGEX in "${REGEXES[@]}"
  do
    if [[ "${FILE}" =~ ${REGEX} ]]; then
      echo "Detected changes in following file: ${FILE}"
      echo "Exiting with FAILURE code"
      exit 1
    fi
  done
done <<< "${FILES}"
echo "No changes detected... Exiting with SUCCESS code"
exit 0
