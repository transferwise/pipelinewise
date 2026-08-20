#!/usr/bin/env bash

set -u

if (( $# == 0 )); then
  echo 'No required environment variables were specified' >&2
  exit 2
fi

missing=()
for variable_name in "$@"; do
  if [[ -z ${!variable_name-} ]]; then
    missing+=("${variable_name}")
  fi
done

if (( ${#missing[@]} > 0 )); then
  echo "Missing required environment variables: ${missing[*]}" >&2
  exit 1
fi

echo 'All required environment variables are configured'
