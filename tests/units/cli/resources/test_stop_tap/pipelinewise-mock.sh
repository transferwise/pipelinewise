#!/bin/bash
# This is a mock of main PPW executable that is running
# linux piped tap and target dummy connectors

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo "this is a mock of main PPW executable"
$DIR/tap-mock.sh | $DIR/target-mock.sh
