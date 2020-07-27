#!/bin/bash
# This is a mock of scheduler that starts PPW executable

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo "this is a mock of scheduled that starts PPW executable"
$DIR/pipelinewise-mock.sh

