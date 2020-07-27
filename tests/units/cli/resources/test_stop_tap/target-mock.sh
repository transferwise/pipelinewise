#!/bin/bash
# This is a mock target that read messages from a tap on STDIN
# and prints the received message to STDOUT

# Reading message from STDIN
while read line
do
  echo "Message received: $line"
done < "${1:-/dev/stdin}"
