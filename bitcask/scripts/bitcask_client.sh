#!/bin/bash

case $1 in
  --view-all)
    java -cp .:json-20231013.jar bitcask.App --view-all
    ;;
  --view)
    java -cp .:json-20231013.jar bitcask.App --view --key="$2"
    ;;
  --perf)
    java -cp .:json-20231013.jar bitcask.App --perf --clients="$2"
    ;;
  *)
    echo "Usage:"
    echo "  ./bitcask_client.sh --view-all"
    echo "  ./bitcask_client.sh --view <key>"
    echo "  ./bitcask_client.sh --perf <num_clients>"
    ;;
esac
