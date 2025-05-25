PYTHON_EXEC="python3"

if [ "$1" == "--view-all" ]; then
  $PYTHON_EXEC bitcask_cli.py --view-all

elif [ "$1" == "--view" ]; then
  KEY=$(echo "$2" | cut -d'=' -f2)
  $PYTHON_EXEC bitcask_cli.py --view "$KEY"

elif [ "$1" == "--perf" ]; then
  CLIENTS=$(echo "$2" | cut -d'=' -f2)
  $PYTHON_EXEC bitcask_cli.py --perf "$CLIENTS"

else
  echo "Usage:"
  echo "./bitcask_client.sh --view-all"
  echo "./bitcask_client.sh --view --key=station_1"
  echo "./bitcask_client.sh --perf --clients=100"
fi