for run in {1..1000}; do
  python3 -m pytest dragonfly/replication_test.py --log-cli-level=INFO -k "test_reproduce_shutdown"
  ret=$?
  if [ $ret -ne 0 ]; then
    echo "Error"
    exit 1
  fi
done
