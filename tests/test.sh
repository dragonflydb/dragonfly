for i in {1..100}
do
  python3 -m pytest dragonfly/replication_test.py -k "test_rotatin"
  if [[ $? -ne 0 ]]; then
    break
  fi
done
