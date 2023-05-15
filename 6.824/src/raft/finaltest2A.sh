#!/bin/bash
for i in {1..10}
do
  go test -run 2C >> fordebug.txt
  echo "______________________________" >> fordebug.txt
done