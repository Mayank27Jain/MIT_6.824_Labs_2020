#!/bin/bash
for i in {1..10}
do
  go test -run 2A
  echo $?
done