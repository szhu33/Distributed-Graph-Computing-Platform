#!/usr/bin/env bash

cd $HOME || exit 1
cd spark-2.2.0-bin-hadoop2.7 || { echo "cd spark directory failed"; exit 1; }
# use default URL for now
./sbin/start-slave.sh spark://fa17-cs425-g28-10.cs.illinois.edu:7077
