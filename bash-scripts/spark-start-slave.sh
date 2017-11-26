#!/usr/bin/env bash

cd $HOME || return
cd spark-2.2.0-bin-hadoop2.7 || (echo "cd spark directory failed"; return)
./sbin/start-slave.sh spark://fa17-cs425-g28-01.cs.illinois.edu:7077
