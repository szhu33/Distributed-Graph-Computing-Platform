#!/usr/bin/env bash

cd $HOME || return
cd spark-2.2.0-bin-hadoop2.7 || (echo "cd spark directory failed"; return)

./sbin/stop-master.sh
./sbin/stop-slave.sh
