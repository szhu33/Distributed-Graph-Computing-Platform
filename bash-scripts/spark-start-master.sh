#!/usr/bin/env bash

cd $HOME || exit 1
cd spark-2.2.0-bin-hadoop2.7 || { echo "cd spark directory failed"; exit 1; }
./sbin/start-master.sh
