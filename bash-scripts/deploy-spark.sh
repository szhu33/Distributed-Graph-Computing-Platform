#!/usr/bin/env bash

cd $HOME || return
wget http://apache.mirrors.ionfish.org/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz
tar -xf spark-2.2.0-bin-hadoop2.7.tgz
