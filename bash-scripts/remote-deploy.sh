#!/usr/bin/env bash

for (( i = 1; i < 11; i++ )); do
    var=$(printf "weiren2@fa17-cs425-g28-%02d.cs.illinois.edu" $i)
    ssh $var 'export PATH=/usr/local/go/bin:$PATH; cd ~/go/src/cs425_mp4; git pull; bash bash-scripts/deploy-spark.sh' &
done
wait
