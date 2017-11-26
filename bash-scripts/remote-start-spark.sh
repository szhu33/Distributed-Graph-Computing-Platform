#!/usr/bin/env bash

for (( i = 1; i < 11; i++ )); do
    var=$(printf "weiren2@fa17-cs425-g28-%02d.cs.illinois.edu" $i)
    if [[ $i -eq 1 ]] ; then
        ssh $var 'export PATH=/usr/local/go/bin:$PATH; cd ~/go/src/cs425_mp4; git pull; bash bash-scripts/spark-start-master.sh'
    fi
    ssh $var 'export PATH=/usr/local/go/bin:$PATH; cd ~/go/src/cs425_mp4; git pull; bash bash-scripts/spark-start-slave.sh' &
done

wait
