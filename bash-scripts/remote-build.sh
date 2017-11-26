#!/usr/bin/env bash

for (( i = 1; i < 11; i++ )); do
    var=$(printf "weiren2@fa17-cs425-g28-%02d.cs.illinois.edu" $i)
    ssh $var 'export PATH=/usr/local/go/bin:$PATH; cd ~/go/src/cs425_mp4; git fetch --all; git checkout master-server; git pull; go version; go get; go build' &
done
wait
