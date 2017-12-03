#!/usr/bin/env bash

for (( i = 1; i < 11; i++ )); do
    var=$(printf "weiren2@fa17-cs425-g28-%02d.cs.illinois.edu" $i)
    if [[ $i -eq 10 ]]; then
        konsole --new-tab -e ssh -t $var 'export PATH=/usr/local/go/bin:$PATH; cd ~/go/src/cs425_mp4; git pull; cd master; go build; clear; ./master; bash -l' &
    elif [[ $i -eq 5 ]]; then
        konsole --new-tab -e ssh -t $var 'export PATH=/usr/local/go/bin:$PATH; cd ~/go/src/cs425_mp4; git pull; cd client; go build; clear; ./client; bash -l' &
    elif [[ $i -eq 9 ]]; then
        konsole --new-tab -e ssh -t $var 'export PATH=/usr/local/go/bin:$PATH; cd ~/go/src/cs425_mp4; git pull; cd master; go build; clear; ./master; bash -l' &
    else
        konsole --new-tab -e ssh -t $var 'export PATH=/usr/local/go/bin:$PATH; cd ~/go/src/cs425_mp4; git pull; cd worker; go build; clear; ./worker; bash -l' &
    fi

    sleep 0.1s
done
