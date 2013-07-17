#!/bin/sh

for cluster in `seq 0 1`
do
    for bucket in `seq 0 1`
    do
        for replica in `seq 0 2`
        do
            name="c${cluster}b${bucket}r${replica}"
            path="cluster.data/$name"
            port=`expr 10000 + $cluster$bucket$replica`
            mkdir -p $path
            echo "Starting replica $name (path=$path, port=$port)"
            ../bin/clblobserver -c cluster.conf -d \
                --clblob.client.replica=$name \
                --clblob.index.sqlite.database="$path/_index" \
                --clblob.store.disk.path=$path \
                --clcommon.http.host=\"127.0.0.1\" \
                --clcommon.http.port=$port > cluster.data/$name.log 2>&1 &
        done
    done
done

echo
echo "Web console: http://127.0.0.1:10000/_console"
echo
echo "Get status of a replica: ../bin/clblobclient -c cluster.conf status c0b0r0"
echo
echo "Put file: ../bin/clblobclient -c cluster.conf put ../README.rst"
echo
echo "Get file: ../bin/clblobclient -c cluster.conf get README.rst"
