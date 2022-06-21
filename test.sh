#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail

echo "Perform tests without restart"
for i in {1..3}
do
	bucket="test$i"
	mc mb "nasxl/$bucket"
	./measure-openFileNolog.sh "nasxl/$bucket" 1 400 500 "/gluster/repositories/fsnoworm-s3/fsnoworm-s3/$bucket/"
done

echo "Perform tests with restart"
for i in {4..6}
do
	echo "Restarting gluster..."
	echo 'y' | gluster volume stop fsnoworm-s3 force
	sleep 10
	gluster volume start fsnoworm-s3
	sleep 10
	echo "Restarted gluster"
	bucket="test$i"
	mc mb "nasxl/$bucket"
	./measure-openFileNolog.sh "nasxl/$bucket" 1 400 500 "/gluster/repositories/fsnoworm-s3/fsnoworm-s3/$bucket/"
done
