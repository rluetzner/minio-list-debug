#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail

systemctl stop minio-fsnoworm-s3.service
sleep 5
rm -rf /gluster/repositories/fsnoworm-s3/fsnoworm-s3/.minio.sys
rm -rf /gluster/repositories/fsnoworm-s3/fsnoworm-s3/*
cp /home/l3support/minio.forked $(which minio)
sed -i 's/server/gateway $GATEWAY_MODE/' /etc/systemd/system/minio-fsnoworm-s3.service
systemctl daemon-reload
sleep 5
systemctl start minio-fsnoworm-s3.service
sleep 10

echo "Perform tests with forked binary"
for i in {1..3}
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

systemctl stop minio-fsnoworm-s3.service
sleep 5
rm -rf /gluster/repositories/fsnoworm-s3/fsnoworm-s3/.minio.sys
rm -rf /gluster/repositories/fsnoworm-s3/fsnoworm-s3/*
cp /home/l3support/minio.official $(which minio)
sed -i 's/gateway $GATEWAY_MODE/server/' /etc/systemd/system/minio-fsnoworm-s3.service
systemctl daemon-reload
sleep 5
systemctl start minio-fsnoworm-s3.service
sleep 10

echo "Perform tests with official binary"
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
