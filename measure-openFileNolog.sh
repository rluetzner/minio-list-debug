#!/bin/bash

# 1. Make sure that you've created an empty test bucket (e.g. ls-test) and an mc alias (e.g. nasxl)
# 2. Call the script like this: ./measure-openFileNolog.sh nasxl/ls-test 1 200 50 /mount/to/gluster/ls-test
# This will create a folder structure with one level (folders named 1 - 200) containing 50 objects in each folder.
# The script will generate two .csv files for PUT object and ListObjects latencies (in secs)

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!!ATTENTION!!! This is not the same test file as posted on GitHub!!!
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# ./measure-openFileNolog.sh nasxl/test20000 1 400 50 /gluster/repositories/<repo>/<space>/test20000 &

trap "exit" INT

TIMEFORMAT=%R

bucket=$1
min_folders=$2
max_folders=$3
files_per_folder=$4
bucket_mount=$5
outfile_prefix=`(echo $bucket | tr '/' '.')`
mc="mc"

touch file

object_count=0
for i in $(seq $min_folders $max_folders); do
    for j in $(seq 1 $files_per_folder); do
        echo `(time $mc cp file $bucket/$i/file$j --quiet --insecure >>/dev/null) 2>&1` >> $outfile_prefix.PUT.$min_folders.$max_folders.$files_per_folder.csv
    done
    ((object_count=object_count+files_per_folder))
    ./walkdir $bucket_mount | tee -a $outfile_prefix.OPEN.$min_folders.$max_folders.$files_per_folder.csv
done
