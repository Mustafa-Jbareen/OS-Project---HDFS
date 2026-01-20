#!/bin/bash

timestamp() {
  date +"%Y-%m-%d_%H-%M-%S"
}

log() {
  echo "[`date '+%H:%M:%S'`] $1"
}

hdfs_rm_if_exists() {
    if hdfs dfs -test -e "$1"; then
        hdfs dfs -rm -r -f "$1"
    else
        return 0
    fi
}
