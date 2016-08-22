#!/bin/bash
###################################################################
#SOURCE FOLDER
CURRENT_DIR=$(pwd)
SOURCE_DIR=$CURRENT_DIR
LOCAL_OUTPUT_DIR=$SOURCE_DIR
LOCAL_INPUT_DIR="$SOURCE_DIR/file.txt"
##################################
#HDFS Directories --> ENTER HERE
HDFS_INPUT_DIR="$CURRENT_DIR/input"
HDFS_OUTPUT_DIR="$CURRENT_DIR/output"
###################################################################
#HDFS_CACHE_DIR="$HDFS_OUTPUT_DIR/cache"
HDFS="$(which hdfs)"
##################################
#hdfs setting input
#$HDFS dfs -rm -r -skipTrash "~/Desktop/BDA/test" ; #REMOVE
$HDFS dfs -mkdir $HDFS_INPUT_DIR #&&
$HDFS dfs -put $LOCAL_INPUT_DIR $HDFS_INPUT_DIR #&&
#$HDFS dfs -mkdir $HDFS_OUTPUT_DIR #&&
#$HDFS dfs -mkdir $HDFS_CACHE_DIR #&&
##################################
