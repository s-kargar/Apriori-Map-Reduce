#!/bin/bash
###################################################################
#SOURCE FOLDER
SOURCE_DIR="/home/cna_kargar/apriori/"
LOCAL_INPUT_DIR="/home/cna_kargar/apriori/chess.dat"
LOCAL_OUTPUT_DIR="/home/cna_kargar/apriori"
SUPPORT_VALUE="3100"
NUM_REDUCERS="2"
##################################
LOCAL_OUTPUT_FILE="$LOCAL_OUTPUT_DIR/output.txt"
LOCAL_OUTPUT_FILE_TEMP="$LOCAL_OUTPUT_DIR/output_temp.txt"
##################################
#HDFS Directories --> ENTER HERE
HDFS_INPUT_DIR="hdfs://cluster-1-m:8020/input"
HDFS_OUTPUT_DIR="hdfs://cluster-1-m:8020/output"
###################################################################
HDFS_CACHE_DIR="$HDFS_OUTPUT_DIR/cache"
CACHE_FILE="$HDFS_CACHE_DIR/cache.txt"
##################################
HADOOP="$( which hadoop )"
HDFS="$(which hdfs)"
JAR_FILE="$SOURCE_DIR/BDA_Apriori.jar"
SORT_SCRIPT="$SOURCE_DIR/py-sort.py"
##################################
TO_CACHE="$HADOOP fs -put -f $LOCAL_OUTPUT_DIR/output.txt $CACHE_FILE"
SORT_OUTPUT="python $SORT_SCRIPT $LOCAL_OUTPUT_FILE_TEMP $LOCAL_OUTPUT_FILE"
##################################
#hdfs setting input
hdfs dfs -rm -r -skipTrash "/*" ;
hadoop fs -mkdir $HDFS_INPUT_DIR #&&
hadoop fs -put $LOCAL_INPUT_DIR $HDFS_INPUT_DIR #&&
hadoop fs -mkdir $HDFS_OUTPUT_DIR #&&
hadoop fs -mkdir $HDFS_CACHE_DIR #&&
##################################
# One frequent Itemset
ITEMSET_SIZE="1"
$HADOOP jar $JAR_FILE $HDFS_INPUT_DIR $HDFS_OUTPUT_DIR $SUPPORT_VALUE $ITEMSET_SIZE $NUM_REDUCERS #&&
$HDFS dfs -getmerge $HDFS_OUTPUT_DIR/$ITEMSET_SIZE $LOCAL_OUTPUT_FILE_TEMP #&&
$SORT_OUTPUT #&&
$TO_CACHE
for i in `seq 2 10`;
	do
	let ITEMSET_SIZE=$i #&&
	$HADOOP jar $JAR_FILE $HDFS_INPUT_DIR $HDFS_OUTPUT_DIR $SUPPORT_VALUE $ITEMSET_SIZE $NUM_REDUCERS #&&
	$HDFS dfs -getmerge $HDFS_OUTPUT_DIR/$ITEMSET_SIZE $LOCAL_OUTPUT_FILE_TEMP #&&
	[ -s $LOCAL_OUTPUT_FILE_TEMP ]|| exit $?
	$SORT_OUTPUT #&&
	$TO_CACHE #&&
	echo "Next itemset"
done
