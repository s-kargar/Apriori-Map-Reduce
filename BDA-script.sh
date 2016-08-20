#!/bin/bash
##################################
#SOURCE FOLDER
SOURCE_DIR="/home/cna_kargar/BDA/"
INPUT_DIR="/home/cna_kargar/BDA/chess.dat"
OUTPUT_DIR="/home/cna/Desktop/BDA"
SUPPORT_VALUE="3100"
##################################
HADOOP="$( which hadoop )"
HDFS="$(which hdfs)"
JAR_FILE="$SOURCE_DIR/BDA_Apriori.jar"
##################################
HDFS_INPUT_DIR="hdfs://cluster-1-m:8020/input"
HDFS_OUTPUT_DIR="hdfs://cluster-1-m:8020/output"
HDFS_CACHE_DIR="hdfs://cluster-1-m:8020/cache"
##################################
OUTPUT_FILE="$OUTPUT_DIR/output.txt"
OUTPUT_FILE_TEMP="$OUTPUT_DIR/output_temp.txt"
CACHE_FILE="$HDFS_CACHE_DIR/cache.txt"
ITEMSET_SIZE="1"
##################################
TO_CACHE="$HADOOP fs -put -f $OUTPUT_DIR/output.txt $CACHE_FILE"
SORT_OUTPUT="python $SOURCE_DIR/py-sort.py  $OUTPUT_FILE_TEMP $OUTPUT_FILE"
##################################
#hdfs setting input
hdfs dfs -rm -r -skipTrash "/*" ;
hadoop fs -mkdir $HDFS_INPUT_DIR #&&
hadoop fs -put $INPUT_DIR $HDFS_INPUT_DIR #&&
hadoop fs -mkdir $HDFS_OUTPUT_DIR #&&
hadoop fs -mkdir $HDFS_CACHE_DIR #&&
##################################
# One frequent Itemset
$HADOOP jar $JAR_FILE $HDFS_INPUT_DIR $HDFS_OUTPUT_DIR $SUPPORT_VALUE $ITEMSET_SIZE #&&
$HDFS dfs -getmerge $HDFS_OUTPUT_DIR/$ITEMSET_SIZE $OUTPUT_FILE_TEMP #&&
$SORT_OUTPUT #&&
$TO_CACHE
for i in `seq 2 10`;
	do
	let ITEMSET_SIZE=$i #&&
	$HADOOP jar $JAR_FILE $HDFS_INPUT_DIR $HDFS_OUTPUT_DIR $SUPPORT_VALUE $ITEMSET_SIZE #&&
	$HDFS dfs -getmerge $HDFS_OUTPUT_DIR/$ITEMSET_SIZE $OUTPUT_FILE_TEMP #&&
	[ -s $OUTPUT_FILE_TEMP ]|| exit $?
	$SORT_OUTPUT #&&
	$TO_CACHE #&&
	echo "Next itemset"
done
