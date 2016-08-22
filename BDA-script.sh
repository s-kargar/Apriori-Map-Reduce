#!/bin/bash

#HDFS Directories --> ENTER HERE
HDFS_INPUT_DIR="hdfs://cluster-1-m:8020/input"
HDFS_OUTPUT_DIR="hdfs://cluster-1-m:8020/output"
SUPPORT_VALUE="3000"
NUM_REDUCERS="1"
###################################
#Setting variables
CURRENT_DIR=$(pwd)
SOURCE_DIR=$CURRENT_DIR
LOCAL_OUTPUT_DIR=$SOURCE_DIR
LOCAL_OUTPUT_FILE="$LOCAL_OUTPUT_DIR/output.txt"
LOCAL_OUTPUT_FILE_TEMP="$LOCAL_OUTPUT_DIR/output_temp.txt"
JAR_FILE="$SOURCE_DIR/BDA_Apriori.jar"
SORT_SCRIPT="$SOURCE_DIR/py-sort.py"
HDFS_CACHE_DIR="$HDFS_OUTPUT_DIR/cache"
CACHE_FILE="$HDFS_CACHE_DIR/cache.txt"

HADOOP="$( which hadoop )"
HDFS="$(which hdfs)"
TO_CACHE="$HDFS dfs -put -f $LOCAL_OUTPUT_DIR/output.txt $CACHE_FILE"
SORT_OUTPUT="python $SORT_SCRIPT $LOCAL_OUTPUT_FILE_TEMP $LOCAL_OUTPUT_FILE"

##hdfs output and cache directory
$HDFS dfs -mkdir -p $HDFS_CACHE_DIR
##################################
#Executing Apriori
# Generating one frequent Itemset
ITEMSET_SIZE="1"
start_time1=`date +%s`
$HADOOP jar $JAR_FILE $HDFS_INPUT_DIR $HDFS_OUTPUT_DIR $SUPPORT_VALUE $ITEMSET_SIZE $NUM_REDUCERS
$HDFS dfs -getmerge $HDFS_OUTPUT_DIR/$ITEMSET_SIZE $LOCAL_OUTPUT_FILE_TEMP
$HDFS dfs -rm -r -skipTrash $HDFS_OUTPUT_DIR/$ITEMSET_SIZE
$SORT_OUTPUT
$TO_CACHE
end_time=`date +%s`
echo "$ITEMSET_SIZE-itemsets time = $((end_time-start_time1))"

# Generating K-frequent itemset (k=2... until no larger item set can be generated)
for i in `seq 2 1000`;
	do
	let ITEMSET_SIZE=$i
	start_time2=`date +%s`
	$HADOOP jar $JAR_FILE $HDFS_INPUT_DIR $HDFS_OUTPUT_DIR $SUPPORT_VALUE $ITEMSET_SIZE $NUM_REDUCERS
	$HDFS dfs -getmerge $HDFS_OUTPUT_DIR/$ITEMSET_SIZE $LOCAL_OUTPUT_FILE_TEMP
	$HDFS dfs -rm -r -skipTrash $HDFS_OUTPUT_DIR/$ITEMSET_SIZE
	[ -s $LOCAL_OUTPUT_FILE_TEMP ]||{
		let end_time=`date +%s`
		echo "total running time =$((end_time-start_time1)), SUPPORT=$SUPPORT_VALUE , number of reducers=$NUM_REDUCERS"
		break
		}
	$SORT_OUTPUT
	$TO_CACHE
        let end_time=`date +%s`
        echo "$ITEMSET_SIZE-itemsets time = $((end_time-start_time2))"
	echo "Next itemset"
done

$HDFS dfs -rm -r -skipTrash $HDFS_CACHE_DIR
$HDFS dfs -put -f $LOCAL_OUTPUT_FILE $HDFS_OUTPUT_DIR
