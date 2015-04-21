#This script imports the whole bucket from ES into the HFS
#COUCHBASE_IP=192.168.56.105
#COUCHBASE_PORT=8091
source env.sh

hadoop fs -rm -r DUMP
sqoop import --username reputation --verbose \
    --connect http://$COUCHBASE_IP:$COUCHBASE_PORT/pools --table DUMP
hadoop fs -getmerge  DUMP/ input.txt
